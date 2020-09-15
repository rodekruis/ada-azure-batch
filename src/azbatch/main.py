import datetime
import os
import sys

import azure.storage.blob as azureblob
import azure.batch.batch_service_client as batch
import azure.batch.batch_auth as batch_auth
import azure.batch.models as batchmodels

import click

from azbatch.utils import query_yes_no, print_batch_exception, wait_for_tasks_to_complete, print_task_output, \
    upload_file_to_container

NEO_IMAGE = "ada510.azurecr.io/neo:merged-python"
# NEO_IMAGE = "ada510.azurecr.io/neo:nvidia"  # doesn't help


def create_container_config(config):
    ada_cr = batchmodels.ContainerRegistry(
        registry_server="ada510.azurecr.io",
        user_name="ada510",
        password=config["CR_PASSWORD"],
    )
    return batchmodels.ContainerConfiguration(
        container_image_names=[NEO_IMAGE],
        container_registries=[ada_cr],
    )


def create_pool(batch_service_client, config):
    """
    Creates a pool of compute nodes with the specified OS settings.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str pool_id: An ID for the new pool.
    :param str publisher: Marketplace image publisher
    :param str offer: Marketplace image offer
    :param str sku: Marketplace image sku
    """
    print("Creating pool [{}]...".format(config['POOL_ID']))

    # Create a new pool of Linux compute nodes using an Azure Virtual Machines
    # Marketplace image. For more information about creating pools of Linux
    # nodes, see:
    # https://azure.microsoft.com/documentation/articles/batch-linux-nodes/

    new_pool = batch.models.PoolAddParameter(
        id=config['POOL_ID'],
        virtual_machine_configuration=batchmodels.VirtualMachineConfiguration(
            image_reference=batchmodels.ImageReference(
                publisher="microsoft-azure-batch",
                offer="ubuntu-server-container",
                sku='16-04-lts',
                version="latest",
            ),
            node_agent_sku_id="batch.node.ubuntu 16.04",
            container_configuration=create_container_config(config),
        ),
        vm_size=config['POOL_VM_SIZE'],
        target_dedicated_nodes=config['POOL_NODE_COUNT'],
    )
    batch_service_client.pool.add(new_pool)


def create_job(batch_service_client, config):
    """
    Creates a job with the specified ID, associated with the specified pool.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str job_id: The ID for the job.
    :param str pool_id: The ID for the pool.
    """
    print("Creating job [{}]...".format(config['JOB_ID']))

    job = batch.models.JobAddParameter(
        id=config['JOB_ID'], pool_info=batch.models.PoolInformation(pool_id=config['POOL_ID'])
    )
    batch_service_client.job.add(job)


def add_tasks(batch_service_client, blob_client, config):
    """
    Adds a task for each input file in the collection to the specified job.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param list input_files: A collection of input files. One task will be
     created for each input file.
    :param config: Config
    """
    print("Adding tasks to job [{}]...".format(config['JOB_ID']))

    # Set up container
    task_container_settings = batchmodels.TaskContainerSettings(
        image_name=NEO_IMAGE,
        container_run_options='--rm --workdir /ada_tools'  #  --gpus all
    )
    admin_identity = batchmodels.UserIdentity(
        auto_user=batchmodels.AutoUserSpecification(
            scope='pool',
            elevation_level='admin',
        )
    )

    # Set up blob client
    input_container_name = 'adafiles'
    blob_client.create_container(input_container_name, fail_on_exist=False)
    container_sas_token = blob_client.generate_container_shared_access_signature(
        input_container_name,
        permission=azureblob.BlobPermissions.WRITE,
        expiry=datetime.datetime.utcnow() + datetime.timedelta(days=1))
    container_url = blob_client.make_container_url(input_container_name, sas_token=container_sas_token)
    output_file_dest = batchmodels.OutputFileBlobContainerDestination(container_url=container_url)
    upload_opts = batchmodels.OutputFileUploadOptions(
        upload_condition=batchmodels.OutputFileUploadCondition.task_success
    )
    tasks = [
        batchmodels.TaskAddParameter(
            id='001-create-file',
            command_line="touch $AZ_BATCH_TASK_WORKING_DIR/test.txt",
            container_settings=task_container_settings,
            user_identity=admin_identity,
            output_files=[batchmodels.OutputFile(file_pattern="**/*.txt",
                                                 destination=batchmodels.OutputFileDestination(
                container=output_file_dest),
                                                 upload_options=upload_opts)]
        ),
        # batchmodels.TaskAddParameter(
        #     id='002-check-file',
        #     command_line="cat /test.txt",
        #     container_settings=task_container_settings,
        #     user_identity=admin_identity,
        #     resource_files=[batchmodels.ResourceFile(storage_container_url=container_url)],
        #     # output_files=[batchmodels.OutputFile(file_pattern="/*.txt",
        #     #                                      destination=output_file_dest)]
        # ),

        # batchmodels.TaskAddParameter(
        #     id='00-info',
        #     command_line="neo info",
        #     container_settings=task_container_settings,
        #     user_identity=admin_identity,
        # ),
        # batchmodels.TaskAddParameter(
        #     id='01-load-images',
        #     command_line="load-images --dest /ada --maxpre 1 --maxpost 1",
        #     container_settings=task_container_settings,
        #     user_identity=admin_identity,
        # ),
        # batchmodels.TaskAddParameter(
        #     id='02-check-images',
        #     command_line="ls /ada",
        #     container_settings=task_container_settings,
        #     user_identity=admin_identity,
        # ),
    ]
    batch_service_client.task.add_collection(config['JOB_ID'], tasks)


@click.command()
@click.option("--pool-id", "-p")
@click.option("--job-id", "-j")
@click.option("--pool-node-count")
@click.option("--pool-vm-size")
@click.option("--std-out-fname")
def deploy(pool_id=None, job_id=None, pool_node_count=None, pool_vm_size=None, std_out_fname=None):
    start_time = datetime.datetime.now().replace(microsecond=0)
    print("Sample start: {}".format(start_time))

    config = {
        "POOL_ID": pool_id or f"pool_{start_time.strftime('%Y%m%d%H%M%S')}",
        "JOB_ID": job_id or f"job_{start_time.strftime('%Y%m%d%H%M%S')}",
        "POOL_NODE_COUNT": pool_node_count or 1,
        "POOL_VM_SIZE": pool_vm_size or "Standard_NC6",  # "STANDARD_D1_V2",
        "STANDARD_OUT_FILE_NAME": std_out_fname or "stdout.txt",

        "BATCH_ACCOUNT_NAME": os.environ.get("_BATCH_ACCOUNT_NAME"),
        "BATCH_ACCOUNT_KEY": os.environ.get("_BATCH_ACCOUNT_KEY"),
        "BATCH_ACCOUNT_URL": os.environ.get("_BATCH_ACCOUNT_URL"),
        "STORAGE_ACCOUNT_NAME": os.environ.get("_STORAGE_ACCOUNT_NAME"),
        "STORAGE_ACCOUNT_KEY": os.environ.get("_STORAGE_ACCOUNT_KEY"),
        "CR_PASSWORD": os.environ.get("_CR_PASSWORD"),
    }

    # Create a Batch service client. We'll now be interacting with the Batch
    # service in addition to Storage
    credentials = batch_auth.SharedKeyCredentials(
        config["BATCH_ACCOUNT_NAME"], config["BATCH_ACCOUNT_KEY"]
    )
    batch_client = batch.BatchServiceClient(
        credentials, batch_url=config["BATCH_ACCOUNT_URL"]
    )

    blob_client = azureblob.BlockBlobService(
        account_name=config["STORAGE_ACCOUNT_NAME"],
        account_key=config["STORAGE_ACCOUNT_KEY"]
    )

    try:
        # Create the pool that will contain the compute nodes that will execute the
        # tasks.
        # create_pool(batch_client, config)

        # Create the job that will run the tasks.
        create_job(batch_client, config)

        # Add the tasks to the job.
        add_tasks(batch_client, blob_client, config)

        # Pause execution until tasks reach Completed state.
        wait_for_tasks_to_complete(
            batch_client, config['JOB_ID'], datetime.timedelta(minutes=30)
        )

        print(
            "  Success! All tasks reached the 'Completed' state within the "
            "specified timeout period."
        )

        # Print the stdout.txt and stderr.txt files for each task to the console
        print_task_output(batch_client, config['JOB_ID'], config['STANDARD_OUT_FILE_NAME'])

    except batchmodels.BatchErrorException as err:
        print_batch_exception(err)
        raise

    # Print out some timing info
    end_time = datetime.datetime.now().replace(microsecond=0)
    print()
    print("Sample end: {}".format(end_time))
    print("Elapsed time: {}".format(end_time - start_time))
    print()

    # Clean up Batch resources (if the user so chooses).
    if query_yes_no("Delete job?") == "yes":
        batch_client.job.delete(config['JOB_ID'])

    if query_yes_no("Delete pool?") == "yes":
        batch_client.pool.delete(config['POOL_ID'])

    print()
    input("Press ENTER to exit...")


if __name__ == "__main__":
    # print(os.environ.get("_BATCH_ACCOUNT_NAME"))
    deploy()

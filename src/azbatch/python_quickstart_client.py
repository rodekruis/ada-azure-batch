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
        ),
        vm_size='STANDARD_D1_V2', # XXX  config['POOL_VM_SIZE'],
        target_dedicated_nodes=1, # XXX config['POOL_NODE_COUNT'],
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


def add_tasks(batch_service_client, input_files, config):
    """
    Adds a task for each input file in the collection to the specified job.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param list input_files: A collection of input files. One task will be
     created for each input file.
    :param output_container_sas_token: A SAS token granting write access to
    the specified Azure Blob storage container.
    """

    print("Adding {} tasks to job [{}]...".format(len(input_files), config['JOB_ID']))

    tasks = list()

    for idx, input_file in enumerate(input_files):

        command = '/bin/bash -c "cat {}"'.format(input_file.file_path)
        tasks.append(
            batch.models.TaskAddParameter(
                id="Task{}".format(idx),
                command_line=command,
                resource_files=[input_file],
            )
        )

    batch_service_client.task.add_collection(config['JOB_ID'], tasks)


def deploy(pool_id=None, pool_node_count=None, pool_vm_size=None, job_id=None, std_out_fname=None):
    config = {
        "POOL_ID": pool_id or "PythonQuickstartPool2",
        "POOL_NODE_COUNT": pool_node_count or 2,
        "POOL_VM_SIZE": pool_vm_size or "STANDARD_A1_v2",
        "JOB_ID": job_id or "PythonQuickstartJob2",
        "STANDARD_OUT_FILE_NAME": std_out_fname or "stdout.txt",

        "BATCH_ACCOUNT_NAME": os.environ.get("_BATCH_ACCOUNT_NAME"),
        "BATCH_ACCOUNT_KEY": os.environ.get("_BATCH_ACCOUNT_KEY"),
        "BATCH_ACCOUNT_URL": os.environ.get("_BATCH_ACCOUNT_URL"),
        "STORAGE_ACCOUNT_NAME": os.environ.get("_STORAGE_ACCOUNT_NAME"),
        "STORAGE_ACCOUNT_KEY": os.environ.get("_STORAGE_ACCOUNT_KEY"),
    }

    start_time = datetime.datetime.now().replace(microsecond=0)
    print("Sample start: {}".format(start_time))
    print()

    # Create the blob client, for use in obtaining references to
    # blob storage containers and uploading files to containers.

    blob_client = azureblob.BlockBlobService(
        account_name=config["STORAGE_ACCOUNT_NAME"],
        account_key=config["STORAGE_ACCOUNT_KEY"],
    )

    # Use the blob client to create the containers in Azure Storage if they
    # don't yet exist.

    input_container_name = "input"
    blob_client.create_container(input_container_name, fail_on_exist=False)

    # The collection of data files that are to be processed by the tasks.
    input_file_paths = [
        os.path.join(sys.path[0], "taskdata0.txt"),
        os.path.join(sys.path[0], "taskdata1.txt"),
        os.path.join(sys.path[0], "taskdata2.txt"),
    ]

    # Upload the data files.
    input_files = [
        upload_file_to_container(blob_client, input_container_name, file_path)
        for file_path in input_file_paths
    ]

    # Create a Batch service client. We'll now be interacting with the Batch
    # service in addition to Storage
    credentials = batch_auth.SharedKeyCredentials(
        config["BATCH_ACCOUNT_NAME"], config["BATCH_ACCOUNT_KEY"]
    )

    batch_client = batch.BatchServiceClient(
        credentials, batch_url=config["BATCH_ACCOUNT_URL"]
    )

    try:
        # Create the pool that will contain the compute nodes that will execute the
        # tasks.
        create_pool(batch_client, config)

        # Create the job that will run the tasks.
        create_job(batch_client, config)

        # Add the tasks to the job.
        add_tasks(batch_client, input_files, config)

        # Pause execution until tasks reach Completed state.
        wait_for_tasks_to_complete(
            batch_client, config['JOB_ID'], datetime.timedelta(minutes=30)
        )

        print(
            "  Success! All tasks reached the 'Completed' state within the "
            "specified timeout period."
        )

        # Print the stdout.txt and stderr.txt files for each task to the console
        print_task_output(batch_client, config['JOB_ID'])

    except batchmodels.BatchErrorException as err:
        print_batch_exception(err)
        raise

    # Clean up storage resources
    print("Deleting container [{}]...".format(input_container_name))
    blob_client.delete_container(input_container_name)

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
        batch_client.pool.delete(config['JOB_ID'])

    print()
    input("Press ENTER to exit...")


if __name__ == "__main__":
    # print(os.environ.get("_BATCH_ACCOUNT_NAME"))
    deploy()

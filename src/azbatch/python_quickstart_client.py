from __future__ import print_function
import datetime
import os
import sys

from azbatch import config
from azbatch.utils import (
    query_yes_no,
    print_batch_exception,
    wait_for_tasks_to_complete,
    print_task_output,
)

import azure.storage.blob as azureblob
import azure.batch.batch_service_client as batch
import azure.batch.batch_auth as batch_auth
import azure.batch.models as batchmodels


# Update the Batch and Storage account credential strings in config.py with values
# unique to your accounts. These are used when constructing connection strings
# for the Batch and Storage client objects.


def upload_file_to_container(block_blob_client, container_name, file_path):
    """
    Uploads a local file to an Azure Blob storage container.

    :param block_blob_client: A blob service client.
    :type block_blob_client: `azure.storage.blob.BlockBlobService`
    :param str container_name: The name of the Azure Blob storage container.
    :param str file_path: The local path to the file.
    :rtype: `azure.batch.models.ResourceFile`
    :return: A ResourceFile initialized with a SAS URL appropriate for Batch
    tasks.
    """
    blob_name = os.path.basename(file_path)

    print("Uploading file {} to container [{}]...".format(file_path, container_name))

    block_blob_client.create_blob_from_path(container_name, blob_name, file_path)

    sas_token = block_blob_client.generate_blob_shared_access_signature(
        container_name,
        blob_name,
        permission=azureblob.BlobPermissions.READ,
        expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=2),
    )

    sas_url = block_blob_client.make_blob_url(
        container_name, blob_name, sas_token=sas_token
    )

    return batchmodels.ResourceFile(http_url=sas_url, file_path=blob_name)


def create_pool(batch_service_client, pool_id):
    """
    Creates a pool of compute nodes with the specified OS settings.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str pool_id: An ID for the new pool.
    :param str publisher: Marketplace image publisher
    :param str offer: Marketplace image offer
    :param str sku: Marketplace image sku
    """
    print("Creating pool [{}]...".format(pool_id))

    # Create a new pool of Linux compute nodes using an Azure Virtual Machines
    # Marketplace image. For more information about creating pools of Linux
    # nodes, see:
    # https://azure.microsoft.com/documentation/articles/batch-linux-nodes/
    print(config._POOL_VM_SIZE, config._POOL_NODE_COUNT)

    new_pool = batch.models.PoolAddParameter(
        id=pool_id,
        virtual_machine_configuration=batchmodels.VirtualMachineConfiguration(
            image_reference=batchmodels.ImageReference(
                publisher="Canonical",
                offer="UbuntuServer",
                sku="18.04-LTS",
                version="latest",
            ),
            node_agent_sku_id="batch.node.ubuntu 18.04",
        ),
        vm_size=config._POOL_VM_SIZE,
        target_dedicated_nodes=config._POOL_NODE_COUNT,
    )
    batch_service_client.pool.add(new_pool)


def create_job(batch_service_client, job_id, pool_id):
    """
    Creates a job with the specified ID, associated with the specified pool.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str job_id: The ID for the job.
    :param str pool_id: The ID for the pool.
    """
    print("Creating job [{}]...".format(job_id))

    job = batch.models.JobAddParameter(
        id=job_id, pool_info=batch.models.PoolInformation(pool_id=pool_id)
    )

    batch_service_client.job.add(job)


def add_tasks(batch_service_client, job_id, input_files):
    """
    Adds a task for each input file in the collection to the specified job.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str job_id: The ID of the job to which to add the tasks.
    :param list input_files: A collection of input files. One task will be
     created for each input file.
    :param output_container_sas_token: A SAS token granting write access to
    the specified Azure Blob storage container.
    """

    print("Adding {} tasks to job [{}]...".format(len(input_files), job_id))

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

    batch_service_client.task.add_collection(job_id, tasks)


def deploy():
    start_time = datetime.datetime.now().replace(microsecond=0)
    print("Sample start: {}".format(start_time))
    print()

    # Create the blob client, for use in obtaining references to
    # blob storage containers and uploading files to containers.

    blob_client = azureblob.BlockBlobService(
        account_name=os.environ.get("_STORAGE_ACCOUNT_NAME"),
        account_key=os.environ.get("_STORAGE_ACCOUNT_KEY"),
    )

    # Use the blob client to create the containers in Azure Storage if they
    # don't yet exist.

    input_container_name = "input"
    blob_client.create_container(input_container_name, fail_on_exist=False)

    # The collection of data files that are to be processed by the tasks.
    input_file_paths = [
        os.path.join("/Users/ondrej/Downloads/batch-python-quickstart/src/azbatch/taskdata0.txt"),
        os.path.join("/Users/ondrej/Downloads/batch-python-quickstart/src/azbatch/taskdata1.txt"),
        os.path.join("/Users/ondrej/Downloads/batch-python-quickstart/src/azbatch/taskdata2.txt"),
    ]

    # Upload the data files.
    input_files = [
        upload_file_to_container(blob_client, input_container_name, file_path)
        for file_path in input_file_paths
    ]

    # Create a Batch service client. We'll now be interacting with the Batch
    # service in addition to Storage
    credentials = batch_auth.SharedKeyCredentials(
        os.environ.get("_BATCH_ACCOUNT_NAME"), os.environ.get("_BATCH_ACCOUNT_KEY")
    )

    batch_client = batch.BatchServiceClient(
        credentials, batch_url=os.environ.get("_BATCH_ACCOUNT_URL")
    )

    try:
        # Create the pool that will contain the compute nodes that will execute the
        # tasks.
        create_pool(batch_client, config._POOL_ID)

        # Create the job that will run the tasks.
        create_job(batch_client, config._JOB_ID, config._POOL_ID)

        # Add the tasks to the job.
        add_tasks(batch_client, config._JOB_ID, input_files)

        # Pause execution until tasks reach Completed state.
        wait_for_tasks_to_complete(
            batch_client, config._JOB_ID, datetime.timedelta(minutes=30)
        )

        print(
            "  Success! All tasks reached the 'Completed' state within the "
            "specified timeout period."
        )

        # Print the stdout.txt and stderr.txt files for each task to the console
        print_task_output(batch_client, config._JOB_ID)

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
        batch_client.job.delete(config._JOB_ID)

    if query_yes_no("Delete pool?") == "yes":
        batch_client.pool.delete(config._POOL_ID)

    print()
    input("Press ENTER to exit...")


if __name__ == "__main__":
    sys.path.append('.')
    sys.path.append('..')
    deploy()

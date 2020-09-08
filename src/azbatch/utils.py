import datetime
import io
import os
import sys
import time

from azure.batch import models as batchmodels
from azure.storage import blob as azureblob


def query_yes_no(question, default="yes"):
    """
    Prompts the user for yes/no input, displaying the specified question text.

    :param str question: The text of the prompt for input.
    :param str default: The default if the user hits <ENTER>. Acceptable values
    are 'yes', 'no', and None.
    :rtype: str
    :return: 'yes' or 'no'
    """
    valid = {"y": "yes", "n": "no"}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("Invalid default answer: '{}'".format(default))

    while 1:
        choice = input(question + prompt).lower()
        if default and not choice:
            return default
        try:
            return valid[choice[0]]
        except (KeyError, IndexError):
            print("Please respond with 'yes' or 'no' (or 'y' or 'n').\n")


def print_batch_exception(batch_exception):
    """
    Prints the contents of the specified Batch exception.

    :param batch_exception:
    """
    print("-------------------------------------------")
    print("Exception encountered:")
    if (
        batch_exception.error
        and batch_exception.error.message
        and batch_exception.error.message.value
    ):
        print(batch_exception.error.message.value)
        if batch_exception.error.values:
            print()
            for mesg in batch_exception.error.values:
                print("{}:\t{}".format(mesg.key, mesg.value))
    print("-------------------------------------------")


def get_container_sas_token(block_blob_client, container_name, blob_permissions):
    """
    Obtains a shared access signature granting the specified permissions to the
    container.

    :param block_blob_client: A blob service client.
    :type block_blob_client: `azure.storage.blob.BlockBlobService`
    :param str container_name: The name of the Azure Blob storage container.
    :param BlobPermissions blob_permissions:
    :rtype: str
    :return: A SAS token granting the specified permissions to the container.
    """
    # Obtain the SAS token for the container, setting the expiry time and
    # permissions. In this case, no start time is specified, so the shared
    # access signature becomes valid immediately.
    container_sas_token = block_blob_client.generate_container_shared_access_signature(
        container_name,
        permission=blob_permissions,
        expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=2),
    )

    return container_sas_token


def wait_for_tasks_to_complete(batch_service_client, job_id, timeout):
    """
    Returns when all tasks in the specified job reach the Completed state.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str job_id: The id of the job whose tasks should be to monitored.
    :param timedelta timeout: The duration to wait for task completion. If all
    tasks in the specified job do not reach Completed state within this time
    period, an exception will be raised.
    """
    timeout_expiration = datetime.datetime.now() + timeout

    print(
        "Monitoring all tasks for 'Completed' state, timeout in {}...".format(timeout),
        end="",
    )

    while datetime.datetime.now() < timeout_expiration:
        print(".", end="")
        sys.stdout.flush()
        tasks = batch_service_client.task.list(job_id)

        incomplete_tasks = [
            task for task in tasks if task.state != batchmodels.TaskState.completed
        ]
        if not incomplete_tasks:
            print()
            return True
        else:
            time.sleep(1)

    print()
    raise RuntimeError(
        "ERROR: Tasks did not reach 'Completed' state within "
        "timeout period of " + str(timeout)
    )


def print_task_output(batch_service_client, job_id, stdout_name, encoding=None):
    """Prints the stdout.txt file for each task in the job.

    :param batch_client: The batch client to use.
    :type batch_client: `batchserviceclient.BatchServiceClient`
    :param str job_id: The id of the job with task output files to print.
    """

    print("Printing task output...")

    tasks = batch_service_client.task.list(job_id)

    for task in tasks:

        node_id = batch_service_client.task.get(job_id, task.id).node_info.node_id
        print("Task: {}".format(task.id))
        print("Node: {}".format(node_id))

        stream = batch_service_client.file.get_from_task(
            job_id, task.id, stdout_name
        )

        file_text = _read_stream_as_string(stream, encoding)
        print("Standard output:")
        print(file_text)


def _read_stream_as_string(stream, encoding):
    """Read stream as string

    :param stream: input stream generator
    :param str encoding: The encoding of the file. The default is utf-8.
    :return: The file content.
    :rtype: str
    """
    output = io.BytesIO()
    try:
        for data in stream:
            output.write(data)
        if encoding is None:
            encoding = "utf-8"
        return output.getvalue().decode(encoding)
    finally:
        output.close()
    raise RuntimeError("could not write data to stream or decode bytes")


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
import datetime

from typing import List, Optional, Dict

import azure.storage.blob as azureblob
import azure.batch.batch_service_client as batch
import azure.batch.models as batchmodels


NEO_IMAGE = "ada510.azurecr.io/neo:merged-python"


def create_sas_token(
        blob_client: azureblob.BlockBlobService,
        container_name: str,
        permissions: Optional[List[str]] = None,
        expire_in: Optional[datetime.timedelta] = None,
) -> str:
    permissions = permissions or ["read"]
    expire_in = expire_in or datetime.timedelta(days=1)

    return blob_client.generate_container_shared_access_signature(
        container_name=container_name,
        permission=azureblob.ContainerPermissions(**{opt: True for opt in permissions}),
        expiry=datetime.datetime.utcnow() + expire_in
    )


def create_resource_url(
        storage_account_name: str,
        container_name: str,
        sas_token: str,
        container_path: Optional[str] = None,
) -> str:
    container_path = container_path or ""
    storage_path = f"{container_name}/{container_path}".replace("//", "/")
    return f"https://{storage_account_name}.blob.core.windows.net/{storage_path}?{sas_token}"


def create_container_config(config: Dict[str, str]) -> batchmodels.ContainerConfiguration:
    ada_cr = batchmodels.ContainerRegistry(
        registry_server="ada510.azurecr.io",
        user_name="ada510",
        password=config["CR_PASSWORD"],
    )
    return batchmodels.ContainerConfiguration(
        container_image_names=[NEO_IMAGE],
        container_registries=[ada_cr],
    )


def create_pool(batch_service_client: batch.BatchServiceClient, config: Dict[str, str]) -> None:
    """
    Creates a pool of compute nodes with the specified OS settings.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param config: Configuration
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


def create_job(batch_service_client: batch.BatchServiceClient, config: Dict[str, str]) -> None:
    """
    Creates a job with the specified ID, associated with the specified pool.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param config: Config
    """
    print("Creating job [{}]...".format(config['JOB_ID']))

    job = batch.models.JobAddParameter(
        id=config['JOB_ID'], pool_info=batch.models.PoolInformation(pool_id=config['POOL_ID'])
    )
    batch_service_client.job.add(job)

import datetime
import os
import sys
import json
import logging

import azure.storage.blob as azureblob
import azure.batch.batch_service_client as batch
import azure.batch.batch_auth as batch_auth
import azure.batch.models as batchmodels
from itertools import chain

module_path = os.path.abspath(os.path.join('../src'))
if module_path not in sys.path:
    sys.path.append(module_path)

from src.azbatch import main
from src.azbatch.neo_functions import NeoTaskGenerator

from dotenv import load_dotenv


def initialize_config() -> dict:
    start_time = datetime.datetime.now().replace(microsecond=0)

    config = {
        "POOL_ID": "pool_0",  # f"job_{start_time.strftime('%Y%m%d%H%M%S')}",
        "JOB_ID": f"job_{start_time.strftime('%Y%m%d%H%M%S')}",
        "POOL_NODE_COUNT": 4,
        "POOL_VM_SIZE": "Standard_NC6_Promo",

        "BATCH_ACCOUNT_NAME": os.environ.get("_BATCH_ACCOUNT_NAME"),
        "BATCH_ACCOUNT_KEY": os.environ.get("_BATCH_ACCOUNT_KEY"),
        "BATCH_ACCOUNT_URL": os.environ.get("_BATCH_ACCOUNT_URL"),

        "CR_PASSWORD": os.environ.get("_CR_PASSWORD"),  # container registry

        "STORAGE_ACCOUNT_NAME": os.environ.get("_STORAGE_ACCOUNT_NAME"),
        "STORAGE_ACCOUNT_KEY": os.environ.get("_STORAGE_ACCOUNT_KEY"),

        "510_DLS_CONNECTION_STRING": os.environ.get("_510_DLS_CONNECTION_STRING"),
        "XCCTEST_CONNECTION_STRING": os.environ.get("_XCCTEST_CONNECTION_STRING")
    }

    return config


def setup_accounts(config: dict):
    shared_credentials = batch_auth.SharedKeyCredentials(
        account_name=config["BATCH_ACCOUNT_NAME"],
        key=config["BATCH_ACCOUNT_KEY"],
    )
    batch_client = batch.BatchServiceClient(
        credentials=shared_credentials,
        batch_url=config["BATCH_ACCOUNT_URL"]
    )
    blob_client_xcctest = azureblob.BlockBlobService(
        connection_string=config["XCCTEST_CONNECTION_STRING"]
    )
    blob_client_510 = azureblob.BlockBlobService(
        connection_string=config["510_DLS_CONNECTION_STRING"])

    return batch_client,blob_client_xcctest,blob_client_510


def create_pool(batch_client: batch.BatchServiceClient, config: dict) -> None:
    if not batch_client.pool.exists(config['POOL_ID']):
        main.create_pool(batch_client, config)
        logger.info(f"Created pool {config['POOL_ID']}.")
    else:
        logger.info(f"Pool {config['POOL_ID']} already exists.")


def create_job(batch_client: batch.BatchServiceClient, config: dict) -> None:
    # Create the job that will run the tasks.
    if not config['JOB_ID'] in [j.id for j in batch_client.job.list()]:
        main.create_job(batch_client, config)
        logger.info(f"Created job {config['JOB_ID']}.")
    else:
        logger.info(f"Job {config['JOB_ID']} already exists.")

def get_tile_index(container_name: str, data_dir: str, tile_filename: str, blob_xcc: azureblob.BlockBlobService) -> dict:
    blob_xcc.get_blob_to_path(container_name=container_name,
                                         blob_name=data_dir + f'/{tile_filename}',
                                         file_path=tile_filename)
    with open(tile_filename) as file:
        index = json.load(file)
        return index


def create_sas_tokens(blob_xcctest: azureblob.BlockBlobService, blob_client_510: azureblob.BlockBlobService):
    adafiles_read_token = main.create_sas_token(blob_xcctest, "adafiles", ["read", "list"])
    adafiles_write_token = main.create_sas_token(blob_xcctest, "adafiles", ["write"])
    _510_read_token = main.create_sas_token(blob_client_510, "neat-eo", ["read", "list"])
    adafiles_output_url = main.create_resource_url("xcctest", "adafiles", adafiles_write_token)
    return {"adafiles_read_token":adafiles_read_token,"adafiles_write_token":adafiles_write_token
               ,"_510_read_token":_510_read_token,"adafiles_output_url":adafiles_output_url}


def get_tasks(data_dir,batch_name,num_id,id_,tokens):
    neo = NeoTaskGenerator("ada510.azurecr.io/neo:latest",num_id,id_,data_dir,batch_name,tokens)
    setup = neo.setup_working_dir()
    cover = neo.cover()
    tile = neo.tile()
    predict = neo.predict()
    vectorize = neo.vectorize()
    return [setup,cover,tile,predict,vectorize]


if __name__ == '__main__':
    logger = logging.getLogger('azure_batch_neo')
    load_dotenv(dotenv_path="environment.env")

    data_dir = "beirut"
    batch_name = datetime.datetime.now().strftime('%Y%m%d%H%M%S')  # necessary to match dependencies

    config = initialize_config()
    _batch_client,_blob_client_xcc_test,_blob_client_510 = setup_accounts(config)

    #If not yet created
    # create_pool(_batch_client,config)
    # create_job(_batch_client,config)

    tokens = create_sas_tokens(_blob_client_xcc_test,_blob_client_510)
    index = get_tile_index("adafiles","beirut","tile_index.json",_blob_client_xcc_test)
    unique_ids = index.keys()
    #
    tasks = [get_tasks(data_dir,batch_name,num_id,id_,tokens) for num_id, id_ in enumerate(unique_ids)]
    flattened = list(chain.from_iterable(tasks))
    _batch_client.task.add_collection(config['JOB_ID'], flattened)


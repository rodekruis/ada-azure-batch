# ADA Azure Batch
Tools for deployment of Automated Damage Assessment tools by 510 on Azure Batch [wip].

Parallelizing existing pipeline (https://github.com/jmargutt/ADA_tools) via a dockerized
app (currently at https://github.com/ondrejzacha/ada-collection) run on Azure Batch.

## Dependencies
This project *depends* on a docker image built via https://github.com/ondrejzacha/ada-collection
and pushed to the Azure container registry `ada510.azurecr.io`.

## Manual work required
The following environment variables should be set manually (easily via a `.env` file):

Batch account information:
- `_BATCH_ACCOUNT_NAME`,
- `_BATCH_ACCOUNT_KEY`,
- `_BATCH_ACCOUNT_URL`,

Connection strings for `510datalakestorage` and `xcctest` blob storage accounts:
- `_510_DLS_CONNECTION_STRING`,
- `_XCCTEST_CONNECTION_STRING`,

Azure Container registry password for the `ada510.azurecr.io` registry server and user `ada510`:

- `_CR_PASSWORD`.


## Notes
This project is work in progress! For a document that can be useful for further 
development, see [NOTES.md](NOTES.md)

Adapted from https://github.com/Azure-Samples/batch-python-quickstart

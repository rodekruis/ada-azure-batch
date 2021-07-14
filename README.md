# ADA Azure Batch
Tools for deployment of Automated Damage Assessment tools by 510 on Azure Batch.

Parallelizing existing pipeline (https://github.com/rodekruis/ada-collection) via a dockerized
app (ada510.azurecr.io/ada:latest) run on Azure Batch.

## Dependencies
This project *depends* on a docker image built via https://github.com/rodekruis/ada-collection
and pushed to the Azure container registry `ada510.azurecr.io`.

## Usage (for 510 only)
1. Get access to the resource group [510Global-ADA](https://portal.azure.com/#@rodekruis.nl/resource/subscriptions/b2d243bd-7fab-4a8a-8261-a725ee0e3b47/resourceGroups/510Global-ADA/overview)
2. Create a new directory `<disaster-name>` in the container [adafiles](https://portal.azure.com/#blade/Microsoft_Azure_Storage/ContainerMenuBlade/overview/storageAccountId/%2Fsubscriptions%2Fb2d243bd-7fab-4a8a-8261-a725ee0e3b47%2FresourceGroups%2F510Global-ADA%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Fxcctest/path/adafiles/etag/%220x8D858B9664A591C%22/defaultEncryptionScope/%24account-encryption-key/denyEncryptionScopeOverride//defaultId//publicAccessVal/None) of the xcctest datalake (under 510Global-ADA)
3. Save pre- and post-disaster images in `<disaster-name>`:
      * automatically from Maxar Open Data (see [instructions](https://github.com/rodekruis/ada-collection#end-to-end-example))
      * manually (store them separately in two directories: `adafiles/<disaster-name>/post-event` and `adafiles/<disaster-name>/pre-event`)
6. Generate index (from [ada-collection](https://github.com/rodekruis/ada-collection/blob/master/ada_tools/src/ada_tools/create_index.py))
```
create-index --data adafiles/<disaster-name> --dest adafiles/<disaster-name>/tile_index.geojson
```
9. Copy credentials from BitWarden ("ada-azure-batch .env") and save them in
  `notebooks/`
2. Run the notebook `notebooks/neo-batch.ipynb`
4. N.B. Double-check that [all pools](https://portal.azure.com/#@rodekruis.nl/resource/subscriptions/b2d243bd-7fab-4a8a-8261-a725ee0e3b47/resourceGroups/510Global-ADA/providers/Microsoft.Batch/batchAccounts/510adagpu/accountPools) are actually deleted
3. Each node will produce an output in a different directory named according to index; merge all outputs using (from [ada-collection](https://github.com/rodekruis/ada-collection/blob/master/ada_tools/src/ada_tools/merge_output.py))
```
merge-output --dir adafiles/ --dest adafiles/<disaster-name>
```

NOTE: Azure batch jobs can be monitored from [here](https://portal.azure.com/#@rodekruis.nl/resource/subscriptions/b2d243bd-7fab-4a8a-8261-a725ee0e3b47/resourceGroups/510Global-ADA/providers/Microsoft.Batch/batchAccounts/510adagpu/accountJobs). Jobs will fail at task 'prepare-data' if no buildings were previously detected (to be fixed). 

## Credentials
The following environment variables should be set in the `.env` file:

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

# Notes
[wip]

## Best practices
Reuse pools and jobs -- pool creation takes a lot of time.
https://docs.microsoft.com/en-us/azure/batch/best-practices

## Persisting data
Data on Batch compute nodes is not persisted. Therefore, any inputs and outputs (that are not 
included in the container itself) have to be copied from and to the outside.

For that, Batch uses Azure blob storage. For inputs, user specifies *resource files* 
that are copied into the *working directory* of a task. Similarly, any outputs that
we want to keep need to be exported to the blob storage via an *output file* specification.  

- working directory inside containers: `$AZ_BATCH_TASK_WORKING_DIR/myfile.txt`
https://docs.microsoft.com/en-us/azure/batch/batch-compute-node-environment-variables
https://azure.microsoft.com/en-in/blog/running-docker-container-on-azure-batch/

- resource file specification types:
    - single file: `http_url`
    - whole folder: `storage_container_url` and `blob_prefix`
    
    - both patched on `file_path`
    
- output files: - similar to resource files, only container path is specified with a 
*write* permission SAS token.
    
- SAS tokens - every url for resource file or output file specification needs a SAS token;
these can be generated using a utility function in `main.py`. Keep in mind downloading 
resource files needs `"read"` and typically also `"list"` permissions,
 uploading output files needs `"write"` permissions for the correct container.
 
- task dependencies - WIP
- troubleshooting: "FileIOException: Failed to allocate xx bytes" error -- looks like some 
files are corrupted and deleting them helps? still not clear
 
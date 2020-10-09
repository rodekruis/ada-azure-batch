# Notes

## Best practices
Reuse pools and jobs -- pool creation takes a lot of time.
https://docs.microsoft.com/en-us/azure/batch/best-practices

For the ADA project, we intend to create a single pool (running on GPU-enabled instances).
Inside that pool, each initial image file should be assigned a job; within that job,
separate tasks would correspond to individual steps in the pipeline. At the very end,
a separate task (or even a locally run script) would collect intermediate results and 
combine them. 

## Persisting data
Data on Batch compute nodes is not persisted. Therefore, any inputs and outputs (that are not 
included in the container itself) have to be copied from and to the outside.

For that, Batch uses Azure blob storage. For inputs, user specifies *resource files* 
that are copied into the *working directory* of a task. Similarly, any outputs that
we want to keep need to be exported to the blob storage via an *output file* specification.

### Working directory
To facilitate transfer of files between the Batch compute node and blob storage container,
it is important to save them into the node's predefined *working directory*.

For container-based tasks (that we use here), it is the default container directory, 
also accessible via `$AZ_BATCH_TASK_WORKING_DIR`.   

https://docs.microsoft.com/en-us/azure/batch/batch-compute-node-environment-variables
https://azure.microsoft.com/en-in/blog/running-docker-container-on-azure-batch/

### Resource and output files
For tasks to use resource (input) files or output files, the file access needs to be
specified during the task definition. Such specification requires a path to a required
file or folder, along with the correct SAS token. 


#### Resource file specification
Resource files can be specified in two main ways:

- For a single file, exact URL path of the required file is passed using the argument `http_url`.
- For a whole folder, one specifies the URL of the whole container (`storage_container_url`),
 along with the `blob_prefix` -- path to the directory _within_ the container.

In both cases, URL needs to end with a SAS token with "read" and "list" permissions. 
When correctly specified, files will be copied
to the compute node to the location specified by `file_path`.
    
#### Output file specification
Specification is similar to resource files. Container path must be specified with a 
"write" permission SAS token.
 
## Task dependencies
To make sure tasks within a job run in the correct order, their dependencies
must be specified. This should be done using the `depends_on` parameter and 
`TaskDependencies(task_ids=[...])` model.
- Still WIP, does not work as intended yet!

## Next steps
- *Predict step* -- as the current batch account doesn't have access to GPU-enabled
 instances, the prediction step (`neo predict`) cannot run. Once these are available,
 the predict step should be rewritten to use the correct resource and output files. 
- *Unifying containers* -- to overcome the previously mentioned issue, 
 we use resource files from the `adadatalakestorage` container instead. Once
 we can create predictions inside the batch job, this second container should
 not be necessary; all files related to the project should be stored in one container.

## Troubleshooting: 
- `FileIOException: Failed to allocate xx bytes` error -- looks like some 
files inside the container are corrupted; deleting them seems to helps
 
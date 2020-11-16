import azure.batch.models as batchmodels
from src.azbatch import main
import azure.storage.blob as azureblob

class NeoTaskGenerator():
    def __init__(self, image_name,num_id,id_,data_dir,batch_name,tokens):
        self.image_name = image_name
        self.num_id = num_id
        self.id_ = id_
        self.data_dir = data_dir
        self.batch_name = batch_name
        self.tokens = tokens
        self.task_common_args, self.upload_opts = self.init_container_conn_settings()

    def init_container_conn_settings(self):
        # common settings
        task_container_settings = batchmodels.TaskContainerSettings(
            image_name=self.image_name,
            # ipc=host needed for pytorch to share memory
            # https://discuss.pytorch.org/t/unable-to-write-to-file-torch-18692-1954506624/9990
            container_run_options='--rm --ipc=host'
        )
        # needed to create folders inside running container
        user = batchmodels.AutoUserSpecification(
                scope='pool',
                elevation_level='admin',
            )
        admin_identity = batchmodels.UserIdentity(auto_user=user)
        task_common_args = {
            "container_settings": task_container_settings,
            "user_identity": admin_identity,
        }
        upload_opts = batchmodels.OutputFileUploadOptions(
            upload_condition=batchmodels.OutputFileUploadCondition.task_success
        )

        return task_common_args,upload_opts

    def setup_working_dir(self,):
        command = f'/bin/bash -c "setup-wd --data {self.data_dir} --index tile_index.json --id {self.id_} --dest images"'
        resource_file_storage = batchmodels.ResourceFile(
                    storage_container_url=main.create_resource_url("xcctest", "adafiles", self.tokens["adafiles_read_token"]),
                    blob_prefix=f'{self.data_dir}/'
                )
        resource_file_tiles = batchmodels.ResourceFile(
                    http_url=main.create_resource_url("xcctest", "adafiles", self.tokens["adafiles_read_token"],
                                                      container_path=f"{self.data_dir}/tile_index.json"),
                    file_path='tile_index.json'
                )
        images_output_file = batchmodels.OutputFile(
                    file_pattern="images/**/*.tif",
                    destination=batchmodels.OutputFileDestination(
                        container=batchmodels.OutputFileBlobContainerDestination(
                            container_url=self.tokens["adafiles_output_url"],
                            path=f"{self.id_}/images",
                        )
                    ),
                    upload_options=self.upload_opts,
                )

        setup_task = batchmodels.TaskAddParameter(
            id=f"setup-{self.batch_name}-{self.num_id}",
            depends_on=None,
            command_line=command,
            resource_files=[resource_file_storage,resource_file_tiles],
            output_files=[images_output_file],
            **self.task_common_args,
        )
        return setup_task

    def cover(self):
        images_resource = batchmodels.ResourceFile(
                    http_url=main.create_resource_url("xcctest", "adafiles", self.tokens["adafiles_read_token"],
                                                      container_path=f"{self.id_}/images/pre-event/merged.tif"),
                    file_path='merged.tif'
                )
        cover_output = batchmodels.OutputFile(
                    file_pattern="cover.csv",
                    destination=batchmodels.OutputFileDestination(
                        container=batchmodels.OutputFileBlobContainerDestination(
                            container_url=self.tokens["adafiles_output_url"],
                            path=f"{self.id_}/cover.csv",
                        )
                    ),
                    upload_options=self.upload_opts,
                )
        cover_task = batchmodels.TaskAddParameter(
            id=f"cover-{self.batch_name}-{self.num_id}",
            depends_on=batchmodels.TaskDependencies(task_ids=[f"setup-{self.batch_name}-{self.num_id}"]),
            command_line=f'/bin/bash -c "neo cover --raster merged.tif --zoom 17 --out cover.csv"',
            resource_files=[images_resource],
            output_files=[cover_output],
            **self.task_common_args,
        )
        return cover_task

    def tile(self):
        command = f'/bin/bash -c "neo tile --raster merged.tif --zoom 17 --cover cover.csv --config config.toml --out tiles --format tif"'
        merging_resource = self.create_resource_file("xcctest","adafiles",self.tokens["adafiles_read_token"],
                                                     f"{self.id_}/images/pre-event/merged.tif",'merged.tif')
        config_resource = self.create_resource_file("xcctest","adafiles",self.tokens["adafiles_read_token"],"config.toml","config.toml")
        cover_resource = self.create_resource_file("xcctest","adafiles",self.tokens["adafiles_read_token"],f"{self.id_}/cover.csv","cover.csv")
        tiles_output = batchmodels.OutputFile(
                    file_pattern="tiles/**/*.tif",
                    destination=batchmodels.OutputFileDestination(
                        container=batchmodels.OutputFileBlobContainerDestination(
                            container_url=self.tokens["adafiles_output_url"],
                            path=f"{self.id_}/tiles",
                        )
                    ),
                    upload_options=self.upload_opts,
                )

        tile_task = batchmodels.TaskAddParameter(
            id=f"tile-{self.batch_name}-{self.num_id}",
            depends_on=batchmodels.TaskDependencies(task_ids=[f"cover-{self.batch_name}-{self.num_id}"]),
            command_line=command,
            resource_files=[merging_resource,config_resource,cover_resource],
            output_files=[tiles_output],
            **self.task_common_args,
        )
        return tile_task

    def predict(self):
        command = '/bin/bash -c "neo predict --config config.toml --cover cover.csv --dataset neo_files --checkpoint neat-fullxview-epoch75.pth --out predictions --metatiles --keep_borders"'
        processed_resource = self.create_resource_file("xcctest","adafiles",
                                                       self.tokens["adafiles_read_token"],"taskout/ada/pre-event/103001007E413300-3013212.tif","processed.tif")
        datalake_resource = self.create_resource_file("adadatalakestorage", "neat-eo"
                                                      , self.tokens["_510_read_token"],"models/neat-fullxview-epoch75.pth","neat-fullxview-epoch75.pth")
        config_resource = self.create_resource_file("xcctest", "adafiles", self.tokens["adafiles_read_token"], "config.toml","config.toml")

        cover_resource = self.create_resource_file("xcctest", "adafiles", self.tokens["adafiles_read_token"], "cover.csv","cover.csv")

        files_resource = batchmodels.ResourceFile(
            storage_container_url=main.create_resource_url("xcctest", "adafiles", self.tokens["adafiles_read_token"]),
            blob_prefix='images/',
            file_path='neo_files/',
        )

        output_predictions = batchmodels.OutputFile(
                file_pattern="predictions/**/*",
                destination=batchmodels.OutputFileDestination(
                    container=batchmodels.OutputFileBlobContainerDestination(
                        container_url=self.tokens["adafiles_output_url"],
                        path="predictions",
                    )
                ),
                upload_options=self.upload_opts,
            )

        predict_task = batchmodels.TaskAddParameter(
            id=f"predict-{self.batch_name}-{self.num_id}",
            depends_on=batchmodels.TaskDependencies(task_ids=[f"cover-{self.batch_name}-{self.num_id}", f"tile-{self.batch_name}-{self.num_id}"]),
            command_line= command,
            resource_files=[processed_resource,datalake_resource,config_resource,cover_resource,files_resource],
            output_files=[output_predictions],
            **self.task_common_args,
        )

        return predict_task

    def vectorize(self):
        command = '/bin/bash -c "neo vectorize --config config.toml --masks predictions --out buildings.geojson --type Building"'

        config_resource = self.create_resource_file("xcctest", "adafiles", self.tokens["adafiles_read_token"], "config.toml","config.toml")
        prediction_resource = batchmodels.ResourceFile(
                            storage_container_url=main.create_resource_url("xcctest", "adafiles", self.tokens["adafiles_read_token"]),
                            blob_prefix="predictions/",
                        )
        output = batchmodels.OutputFile(
                            file_pattern="buildings.geojson",
                            destination=batchmodels.OutputFileDestination(
                                container=batchmodels.OutputFileBlobContainerDestination(
                                    container_url=self.tokens["adafiles_output_url"],
                                    path="buildings.geojson",
                                )
                            ),
                            upload_options=self.upload_opts,
                        )

        vectorize_task = batchmodels.TaskAddParameter(
                    id=f"vectorize-{self.batch_name}-{self.num_id}",
                    depends_on=batchmodels.TaskDependencies(task_ids=[f"predict-{self.batch_name}-{self.num_id}"]),
                    command_line=command,
                    resource_files=[prediction_resource,config_resource],
                    output_files=[output],
                    **self.task_common_args,
                )
        return vectorize_task

    def cleanup_resources(self,config,batch_client):
        batch_client.job.delete(config['JOB_ID'])
        batch_client.pool.delete(config['POOL_ID'])
        # delete all jobs
        for job in batch_client.job.list():
            batch_client.job.delete(job.id)

        # delete all pools
        for pool in batch_client.pool.list():
            batch_client.pool.delete(pool.id)

    def create_resource_file(self,storage_account, container, token,container_path, file_path):
        return batchmodels.ResourceFile(
            http_url=main.create_resource_url(storage_account,container, token,
                                              container_path=container_path),
            file_path=file_path
        )

"""
Interacts with Azure Data Lake.

"""
from airflow.contrib.hooks.azure_data_lake_hook import AzureDataLakeHook

from imars_etl.object_storage.BaseObjectHook import BaseObjectHook
from imars_etl.filepath.format_filepath import format_filepath


class AzureDataLakeObjectHook(AzureDataLakeHook, BaseObjectHook):

    def __init__(
        self, *args,
        **kwargs
    ):
        kwargs.setdefault('source', None)
        kwargs['azure_data_lake_conn_id'] = "imars_etl_azure_lake_conn_id"
        super(AzureDataLakeObjectHook, self).__init__(
            *args, **kwargs
        )

    def load(self, filepath=None, **kwargs):
        remote_target_path = format_filepath(**kwargs)
        self.upload_file(
            local_path=filepath,
            remote_path=remote_target_path
        )
        return remote_target_path

    def extract(self, src_path, target_path, **kwargs):
        self.download_file(local_path=target_path, remote_path=src_path)
        return target_path

"""
Provides wrapper for airflow.contrib.hooks.AzureDataLake-like object storage
hooks.
"""
from imars_etl.filepath.format_filepath import format_filepath
from imars_etl.object_storage.hook_wrappers.BaseHookWrapper \
    import BaseHookWrapper


class DataLakeHookWrapper(BaseHookWrapper):

    REQUIRED_ATTRS = {
        'load': ['upload_file'],
        'extract': ['download_file'],
        'format_filepath': [],
    }

    def load(self, filepath=None, **kwargs):
        local_src_path = kwargs['filepath']
        remote_target_path = self.format_filepath(**kwargs)
        # print("\n\n\t{}\n\n".format(kwargs))
        if kwargs.get('dry_run', False) is False:
            self.hook.upload_file(
                local_path=local_src_path,
                remote_path=remote_target_path
            )
        return remote_target_path

    def extract(self, src_path, target_path, **kwargs):
        self.hook.download_file(local_path=target_path, remote_path=src_path)
        return target_path

    def format_filepath(self, **kwargs):
        format_filepath(self.hook, **kwargs)

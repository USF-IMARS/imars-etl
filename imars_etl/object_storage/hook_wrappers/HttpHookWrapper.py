"""
Provides wrapper for airflow.hooks.http_hook-like object storage
hooks.
"""
from imars_etl.filepath.format_filepath import format_filepath
from imars_etl.object_storage.hook_wrappers.BaseHookWrapper \
    import BaseHookWrapper


class HttpHookWrapper(BaseHookWrapper):

    REQUIRED_ATTRS = {
        'load': [],
        'extract': ['run'],
        'format_filepath': [],
    }

    def load(self, filepath=None, **kwargs):
        raise NotImplementedError('http loading not (yet) supported')

    def extract(self, src_path, target_path_or_file_handle):
        response = self.hook.run(
            endpoint=src_path
        )
        try:  # target_path_or_file_handle is file handle
            tgt_file = target_path_or_file_handle
            tgt_file.write(response.content)
        except AttributeError:  # eg: 'str' object has no attribute 'write'
            # target_path_or_file_handle is str filepath
            with open(target_path_or_file_handle, 'w') as tgt_file:
                tgt_file.write(response.content)
        return target_path_or_file_handle

    def format_filepath(self, **kwargs):
        format_filepath(self.hook, **kwargs)

"""
Provides wrapper for airflow.hooks.http_hook-like object storage
hooks.
"""
import logging
import sys

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

    def extract(self, src_path, target_path_or_file_handle, **kwargs):
        logger = logging.getLogger("{}.{}".format(
            __name__,
            sys._getframe().f_code.co_name)
        )
        logger.info("fetching URL http://{} / {}".format(
            self.hook.base_url, src_path
        ))
        response = self.hook.run(
            endpoint=src_path
        )
        try:  # target_path_or_file_handle is file handle
            tgt_file = target_path_or_file_handle
            tgt_file.write(response.content)
            logger.info("used given file handle")
        except AttributeError:  # eg: 'str' object has no attribute 'write'
            # target_path_or_file_handle is str filepath
            logger.info("opening file '{}'".format(target_path_or_file_handle))
            with open(target_path_or_file_handle, 'wb') as tgt_file:
                tgt_file.write(response.content)
        return target_path_or_file_handle

    def format_filepath(self, **kwargs):
        format_filepath(self.hook, **kwargs)

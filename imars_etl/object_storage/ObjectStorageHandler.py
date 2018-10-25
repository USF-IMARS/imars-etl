"""
Connections are managed using apache-airflow hooks.
Connections can be installed using the `airflow connections` command.
Within the code connections are wrapped in a few ways to unify backend
interactions down to two interfaces: "object_storage" and "metadata_db".

The hooks provided by airflow often have differing interfaces.
Multiple HookWrappers are provided here to wrap around one or more hook classes
and provide a consistent interface to object_storage or metadata_db backends.
Two HookHandlers (ObjectStorageHandler and MetadataDBHandler) are provided
to serve as the topmost interface.
HookHandlers apply the appropriate wrappers to hooks to provide either a
metadata or object-store interface, so users don't have to worry about
HookHandlers.

```
# OSI-like model:

        |-------------------------------------+-----------------------------|
methods | .load() .extract()                  | .get_first() .get_records() |
        | .format_filepath()                  | .insert_rows()              |
        |-------------------------------------|-----------------------------|
Handles | ObjectStorageHandler                | MetadataDBHandler           |
        |---------------+---------------------|-----------------------------|
Wrappers| FSHookWrapper | DataLakeHookWrapper |                             |
        |---------------|-----+---------------|-----------------------------|
Hooks   |       FS      | S3  | AzureDataLake | DbAPIHook                   |
        |---------------|-----|---------------|------------+-------+--------|
Backends| HD| NFS| FUSE | S3  | Azure         | MySQL      | MsSQL | SQLite |
        |---------------+-----+---------------+------------+-------+--------+
```
"""
import logging
import sys

from imars_etl.get_hook import get_hook
from imars_etl.object_storage.hook_wrappers.DataLakeHookWrapper \
    import DataLakeHookWrapper
from imars_etl.object_storage.hook_wrappers.FSHookWrapper \
    import FSHookWrapper
from imars_etl.object_storage.hook_wrappers.BaseHookWrapper \
    import WrapperMismatchException


class ObjectStorageHandler(object):
    def __init__(self, **kwargs):
        self.obj_store_hook = get_hook(kwargs['object_store'])

    def load(self, **kwargs):
        # NOTE: in the future we could do better than just guessing
        return self._guess_load_hook_handlers(**kwargs)

    def extract(self, src_path, target_path, **kwargs):
        # assume azure_data_lake-like interface:
        self.obj_store_hook.download_file(
            local_path=target_path, remote_path=src_path
        )
        return target_path

    def format_filepath(self):
        # TODO
        raise NotImplementedError()

    def _guess_load_hook_handlers(self, **kwargs):
        """ tries hook handlers until one works """
        # load file into IMaRS data warehouse
        logger = logging.getLogger("{}.{}".format(
            __name__,
            sys._getframe().f_code.co_name)
        )
        result = None

        try:  # direct usage (no wrapper)
            result = self.obj_store_hook.load(**kwargs)
        except AttributeError:
            logger.debug('raw hook failed')

        try:  # azure_data_lake-like interface:
            result = DataLakeHookWrapper(self.obj_store_hook).load(**kwargs)
        except WrapperMismatchException:
            logger.debug('hook not DataLake-like')

        try:
            result = FSHookWrapper(self.obj_store_hook).load(**kwargs)
        except WrapperMismatchException:
            logger.debug('hook not FSHook-like')

        if result is None:
            raise AttributeError(
                "hook '{}' has unknown interface.".format(self.obj_store_hook)
            )
        else:
            return result

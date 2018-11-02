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
from imars_etl.BaseHookHandler import BaseHookHandler
from imars_etl.object_storage.hook_wrappers.DataLakeHookWrapper \
    import DataLakeHookWrapper
from imars_etl.object_storage.hook_wrappers.FSHookWrapper \
    import FSHookWrapper
from imars_etl.object_storage.hook_wrappers.HttpHookWrapper \
    import HttpHookWrapper

OBJECT_WRAPPERS = [DataLakeHookWrapper, FSHookWrapper, HttpHookWrapper]
DEFAULT_OBJ_STORE_CONN_ID = (
    "fallback_chain."
    "local_tmp.imars_objects.imars_http"
)


class ObjectStorageHandler(BaseHookHandler):
    def __init__(self, **kwargs):
        super(ObjectStorageHandler, self).__init__(
            hook_conn_id=kwargs.get('object_store', DEFAULT_OBJ_STORE_CONN_ID),
            wrapper_classes=OBJECT_WRAPPERS,
        )

    def load(self, **kwargs):
        # _guess_wrapper(h, 'load', **kwargs) for h in self.obj_store_hooks
        # ... but catching exceptions
        return self.try_hooks_n_wrappers(
            method='load',
            m_kwargs=kwargs
        )

    def extract(self, src_path, target_path, **kwargs):
        return self.try_hooks_n_wrappers(
            method='extract',
            m_args=[src_path, target_path],
            m_kwargs=kwargs
        )

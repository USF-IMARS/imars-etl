import os

from imars_etl.get_hook import get_hook
from imars_etl.util.get_sql_result import get_sql_result
from imars_etl.object_storage.IMaRSObjectsObjectHook \
    import IMaRSObjectsObjectHook

EXTRACT_DEFAULTS = {
    'storage_driver': "imars_objects"
}

STORAGE_DRIVERS = {  # map from input strings to extract fn for each backend
    'imars_objects': IMaRSObjectsObjectHook().extract,
}


def extract(
    sql,
    output_path=None,
    storage_driver=EXTRACT_DEFAULTS['storage_driver'],
    first=False,
    metadata_conn_id="imars_metadata",  # TODO: add this to the CLI
    object_store_conn_id="imars_object_store",  # TODO: add this to the CLI
    **kwargs
):
    """
    Example usage:
    ./imars-etl.py -vvv extract 'area_id=1'
    """
    full_sql_str = "SELECT filepath FROM file WHERE {}".format(sql)

    result = get_sql_result(full_sql_str, conn_id=metadata_conn_id)

    src_path = result['filepath']

    if output_path is None:
        output_path = "./" + os.path.basename(src_path)

    # use driver to download & then print a path to where the file can be
    # accessed on the local machine.
    fpath = _extract(
        object_store_conn_id,
        src_path,
        target_path=output_path,
        storage_driver=storage_driver,
        first=False,
        **kwargs
    )

    print(fpath)
    return fpath


def _extract(obj_store_conn_id, src_path, target_path, **kwargs):
    obj_store_hook = get_hook(obj_store_conn_id)
    # assume azure_data_lake-like interface:
    obj_store_hook.download_file(local_path=target_path, remote_path=src_path)
    return target_path

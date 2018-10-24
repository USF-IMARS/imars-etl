import os

from imars_etl.get_hook import get_hook
from imars_etl.get_hook import DEFAULT_OBJ_STORE_CONN_ID
from imars_etl.util.get_sql_result import get_sql_result
from imars_etl.metadata_db import DEFAULT_METADATA_DB_CONN_ID

EXTRACT_DEFAULTS = {
}


def extract(
    sql,
    output_path=None,
    first=False,
    metadata_conn_id=DEFAULT_METADATA_DB_CONN_ID,
    object_store=DEFAULT_OBJ_STORE_CONN_ID,
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

    # use connection to download & then print a path to where the file can be
    # accessed on the local machine.
    fpath = _extract(
        object_store,
        src_path,
        target_path=output_path,
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

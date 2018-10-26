import os

from imars_etl.object_storage.ObjectStorageHandler import ObjectStorageHandler
from imars_etl.metadata_db.MetadataDBHandler import MetadataDBHandler
from imars_etl.BaseHookHandler import DEFAULT_OBJ_STORE_CONN_ID
from imars_etl.BaseHookHandler import DEFAULT_METADATA_DB_CONN_ID

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

    metadata_db = MetadataDBHandler(
        metadata_db=metadata_conn_id,
    )
    result = metadata_db.get_records(
        full_sql_str,
        first=first,
        **kwargs
    )

    src_path = result['filepath']

    if output_path is None:
        output_path = "./" + os.path.basename(src_path)

    object_storage = ObjectStorageHandler(
        sql=sql,
        output_path=output_path,
        first=first,
        metadata_conn_id=metadata_conn_id,
        object_store=object_store,
        **kwargs
    )
    # use connection to download & then print a path to where the file can be
    # accessed on the local machine.
    fpath = object_storage.extract(
        src_path,
        target_path=output_path,
        first=False,
        **kwargs
    )

    print(fpath)
    return fpath

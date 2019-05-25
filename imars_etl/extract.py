import os

from imars_etl.object_storage.ObjectStorageHandler import ObjectStorageHandler
from imars_etl.object_storage.ObjectStorageHandler \
    import DEFAULT_OBJ_STORE_CONN_ID
from imars_etl.metadata_db.MetadataDBHandler import MetadataDBHandler
from imars_etl.metadata_db.MetadataDBHandler import DEFAULT_METADATA_DB_CONN_ID
from imars_etl.config_logger import config_logger

EXTRACT_DEFAULTS = {
}


def extract(
    sql,
    output_path=None,
    first=False,
    metadata_conn_id=DEFAULT_METADATA_DB_CONN_ID,
    object_store=DEFAULT_OBJ_STORE_CONN_ID,
    verbose=0,
    link=False,
    **kwargs
):
    """
    Example usage:
    ./imars-etl.py -vvv extract 'area_id=1'
    """
    config_logger(verbose)
    full_sql_str = "SELECT filepath FROM file WHERE {}".format(sql)

    metadata_db = MetadataDBHandler(
        metadata_db=metadata_conn_id,
    )
    try:
        result = metadata_db.get_records(
            full_sql_str,
            first=first,
        )
    except TooManyMetadataMatchesException:
        # === handle acceptable duplicate multihash entries (issue #41)
        if first=True:  # shouldn't get here if first=True anyway
            raise
        multihash_sql = full_sql_str.replace(
            "SELECT filepath FROM file ",
            "SELECT multihash FROM file "
        )
        multihashes = metadata_db.get_records(
            multihash_sql,
            check_result=False
        )
        # if all list elements are equal
        if multihashes.count(multihashes[0]) == len(multihashes):
            result = metadata_db.get_records(
                full_sql_str,
                first=True,
            )

    src_path = result[0]

    if output_path is None:
        output_path = "./" + os.path.basename(src_path)

    if link:
        raise NotImplementedError("symlink creation NYI.")

    object_storage = ObjectStorageHandler(
        sql=sql,
        output_path=output_path,
        first=first,
        link=link,
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
        link=link,
        **kwargs
    )

    return fpath

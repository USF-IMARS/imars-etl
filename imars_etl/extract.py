import os

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
    conn_id="imars_metadata_database_default",  # TODO: add this to the CLI
    **kwargs
):
    """
    Example usage:
    ./imars-etl.py -vvv extract 'area_id=1'
    """
    full_sql_str = "SELECT filepath FROM file WHERE {}".format(sql)

    result = get_sql_result(full_sql_str, conn_id=conn_id)

    src_path = result['filepath']

    if output_path is None:
        output_path = "./" + os.path.basename(src_path)

    # use driver to download & then print a path to where the file can be
    # accessed on the local machine.
    fpath = STORAGE_DRIVERS[
        storage_driver
    ](
        src_path,
        target_path=output_path,
        storage_driver=storage_driver,
        first=False,
        **kwargs
    )

    print(fpath)
    return fpath

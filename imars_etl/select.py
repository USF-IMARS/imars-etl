
from imars_etl.metadata_db.MetadataDBHandler import MetadataDBHandler
from imars_etl.metadata_db.MetadataDBHandler import DEFAULT_METADATA_DB_CONN_ID


def select(
    sql='',
    cols='*',
    first=False,
    metadata_conn_id=DEFAULT_METADATA_DB_CONN_ID,
    **kwargs
):
    """
    Prints json-formatted metadata for first entry in given args.sql

    args can be dict or argparse.Namespace

    Example usage:
        imars-etl select -cols 'filepath,date_time' 'area_id=1'

    returns:
    --------
    metadata : dict
        metadata from db

    """
    sql_query = "SELECT {} FROM file WHERE {};".format(cols, sql)
    print(sql_query)
    metadata_db = MetadataDBHandler(
        metadata_db=metadata_conn_id,
    )
    result = metadata_db.get_records(
        sql_query,
        first=first,
    )

    print(result)
    return result

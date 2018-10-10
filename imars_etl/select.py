
from imars_etl.util import get_sql_result
from imars_etl.metadata_db import DEFAULT_METADATA_CONN_ID


def select(
    sql='',
    cols='*',
    first=False,
    metadata_conn_id=DEFAULT_METADATA_CONN_ID,
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
    result = get_sql_result(
        sql_query,
        conn_id=metadata_conn_id,
        first=first,
    )
    print(result)
    return result

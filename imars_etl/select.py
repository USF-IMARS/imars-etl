
from imars_etl.metadata_db.MetadataDBHandler import MetadataDBHandler
from imars_etl.metadata_db.MetadataDBHandler import DEFAULT_METADATA_DB_CONN_ID


def select(
    sql='',
    cols='*',
    post_where='',
    first=False,
    metadata_conn_id=DEFAULT_METADATA_DB_CONN_ID,
    **kwargs
):
    """
    Prints json-formatted metadata for first entry in given args.sql

    args can be dict or argparse.Namespace

    Example usage:
        python -m imars_etl -v select 'area_id=1' \
            --cols 'filepath,date_time' \
            --post_where 'ORDER BY last_processed ASC LIMIT 1'
    returns:
    --------
    metadata : dict
        metadata from db

    """
    assert ';' not in sql  # lazy SQL injection check
    assert ';' not in cols
    assert ';' not in post_where
    sql_query = "SELECT {} FROM file WHERE {} {};".format(
        cols, sql, post_where
    )
    # logger.debug(sql_query)
    metadata_db = MetadataDBHandler(
        metadata_db=metadata_conn_id,
    )
    result = metadata_db.get_records(
        sql_query,
        first=first,
    )

    return result

import logging

from imars_etl.metadata_db.MetadataDBHandler import MetadataDBHandler
from imars_etl.metadata_db.MetadataDBHandler import DEFAULT_METADATA_DB_CONN_ID
from imars_etl.config_logger import config_logger


def select(
    sql='',
    cols='*',
    post_where='',
    first=False,
    metadata_conn_id=DEFAULT_METADATA_DB_CONN_ID,
    verbose=0,
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
    logger = logging.getLogger("imars_etl.{}".format(
        __name__,
    ))
    config_logger(verbose)

    assert ';' not in sql  # lazy SQL injection check
    assert ';' not in cols
    assert ';' not in post_where

    # updated select() allows more direct usage of SQL:
    V2_KEYWORDS = ['WHERE', 'LIMIT', 'GROUP', 'BY', 'JOIN', 'ORDER']
    if any(k in sql.upper().split() for k in V2_KEYWORDS):
        logger.debug("using newer select() API v >= 0.9.0")
        if post_where != '':
            raise ValueError(
                "Cannot use post_where when sql contains:\n\t{}".format(
                    V2_KEYWORDS
                )
            )
        sql_query = "SELECT {} FROM file {}".format(
            cols, sql
        )
    else:  # fallback to old (v0.8.8) select() syntax
        logger.debug("using older select() API v <= 0.8.8")
        sql_query = "SELECT {} FROM file WHERE {} {}".format(
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

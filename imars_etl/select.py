import logging

from imars_etl.metadata_db.mysql import meta_db_select
from imars_etl.metadata_db.mysql import check_result
from imars_etl.util.config_logger import config_logger


def _output_formatter_unix(output_result):
    """
    Converts sql output_result of shape [[]] into a unix-friendly string.
    Rows are split by newlines and columns are split by tabs.
    """
    output_str = ""
    for row in output_result:
        for col in row:
            try:  # use isoformat for datetimes instead of str()
                col_str = col.isoformat()
            except AttributeError:
                col_str = str(col)
            output_str += col_str.replace('"', '').replace("'", "") + "\t"
        output_str += "\n"
    return output_str


SELECT_OUTPUT_FORMATTERS = {
    "py_obj": lambda x: x,
    "unix": _output_formatter_unix,
}


def select(
    sql='',
    cols='*',
    post_where='',
    verbose=0,
    format=SELECT_OUTPUT_FORMATTERS["py_obj"],
    **kwargs  # NOTE: these get thrown out
):
    logger = logging.getLogger("imars_etl.{}".format(
        __name__,
    ))
    if len(kwargs) > 0:
        logger.warning(
            "Throwing out unrecognized kwargs: \n\t{}".format(kwargs)
        )
    return format(
        _select(sql, cols, post_where, verbose)
    )


def _select(sql, cols, post_where, verbose):
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
    result = meta_db_select(
        sql_query
    )
    check_result(result, min_results=1)
    return result

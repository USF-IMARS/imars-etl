import logging
import sys

from airflow.hooks.mysql_hook import MySqlHook

from imars_etl.exceptions.NoMetadataMatchException \
    import NoMetadataMatchException
from imars_etl.exceptions.TooManyMetadataMatchesException \
    import TooManyMetadataMatchesException


def get_sql_result(
    sql, first=True, check_result=True, should_commit=False,
    conn_id="imars_metadata_database_default",
):
    """
    Parameters:
    -----------
    sql : str
        sql query to execute
    check_result : bool
        Should we check the response for possible errors? Example errors:
            * more than 1 result returned
            * less than 1 result returned
    should_commit : bool
        Connection is not autocommit by default so you must commit to
        save changes to the database.
    """
    object_metadata_hook = MySqlHook(
        mysql_conn_id=conn_id,
    )

    if first is True:
        result = object_metadata_hook.get_first(sql)
    else:
        result = object_metadata_hook.get_records(sql)

    result = validate_sql_result(result)


def validate_sql_result(result):
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )

    if (not result):
        raise NoMetadataMatchException(
            "Zero files found matching given metadata."
        )
        # exit(EXIT_STATUS.NO_MATCHING_FILES)
    elif (len(result) > 1):
        # TODO: request more info from user?
        logger.error(result)
        raise TooManyMetadataMatchesException(
            "Too many results found matching given metadata."
        )
        # exit(EXIT_STATUS.MULTIPLE_MATCH)
    else:
        return result[0]

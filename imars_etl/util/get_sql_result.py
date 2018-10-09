import logging
import sys

from imars_etl import metadatabase
from imars_etl.exceptions.NoMetadataMatchException \
    import NoMetadataMatchException
from imars_etl.exceptions.TooManyMetadataMatchesException \
    import TooManyMetadataMatchesException


def get_sql_result(
    sql, first=True, check_result=True, should_commit=False
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
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )

    connection = metadatabase.get_conn()
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql)

            if first is True:
                result = [cursor.fetchone()]
            else:
                result = cursor.fetchmany(2)

            if check_result is True:
                validate_sql_result(result)

            if should_commit:
                connection.commit()

            try:
                return result[0]
            except IndexError:
                if check_result:
                    raise
                else:  # we don't care about the error
                    logger.info("no result from sql request")
                    return result
    finally:
        connection.close()


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
        return

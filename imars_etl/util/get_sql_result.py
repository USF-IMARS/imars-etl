import logging
import sys

from imars_etl import metadatabase
from imars_etl.util.exit_status import EXIT_STATUS

def get_sql_result(args, sql, check_result=True, should_commit=False):
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )

    connection = metadatabase.get_conn()
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql)

            if getattr(args, "first", False) is True:
                result = [cursor.fetchone()]
            else:
                result = cursor.fetchmany(2)

            if (check_result and not result):
                logger.error("No files found matching given metadata")
                exit(EXIT_STATUS.NO_MATCHING_FILES)
            elif (check_result and len(result) > 1):
                # TODO: request more info from user?
                logger.error("Too many results!")
                logger.error(result)
                exit(EXIT_STATUS.MULTIPLE_MATCH)
            else:
                if should_commit:
                    # connection is not autocommit by default.
                    # So you must commit to save your changes.
                    connection.commit()
                try:
                    return result[0]
                except IndexError as i_err:
                    if check_result:
                        raise
                    else:  # we don't care about the error
                        logger.info("no result from sql request")
                        return result


    finally:
        connection.close()
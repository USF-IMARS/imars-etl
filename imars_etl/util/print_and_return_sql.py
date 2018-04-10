import logging
import sys

from imars_etl import metadatabase
from imars_etl.util.exit_status import EXIT_STATUS

def print_and_return_sql(args, sql):
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )

    connection = metadatabase.get_conn()
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql)

            if args.first is True:
                result = [cursor.fetchone()]
            else:
                result = cursor.fetchmany(2)
                
            if (not result):
                logger.error("No files found matching given metadata")
                exit(EXIT_STATUS.NO_MATCHING_FILES)
            elif (len(result) > 1):
                # TODO: request more info from user?
                logger.error("Too many results!")
                logger.error(result)
                exit(EXIT_STATUS.MULTIPLE_MATCH)
            else:
                print(result[0])
                return result[0]

    finally:
        connection.close()

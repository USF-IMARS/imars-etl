import logging
import sys

from imars_etl import metadatabase
from imars_etl.util import dict_to_argparse_namespace
from imars_etl.util.exit_status import EXIT_STATUS

def id_lookup(args):
    """
    """
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    if isinstance(args, dict):  # args can be dict
        args = dict_to_argparse_namespace(args)

    # are we translating id#->short_name or opposite?
    try:
        value = int(args.value)
        column_given = 'id'
        column_to_get= 'short_name'
    except ValueError as v_err:
        value = "'"+args.value+"'"
        column_given = 'short_name'
        column_to_get= 'id'

    connection = metadatabase.get_conn()
    try:
        with connection.cursor() as cursor:
            # Read a single record
            sql = "SELECT {} FROM {} WHERE {}={}".format(
                column_to_get,
                args.table,
                column_given,
                value
            )
            logger.debug("query:" + sql)
            cursor.execute(sql)
            result = cursor.fetchone()
            if (result is None):
                logger.error("No files found matching given metadata")
                exit(EXIT_STATUS.NO_MATCHING_FILES)
            elif (len(result) > 1):
                logger.error("Too many results!")
                logger.error(result)
                exit(EXIT_STATUS.MULTIPLE_MATCH)
            else:
                translation = result[column_to_get]
                print(translation)
                return translation

    finally:
        connection.close()

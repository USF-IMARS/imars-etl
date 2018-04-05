import logging
import sys

from imars_etl import metadatabase
from imars_etl.util import dict_to_argparse_namespace

class EXIT_STATUS(object):
    NO_MATCHING_FILES = 7
    MULTIPLE_MATCH = 6

def get_metadata(args):
    """
    prints json-formatted metadata for first entry in given args.sql

    args can be dict or argparse.Namespace

    Example usage:
        ./imars-etl.py -vvv get_metadata 'area_id=1'

    returns:
    --------
    metadata : dict
        metadata from db
    """
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    if isinstance(args, dict):  # args can be dict
        args = dict_to_argparse_namespace(args)

    connection = metadatabase.get_conn()
    try:
        with connection.cursor() as cursor:
            # Read a single record
            sql = "SELECT * FROM file WHERE {}".format(args.sql)
            cursor.execute(sql)
            result = cursor.fetchone()
            if (result is None):
                logger.error("No files found matching given metadata")
                exit(EXIT_STATUS.NO_MATCHING_FILES)
            elif (len(result) > 1):
                # TODO: request more info from user
                logger.error("Too many results!")
                logger.error(result)
                exit(EXIT_STATUS.MULTIPLE_MATCH)
            else:
                # TODO: download & then print a path to where the file can be
                # accessed on the local machine.
                # NOTE: currently this just gives the path because we assume:
                #   1. that the path is in /srv/imars-objects
                #   2. that the system is set up to access /srv/imars-objects
                print(result)
                return result

    finally:
        connection.close()

import logging

from imars_etl import metadatabase

class EXIT_STATUS(object):
    NO_MATCHING_FILES = 7
    MULTIPLE_MATCH = 6

def extract(args):
    """
    Example usage:
        ./imars-etl.py -vvv extract 'area_id=1'
    """
    logger = logging.getLogger(__name__)
    logger.debug('extract')

    connection = metadatabase.get_conn()
    try:
        with connection.cursor() as cursor:
            # Read a single record
            sql = "SELECT filepath FROM file WHERE {}".format(args.sql)
            logger.debug('query:\n\t'+sql)
            cursor.execute(sql)
            result = cursor.fetchone()
            if (result is None):
                logger.error("No files found matching given metadata")
                exit(EXIT_STATUS.NO_MATCHING_FILES)
            elif (len(result) > 1):
                # TODO: request more info from user
                logger.error("Too many results!")
                print(result)
                exit(EXIT_STATUS.MULTIPLE_MATCH)
            else:
                # print a path to where the file can be accessed on the local
                # machine.
                # NOTE: currently this just gives the path because we assume:
                #   1. that the path is in /srv/imars-objects
                #   2. that the system is set up to access /srv/imars-objects
                print(result)

    finally:
        connection.close()
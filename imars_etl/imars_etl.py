import logging
import json

import pymysql

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

    connection = _get_conn()
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

def load(args):
    """
    Example Usages:
        ./imars-etl.py -vvv load /home/me/myfile.png '{"area_id":1}'

        ./imars-etl.py -vvv load
            -f /home/tylar/usf-imars.github.io/assets/img/bg.png
            -a 1
            -t 1
            -d '2018-02-26T13:00'
            -j '{"status":0}'
    """
    logger = logging.getLogger(__name__)
    logger.debug('load')
    # raise NotImplementedError("loading files not yet implemented")
    # # TODO: check for required args
    # # TODO: prompt for missing args
    connection = _get_conn()
    try:
            with connection.cursor() as cursor:
                try:
                    json_dict = json.loads(args.json)
                except TypeError as t_err:  # json str is empty
                    json_dict = dict()
                json_dict["filepath"] = '"'+args.filepath+'"'
                json_dict["area_id"] = args.area
                json_dict["date_time"] = '"'+args.date+'"'
                json_dict["product_type_id"] = args.type

                str_concat=(lambda x, y: str(x)+","+str(y))
                keys = reduce(
                    str_concat,
                    [str(key) for key in json_dict]
                )
                vals = reduce(
                    str_concat,
                    [str(json_dict[key]) for key in json_dict]
                )
                logger.debug(keys)
                logger.debug(vals)
                # Create a new record
                sql = "INSERT INTO file ("+keys+") VALUES ("+vals+")"
                logger.debug('query:\n\t'+sql)
                result = cursor.execute(sql)

            # connection is not autocommit by default. So you must commit to save
            # your changes.
            connection.commit()
    finally:
       connection.close()

def _get_conn():
    # get connection to the metadata database
    return pymysql.connect(
        host='reef02master',
        user='imars_bot',
        password='***REMOVED***',
        db='imars_product_metadata',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

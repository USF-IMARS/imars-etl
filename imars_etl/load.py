import logging
import json

from imars_etl import metadatabase

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
    connection = metadatabase.get_conn()
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

import logging
import json

from imars_etl import metadatabase
from imars_etl.filepath.parse import parse
from imars_etl.util import dict_to_argparse_namespace

def load(args):
    """
    args can be a dict or argparse.Namespace

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

    if isinstance(args, dict):  # args can be dict
        args = dict_to_argparse_namespace(args)

    args = _validate_args(args)

    connection = metadatabase.get_conn()
    try:
        with connection.cursor() as cursor:
            try:
                json_dict = json.loads(args.json)
            except TypeError as t_err:  # json str is empty
                json_dict = dict()
            json_dict["filepath"] = '"'+args.filepath+'"'
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

            if args.dry_run:  # test mode returns the sql string
                return sql
            else:
                result = cursor.execute(sql)
                # connection is not autocommit by default. So you must commit to save
                # your changes.
                connection.commit()
    finally:
       connection.close()

def _validate_args(args):
    """
    Returns properly formatted & complete argument list.
    Makes attempts to guess at filling in missing args.
    """
    logger = logging.getLogger(__name__)
    # these are soft-required args, ones that we might try to guess if not
    # given, but we have to give up if we cannot figure them out.
    required_args = ['type','date']
    for arg in required_args:
        try:
            if getattr(args, arg) is None:
                guessed_val = _guess_arg_value(args, arg)
                if guessed_val is not None:
                    setattr(args, arg, guessed_val)
            # else keep the given value
        except ValueError as v_err:
            logger.debug("failed to guess value for '{}'".format(arg))
    return args

def _guess_arg_value(args, arg_to_guess):
    """
    Attempts to guess a value for the given arg_to_guess using info from the
    other args (mostly args.filepath).
    Will overwrite args[arg_to_guess] if it finds a more appropriate value.
    """
    # try to guess the arg using filepath
    val, mod_path = parse(arg_to_guess, args.filepath, args.filepath)
    return val

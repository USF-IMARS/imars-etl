from datetime import datetime
import logging
import sys
import json
import os

from imars_etl import metadatabase
from imars_etl.filepath.parse_param import parse_all_from_filename
from imars_etl.filepath.data import get_product_data_from_id, get_product_id, get_product_name
from imars_etl.util import dict_to_argparse_namespace
from imars_etl.drivers.imars_objects.load import _load

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
    if isinstance(args, dict):  # args can be dict
        args = dict_to_argparse_namespace(args)

    if args.filepath is not None:
        return _load_file(args)
    elif args.directory is not None:
        return _load_dir(args)
    else:
        # NOTE: this should be thrown by the arparse arg group before getting
        #   here, but we throw here for the python API.
        raise ValueError("one of --filepath or --directory is required.")

def _load_dir(args):
    """
    loads multiple files that match from a directory

    returns:
        insert_statements : str[]
            list of insert statements for all files found
    """
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    insert_statements = []  #
    # logger.debug("searching w/ '{}'...".format(fmt))
    for root, dirs, files in os.walk(args.directory):
        for filename in files:
            try:
                fpath = os.path.join(root,filename)
                args.filepath = fpath
                insert_statements.append(_load_file(args))
                logger.debug("loading {}...".format(fpath))
            except SyntaxError as s_err:
                logger.debug("skipping {}...".format(fpath))
    return insert_statements

def _load_file(args):
    """loads a single file"""
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    args = _validate_args(args)

    connection = metadatabase.get_conn()
    try:
        with connection.cursor() as cursor:
            sql = _make_sql_insert(args)
            logger.debug('query:\n\t'+sql)
            # load file into IMaRS data warehouse
            # NOTE: _load should support args.dry_run=True also
            new_filepath = _load(vars(args))
            sql = sql.replace(args.filepath, new_filepath)
            if args.dry_run:  # test mode returns the sql string
                return sql
            else:
                result = cursor.execute(sql)
                connection.commit()
                return sql
    finally:
       connection.close()

def _make_sql_insert(args):
    """creates SQL INSERT INTO statement with metadata from given args dict"""
    try:
        json_dict = json.loads(args.json)
    except TypeError as t_err:  # json str is empty
        json_dict = dict()
    json_dict["filepath"] = '"'+args.filepath+'"'
    json_dict["date_time"] = '"'+args.time+'"'
    json_dict["product_type_id"] = args.product_type_id

    str_concat=(lambda x, y: str(x)+","+str(y))
    keys = reduce(
        str_concat,
        [str(key) for key in json_dict]
    )
    vals = reduce(
        str_concat,
        [str(json_dict[key]) for key in json_dict]
    )
    # Create a new record
    return "INSERT INTO file ("+keys+") VALUES ("+vals+")"

def _validate_args(args):
    """
    Returns properly formatted & complete argument list.
    Makes attempts to guess at filling in missing args.
    """
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    logger.setLevel(logging.INFO)

    # === validate product name and id
    if (  # require name or id for directory loading
            getattr(args, 'directory', None) is not None and
            getattr(args, 'product_type_id', None) is None and
            getattr(args, 'product_type_name', None) is None
        ):
        # NOTE: this is probably not a hard requirement
        #   but it seems like a good safety precaution.
        raise ValueError(
            "--product_type_id or --product_type_name must be" +
            " explicitly set if --directory is used."
        )
    elif (  # fill id from name
        getattr(args, 'product_type_id', None) is None and
        getattr(args, 'product_type_name', None) is not None
    ):
        setattr(args, "product_type_id", get_product_id(args.product_type_name))
    elif (  # fill name from id
        getattr(args, 'product_type_id', None) is not None and
        getattr(args, 'product_type_name', None) is None
    ):
        setattr(args, "product_type_name", get_product_name(args.product_type_id))
    else:
        pass
        # TODO: ensure that given id and name match
        # assert(
        #     args.product_type_id == args.get_product_id(args.product_type_name)
        # )

    product_data = get_product_data_from_id(args.product_type_id)


    logger.debug("pre-guess-args : " + str(args))
    args = parse_all_from_filename(args)
    logger.debug("post-guess-args: " + str(args))

    ISO_8601_FMT="%Y-%m-%dT%H:%M:%S"

    try:
        dt = datetime.strptime(args.time, ISO_8601_FMT)
        logger.debug("full datetime parsed")
    except ValueError as v_err:
        dt = datetime.strptime(args.time, ISO_8601_FMT[:-3])
        logger.debug("partial datetime parsed (no seconds)")
    except TypeError as t_err:
        logger.error("{}\n\n".format(t_err))
        raise ValueError(
            "Could not determine datetime for product(s)." +
            " Please input more information by using more arguments" +
            " or try to debug using super-verbose mode -vvv."
        )

    setattr(args, "datetime", dt)

    return args

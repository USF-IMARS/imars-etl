from datetime import datetime
import logging
import sys
import json
import os
import re
from argparse import ArgumentError

from imars_etl import metadatabase
from imars_etl.filepath.parse import parse, parse_all_from_filename
from imars_etl.filepath.data import get_product_data_from_id
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
        raise ArgumentError("one of --filepath or --directory is required.")

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
    if args.product_type_id is None:
        # TODO: this is probably not a hard requirement
        #   but it seems like a good safety precaution.
        raise ArgumentError(
            "--product_type_id must be explicitly set if --directory is used."
        )
    else:
        insert_statements = []  #
        # TODO: mv id to name conversion into _validate_args?
        product_data = get_product_data_from_id(args.product_type_id)
        if args.ingest_key is not None:
            try:  # just the one regex
                regex = product_data["ingest_formats"][args.ingest_key]["find_regex"]
            except KeyError as k_err:
                raise KeyError("no ingest_key '{}' in product {}".format(
                    args.ingest_key,
                    args.product_type_id
                ))
        elif len(product_data["ingest_formats"]) == 1:
            # if there is only 1 ingest_format then we must use that one
            ingest_key, regex = product_data["ingest_formats"].itervalues().next()
        else:
            # we don't know what ingest_format to use
            raise KeyError(
                "--ingest_key must be given for product # {}".format(
                    args.product_type_id
                )
            )

        prod_reg = re.compile(r'{}'.format(regex))
        logger.debug("searching w/ {}...".format(regex))
        for root, dirs, files in os.walk(args.directory):
            for filename in files:
                # logger.debug("{}?".format(filename))
                if prod_reg.match(filename):
                    fpath = os.path.join(root,filename)
                    logger.debug("load {}...".format(fpath))
                    args.filepath = fpath
                    insert_statements.append(_load_file(args))
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
    setattr(args, "datetime", dt)

    return args

def _guess_arg_value(args, arg_to_guess):
    """
    Attempts to guess a value for the given arg_to_guess using info from the
    other args (mostly args.filepath).
    Will overwrite args[arg_to_guess] if it finds a more appropriate value.
    """
    # try to guess the arg using filepath
    v, fp = parse(arg_to_guess, args.filepath, args.filepath)
    return v

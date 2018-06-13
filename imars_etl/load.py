from datetime import datetime
import logging
import sys
import json
import os
import copy
import numbers

from pymysql.err import IntegrityError

from imars_etl.filepath.parse_filepath import parse_filepath
from imars_etl.filepath.get_product_id import get_product_id

from imars_etl.filepath.get_product_name import get_product_name
from imars_etl.util import get_sql_result
from imars_etl.util import dict_to_argparse_namespace
from imars_etl.util.exceptions import InputValidationError
from imars_etl.drivers.imars_objects import load_file

LOAD_DEFAULTS = {
    'storage_driver': "imars_objects",
    'output_path': None
}

# map from input strings to load_file functions for each backend
STORAGE_DRIVERS = {
    'imars_objects': load_file,
    'no_upload': lambda args: args['filepath'],
}


def load(argvs):
    """
    Args can be a dict or argparse.Namespace

    Example Usages:
        ./imars-etl.py -vvv load /home/me/myfile.png '{"area_id":1}'

        ./imars-etl.py -vvv load
            -f /home/tylar/usf-imars.github.io/assets/img/bg.png
            -a 1
            -t 1
            -d '2018-02-26T13:00'
            -j '{"status_id":0}'
    """
    if isinstance(argvs, dict):  # args can be dict
        args_dict = argvs
        args_ns = dict_to_argparse_namespace(argvs)
    else:  # assume we have an argparse namespace
        args_dict = vars(argvs)
        args_ns = argvs

    return _load(args_ns=args_ns, **args_dict)


def _load(args_ns, filepath=None, directory=None, **kwargs):
    if filepath is not None:
        return _load_file(args_ns)
    elif directory is not None:
        return _load_dir(args_ns)
    else:
        # NOTE: this should be thrown by the arparse arg group before getting
        #   here, but we throw here for the python API.
        raise ValueError("one of --filepath or --directory is required.")


def _load_dir(args):
    """
    Loads multiple files that match from a directory

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
    orig_args = copy.deepcopy(args)
    loaded_count = 0
    skipped_count = 0
    duplicate_count = 0
    for root, dirs, files in os.walk(args.directory):
        for filename in files:
            try:
                fpath = os.path.join(root, filename)
                args.filepath = fpath
                insert_statements.append(_load_file(args))
                logger.debug("loading {}...".format(fpath))
                loaded_count += 1
            except SyntaxError as s_err:
                logger.debug("skipping {}...".format(fpath))
                skipped_count += 1
            except IntegrityError as i_err:
                logger.debug(i_err)
                errnum, errmsg = i_err.args
                logger.debug("errnum,={}".format(errnum))
                logger.debug("errmsg,={}".format(errmsg))
                DUPLICATE_ENTRY_ERRNO = 1062
                if (
                    errnum == DUPLICATE_ENTRY_ERRNO and
                    getattr(args, 'duplicates_ok', False)
                ):
                    logger.warn(
                        "IntegrityError: Duplicate entry for '{}'".format(
                            fpath
                        )
                    )
                    duplicate_count += 1
                else:
                    raise
            finally:
                args = copy.deepcopy(orig_args)  # reset args
    logger.info("{} files loaded, {} skipped, {} duplicates.".format(
        loaded_count, skipped_count, duplicate_count
    ))
    return insert_statements


def _load_file(args):
    """Loads a single file"""
    args_dict = _validate_args(args)
    args = dict_to_argparse_namespace(args_dict)  # TODO: rm need for this

    # load file into IMaRS data warehouse
    # NOTE: _load should support args.dry_run=True also
    selected_driver = args_dict.get(
        'storage_driver',
        LOAD_DEFAULTS['storage_driver']
    )
    try:
        new_filepath = STORAGE_DRIVERS[selected_driver](args_dict)
    except TypeError:  # for some reason nosetests needs it like this:
        new_filepath = STORAGE_DRIVERS[selected_driver].load_file(args_dict)

    sql = _make_sql_insert(args_dict)
    sql = sql.replace(args.filepath, new_filepath)
    if getattr(args, 'dry_run', False):  # test mode returns the sql string
        return sql
    else:
        return get_sql_result(
            args,
            sql,
            check_result=False,
            should_commit=(not getattr(args, 'dry_run', False)),
        )


def _make_sql_insert(args):
    """Creates SQL INSERT INTO statement with metadata from given args dict"""
    VALID_FILE_TABLE_COLNAMES = [  # TODO: get this from db
        'filepath', 'date_time', 'product_id', 'is_day_pass',
        'area_id', 'status_id'
    ]
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    KEY_FMT_STR = '{},'  # how we format sql keys
    keys = ""
    vals = ""
    for key in args:
        val = args[key]
        if key in VALID_FILE_TABLE_COLNAMES:
            if isinstance(val, numbers.Number):
                val_fmt_str = '{},'
            else:  # fmt as str
                val_fmt_str = '"{}",'
            keys += KEY_FMT_STR.format(key)
            vals += val_fmt_str.format(val)
    keys = keys[:-1]  # trim last comma
    vals = vals[:-1]

    # Create a new record
    SQL = "INSERT INTO file ("+keys+") VALUES ("+vals+")"
    logger.debug(SQL)
    return SQL


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

    setattr(
        args, 'storage_driver',
        getattr(args, 'storage_driver', 'imars_objects')
    )

    # === validate product name and id
    if (  # require name or id for directory loading
        getattr(args, 'directory', None) is not None and
        getattr(args, 'product_id', None) is None and
        getattr(args, 'product_type_name', None) is None
    ):
        # NOTE: this is probably not a hard requirement
        #   but it seems like a good safety precaution.
        raise ValueError(
            "--product_id or --product_type_name must be" +
            " explicitly set if --directory is used."
        )
    elif (  # fill id from name
        getattr(args, 'product_id', None) is None and
        getattr(args, 'product_type_name', None) is not None
    ):
        setattr(args, "product_id", get_product_id(args.product_type_name))
    elif (  # fill name from id
        getattr(args, 'product_id', None) is not None and
        getattr(args, 'product_type_name', None) is None
    ):
        setattr(args, "product_type_name", get_product_name(args.product_id))
    else:
        pass
        # TODO: ensure that given id and name match
        # assert(
        #     args.product_id == args.get_product_id(args.product_type_name)
        # )
        # assert(
        #   get_product_data_from_id(args.product_id),
        #   ???
        # )

    logger.debug("pre-guess-args : " + str(args))
    args = parse_filepath(args)
    logger.debug("post-guess-args: " + str(args))

    ISO_8601_FMT = "%Y-%m-%dT%H:%M:%S"

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

    # TODO: do all the stuff above to make this dict instead
    arg_dict = vars(args)

    try:  # add json args to arg_dict
        json_dict = json.loads(arg_dict['json'])
        for key in json_dict:
            if key in arg_dict and arg_dict[key] != json_dict[key]:
                raise InputValidationError(
                    "CLI argument passed that contradicts json argument:\n" +
                    "\t CLI arg : {}".format(arg_dict[key]) +
                    "\tjson arg : {}".format(json_dict[key])
                )
            else:
                arg_dict[key] = json_dict[key]
    except TypeError:
        logger.debug("json str is empty")
    except KeyError:
        logger.warn("args['json'] is None?")

    # create args['date_time'] from args['time']
    arg_dict['date_time'] = arg_dict['time']
    return arg_dict

from datetime import datetime
import logging
import sys
import json
import os
import copy

from pymysql.err import IntegrityError

from imars_etl.filepath.parse_param import parse_all_from_filename
from imars_etl.filepath.data import get_product_data_from_id, get_product_id, get_product_name
from imars_etl.util import dict_to_argparse_namespace, get_sql_result
from imars_etl.drivers.imars_objects import load_file

STORAGE_DRIVERS = {  # map from input strings to load_file functions for each backend
    'imars_objects': load_file,
    'no_upload': lambda args: args['filepath'],
}

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
            -j '{"status_id":0}'
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
    orig_args = copy.deepcopy(args)
    loaded_count=0
    skipped_count=0
    duplicate_count=0
    for root, dirs, files in os.walk(args.directory):
        for filename in files:
            try:
                fpath = os.path.join(root,filename)
                args.filepath = fpath
                insert_statements.append(_load_file(args))
                logger.debug("loading {}...".format(fpath))
                loaded_count+=1
            except SyntaxError as s_err:
                logger.debug("skipping {}...".format(fpath))
                skipped_count+=1
            except IntegrityError as i_err:
                logger.debug(i_err)
                errnum, errmsg = i_err.args
                logger.debug("errnum,={}".format(errnum))
                logger.debug("errmsg,={}".format(errmsg))
                DUPLICATE_ENTRY_ERRNO=1062
                if (
                    errnum==DUPLICATE_ENTRY_ERRNO and
                    getattr(args,'duplicates_ok', False)
                ):
                    logger.warn(
                        "IntegrityError: Duplicate entry for '{}'".format(
                            fpath
                        )
                    )
                    duplicate_count+=1
                else:
                    raise
            finally:
                args = copy.deepcopy(orig_args)  # reset args
    logger.info("{} files loaded, {} skipped, {} duplicates.".format(
        loaded_count, skipped_count, duplicate_count
    ))
    return insert_statements

def _load_file(args):
    """loads a single file"""
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    args = _validate_args(args)

    # load file into IMaRS data warehouse
    # NOTE: _load should support args.dry_run=True also
    try:
        new_filepath = STORAGE_DRIVERS[args.storage_driver](vars(args))
    except TypeError:  # for some reason nosetests needs it like this:
        new_filepath = STORAGE_DRIVERS[args.storage_driver].load_file(vars(args))

    sql = _make_sql_insert(args)
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
    """creates SQL INSERT INTO statement with metadata from given args dict"""
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    try:
        json_dict = json.loads(args.json)
    except (TypeError, AttributeError):  # json str is empty
        json_dict = dict()
    json_dict["filepath"] = '"'+args.filepath+'"'
    json_dict["date_time"] = '"'+args.time+'"'
    json_dict["product_id"] = args.product_id

    keys=""
    vals=""
    for key in json_dict:
        keys += str(key)+","
        vals += str(json_dict[key])+","
    keys=keys[:-1] # trim last comma
    vals=vals[:-1]

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

    product_data = get_product_data_from_id(args.product_id)


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

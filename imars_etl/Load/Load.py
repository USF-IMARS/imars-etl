import logging
import sys
import os
import copy
import numbers

from pymysql.err import IntegrityError

from imars_etl.drivers import DRIVER_MAP_DICT
from imars_etl.drivers_metadata import dhus_json
from imars_etl.util import get_sql_result

from imars_etl.Load.unify_metadata import unify_metadata
from imars_etl.Load.hashcheck import hashcheck
from imars_etl.Load.validate_args import validate_args

LOAD_DEFAULTS = {
    'storage_driver': DRIVER_MAP_DICT["imars_objects"],
    'output_path': None,
    'metadata_file': None,
    'metadata_file_driver': dhus_json.Parser,
    'nohash': True,
}


def load(
    filepath=None, directory=None,
    metadata_file_driver=LOAD_DEFAULTS['metadata_file_driver'],
    **kwargs
):
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
    args_dict = dict(
        filepath=filepath,
        directory=directory,
        metadata_file_driver=metadata_file_driver,
        **kwargs
    )
    if filepath is not None:
        return _load_file(args_dict)
    elif directory is not None:
        return _load_dir(args_dict)
    else:
        # NOTE: this should be thrown by the arparse arg group before getting
        #   here, but we throw here for the python API.
        raise ValueError("one of --filepath or --directory is required.")


def _load_dir(args_dict):
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
    orig_args = copy.deepcopy(args_dict)
    loaded_count = 0
    skipped_count = 0
    duplicate_count = 0
    for root, dirs, files in os.walk(args_dict['directory']):
        for filename in files:
            try:
                fpath = os.path.join(root, filename)
                args_dict['filepath'] = fpath
                insert_statements.append(_load_file(args_dict))
                logger.debug("loading {}...".format(fpath))
                loaded_count += 1
            except SyntaxError:
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
                    args_dict.get('duplicates_ok', False)
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
                args_dict = copy.deepcopy(orig_args)  # reset args
    logger.info("{} files loaded, {} skipped, {} duplicates.".format(
        loaded_count, skipped_count, duplicate_count
    ))
    return insert_statements


def _load_file(args_dict):
    """Loads a single file"""
    args_dict = unify_metadata(args_dict)
    args_dict['storage_driver'] = args_dict.get(
        'storage_driver', LOAD_DEFAULTS['storage_driver']
    )
    args_dict = validate_args(args_dict)

    if args_dict.get('nohash', LOAD_DEFAULTS['nohash']) is False:
        hashcheck(**args_dict)

    new_filepath = _actual_load_file_with_driver(**args_dict)

    sql = _make_sql_insert(**args_dict)
    sql = sql.replace(args_dict['filepath'], new_filepath)
    if args_dict.get('dry_run', False):  # test mode returns the sql string
        return sql
    else:
        return get_sql_result(
            sql,
            check_result=False,
            should_commit=(not args_dict.get('dry_run', False)),
            first=args_dict.get("first", False),
        )


def _actual_load_file_with_driver(**kwargs):
    # load file into IMaRS data warehouse
    # NOTE: _load should support args.dry_run=True also
    selected_driver = kwargs.get(
        'storage_driver',
        LOAD_DEFAULTS['storage_driver']
    )
    print("\n\n\t{}\n\n".format(kwargs))
    return selected_driver(**kwargs)


def _make_sql_insert(**kwargs):
    """Creates SQL INSERT INTO statement with metadata from given args dict"""
    VALID_FILE_TABLE_COLNAMES = [  # TODO: get this from db
        'filepath', 'date_time', 'product_id', 'is_day_pass',
        'area_id', 'status_id', 'uuid'
    ]
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    KEY_FMT_STR = '{},'  # how we format sql keys
    keys = ""
    vals = ""
    for key in kwargs:
        val = kwargs[key]
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

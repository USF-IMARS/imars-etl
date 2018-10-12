import logging
import sys
import os
import copy
import numbers

from pymysql.err import IntegrityError

from imars_etl.drivers_metadata.get_metadata_driver_from_key\
    import get_metadata_driver_from_key
from imars_etl.util import get_sql_result
from imars_etl.get_hook import get_hook
from imars_etl.Load.validate_args import validate_args
from imars_etl.object_storage import DEFAULT_OBJ_STORE_CONN_ID
from imars_etl.metadata_db import DEFAULT_METADATA_DB_CONN_ID
from imars_etl.object_storage.hook_wrappers.DataLakeHookWrapper \
    import DataLakeHookWrapper
from imars_etl.object_storage.hook_wrappers.FSHookWrapper \
    import FSHookWrapper
from imars_etl.object_storage.hook_wrappers.BaseHookWrapper \
    import WrapperMismatchException


LOAD_DEFAULTS = {
    'output_path': None,
    'metadata_file': None,
    'metadata_file_driver': get_metadata_driver_from_key('dhus_json'),
    'nohash': False,
    'noparse': False,
    'object_store': DEFAULT_OBJ_STORE_CONN_ID,
    'metadata_db': DEFAULT_METADATA_DB_CONN_ID,
}

VALID_FILE_TABLE_COLNAMES = [  # TODO: get this from db
    'filepath', 'date_time', 'product_id', 'is_day_pass',
    'area_id', 'status_id', 'uuid', 'multihash'
]


def load(
    filepath=None, directory=None,
    metadata_file_driver=LOAD_DEFAULTS['metadata_file_driver'],
    object_store=LOAD_DEFAULTS['object_store'],
    metadata_db=LOAD_DEFAULTS['metadata_db'],
    sql="",
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
        object_store=object_store,
        metadata_db=metadata_db,
        sql=sql,
        **kwargs
    )
    if filepath is not None:
        return _load_file(args_dict)  # TODO: ideally we would splat these.
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
                _handle_integrity_error(i_err, fpath, **args_dict)
                duplicate_count += 1
            finally:
                args_dict = copy.deepcopy(orig_args)  # reset args
    logger.info("{} files loaded, {} skipped, {} duplicates.".format(
        loaded_count, skipped_count, duplicate_count
    ))
    return insert_statements


def _handle_integrity_error(i_err, fpath, duplicates_ok=False, **kwargs):
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    logger.debug(i_err)
    errnum, errmsg = i_err.args
    logger.debug("errnum,={}".format(errnum))
    logger.debug("errmsg,={}".format(errmsg))
    DUPLICATE_ENTRY_ERRNO = 1062
    if (errnum == DUPLICATE_ENTRY_ERRNO and duplicates_ok):
        logger.warn(
            "IntegrityError: Duplicate entry for '{}'".format(
                fpath
            )
        )
    else:
        raise


def _load_file(args_dict):
    """Loads a single file"""
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    logger.setLevel(logging.DEBUG)
    logger.info("\n\n------- loading file {} -------\n".format(
        args_dict.get('filepath', '???').split('/')[-1]
    ))
    args_dict = validate_args(args_dict, DEFAULTS=LOAD_DEFAULTS)

    new_filepath = _actual_load_file_with_driver(**args_dict)

    fields, rows = _make_sql_row_and_key_lists(**args_dict)
    for row_i, row in enumerate(rows):
        for i, element in enumerate(row):
            try:
                rows[row_i][i] = element.replace(
                    args_dict['filepath'], new_filepath
                )
            except (AttributeError, TypeError):  # element not a string
                pass
    if args_dict.get('dry_run', False):  # test mode returns the sql string
        logger.debug('oh, just a test')
        return _make_sql_insert(**args_dict).replace(
            args_dict['filepath'], new_filepath
        )
    else:
        try:
            db_conn_id = args_dict['metadata_db']
            metadata_hook = get_hook(db_conn_id)
            metadata_hook.insert_rows(
                table='file',
                rows=rows,
                target_fields=fields,
                commit_every=1000,
                replace=False,
            )
        except IntegrityError as i_err:
            _handle_integrity_error(i_err, new_filepath, **args_dict)


def _actual_load_file_with_driver(**kwargs):
    # load file into IMaRS data warehouse
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    obj_store_hook = get_hook(kwargs['object_store'])
    result = None

    try:  # direct usage (no wrapper)
        result = obj_store_hook.load(**kwargs)
    except AttributeError:
        logger.debug('raw hook failed')

    try:  # azure_data_lake-like interface:
        result = DataLakeHookWrapper(obj_store_hook).load(**kwargs)
    except WrapperMismatchException:
        logger.debug('hook not DataLake-like')

    try:
        result = FSHookWrapper(obj_store_hook).load(**kwargs)
    except WrapperMismatchException:
        logger.debug('hook not FSHook-like')

    if result is None:
        raise AttributeError(
            "hook '{}' has unknown interface.".format(obj_store_hook)
        )
    else:
        return result


def _make_sql_row_and_key_lists(**kwargs):
    """
    Creates SQL key & value lists with metadata from given args dict
    """
    keys = []
    vals = []
    for key in kwargs:
        val = kwargs[key]
        if key in VALID_FILE_TABLE_COLNAMES:
            keys.append(key)
            vals.append(val)
    return keys, [vals]


def _make_sql_row_and_key_strings(**kwargs):
    """
    !!! DEPRECATED !!!
    Creates SQL key & value strings with metadata from given args dict
    """
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
    return keys, vals


def _make_sql_insert(**kwargs):
    """
    !!! DEPRECATED !!!
    Creates SQL INSERT INTO statement with metadata from given args dict
    """
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    logger.setLevel(logging.WARN)
    keys, vals = _make_sql_row_and_key_strings(**kwargs)
    # Create a new record
    SQL = "INSERT INTO file ("+keys+") VALUES ("+vals+")"
    logger.debug(SQL)
    return SQL

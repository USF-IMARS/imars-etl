import logging
import sys
import os
import copy
import numbers

from pymysql.err import IntegrityError

from imars_etl.object_storage.get_storage_driver_from_key\
    import get_storage_driver_from_key
from imars_etl.drivers_metadata.get_metadata_driver_from_key\
    import get_metadata_driver_from_key
from imars_etl.util import get_sql_result
from imars_etl.get_hook import get_hook
from imars_etl.Load.validate_args import validate_args
from imars_etl.filepath.format_filepath import format_filepath

LOAD_DEFAULTS = {
    'storage_driver': get_storage_driver_from_key('imars_objects'),
    'output_path': None,
    'metadata_file': None,
    'metadata_file_driver': get_metadata_driver_from_key('dhus_json'),
    'nohash': False,
    'noparse': False,
}


def load(
    filepath=None, directory=None,
    metadata_file_driver=LOAD_DEFAULTS['metadata_file_driver'],
    object_store_conn_id="imars_object_store",  # TODO: add this to the CLI
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
    logger.info("\n\n------- loading file {} -------\n".format(
        args_dict.get('filepath', '???').split('/')[-1]
    ))
    args_dict = validate_args(args_dict, DEFAULTS=LOAD_DEFAULTS)

    new_filepath = _actual_load_file_with_driver(**args_dict)

    sql = _make_sql_insert(**args_dict)
    sql = sql.replace(args_dict['filepath'], new_filepath)
    if args_dict.get('dry_run', False):  # test mode returns the sql string
        return sql
    else:
        try:
            return get_sql_result(
                sql,
                check_result=False,
                should_commit=(not args_dict.get('dry_run', False)),
                first=args_dict.get("first", False),
            )
        except IntegrityError as i_err:
            _handle_integrity_error(i_err, new_filepath, **args_dict)


def _actual_load_file_with_driver(
        obj_store_conn_id='imars_object_store', **kwargs
):
    # load file into IMaRS data warehouse
    obj_store_hook = get_hook(obj_store_conn_id)
    # assume azure_data_lake-like interface:
    local_src_path = kwargs['filepath']
    remote_target_path = format_filepath(**kwargs)
    # print("\n\n\t{}\n\n".format(kwargs))
    if kwargs.get('dry_run', False) is False:
        obj_store_hook.upload_file(
            local_path=local_src_path,
            remote_path=remote_target_path
        )
    return remote_target_path


def _make_sql_insert(**kwargs):
    """Creates SQL INSERT INTO statement with metadata from given args dict"""
    VALID_FILE_TABLE_COLNAMES = [  # TODO: get this from db
        'filepath', 'date_time', 'product_id', 'is_day_pass',
        'area_id', 'status_id', 'uuid', 'multihash'
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

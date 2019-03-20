import logging
import numbers

from imars_etl.drivers_metadata.get_metadata_driver_from_key\
    import get_metadata_driver_from_key
from imars_etl.Load.validate_args import validate_args
from imars_etl.Load.validate_args import _get_handles
from imars_etl.object_storage.ObjectStorageHandler \
    import DEFAULT_OBJ_STORE_CONN_ID
from imars_etl.metadata_db.MetadataDBHandler import DEFAULT_METADATA_DB_CONN_ID

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
    filepath=None,
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
            -a 1
            -t 1
            -t '2018-02-26T13:00'
            -j '{"status_id":0}'
            /home/tylar/usf-imars.github.io/assets/img/bg.png
    """
    args_dict = dict(
        filepath=filepath,
        metadata_file_driver=metadata_file_driver,
        object_store=object_store,
        metadata_db=metadata_db,
        sql=sql,
        **kwargs
    )
    if filepath is not None:
        return _load_file(args_dict)  # TODO: ideally we would splat these.
    else:
        # NOTE: this should be thrown by the arparse arg group before getting
        #   here, but we throw here for the python API.
        raise ValueError("filepath is required.")


def _load_file(args_dict):
    """Loads a single file"""
    logger = logging.getLogger("imars_etl.{}".format(
        __name__,
        )
    )
    logger.info("------- loading file {} ----------------\n".format(
        args_dict.get('filepath', '???').split('/')[-1]
    ))
    object_storage, metadata_db = _get_handles(**args_dict)
    args_dict = validate_args(args_dict, DEFAULTS=LOAD_DEFAULTS)

    new_filepath = object_storage.load(**args_dict)

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
        metadata_db.insert_rows(
            table='file',
            rows=rows,
            target_fields=fields,
            commit_every=1000,
            replace=False,
        )


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
    logger = logging.getLogger("imars_etl.{}".format(
        __name__,
        )
    )
    keys, vals = _make_sql_row_and_key_strings(**kwargs)
    # Create a new record
    SQL = "INSERT INTO file ("+keys+") VALUES ("+vals+")"
    logger.debug(SQL)
    return SQL

import logging
import numbers

from imars_etl.drivers_metadata.get_metadata_driver_from_key\
    import get_metadata_driver_from_key
from imars_etl.Load.validate_args import validate_args
from imars_etl.object_storage.ObjectStorageHandler \
    import DEFAULT_OBJ_STORE_CONN_ID
from imars_etl.metadata_db.MetadataDBHandler import DEFAULT_METADATA_DB_CONN_ID
from imars_etl.config_logger import config_logger

LOAD_DEFAULTS = {  # defaults here instead of fn def for cli argparse usage
    'metadata_file_driver': get_metadata_driver_from_key('dhus_json'),
    'nohash': False,
    'noparse': False,
    'object_store': DEFAULT_OBJ_STORE_CONN_ID,
    'metadata_db': DEFAULT_METADATA_DB_CONN_ID,
    'sql': '',
    'dry_run': False,
}

VALID_FILE_TABLE_COLNAMES = [  # TODO: get this from db
    'filepath', 'date_time', 'product_id', 'is_day_pass',
    'area_id', 'status_id', 'uuid', 'multihash', 'provenance'
]


def load(*, verbose=0, **kwargs):
    config_logger(verbose)
    return _load(
        **validate_args(kwargs, DEFAULTS=LOAD_DEFAULTS)
    )


def _load(
    filepath, *args,
    object_storage_handle, metadata_db_handle,
    dry_run, **kwargs
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
    assert len(args) == 0
    args_dict = dict(
        filepath=filepath,
        object_storage_handle=object_storage_handle,
        metadata_db_handle=metadata_db_handle,
        dry_run=dry_run,
        **kwargs
    )

    logger = logging.getLogger("imars_etl.{}".format(
        __name__,
        )
    )
    logger.info("------- loading file {} ----------------\n".format(
        filepath.split('/')[-1]
    ))

    new_filepath = object_storage_handle.load(**args_dict)

    fields, rows = _make_sql_row_and_key_lists(**args_dict)
    for row_i, row in enumerate(rows):
        for i, element in enumerate(row):
            try:
                rows[row_i][i] = element.replace(
                    filepath, new_filepath
                )
            except (AttributeError, TypeError):  # element not a string
                pass
    if dry_run:  # test mode returns the sql string
        logger.debug('oh, just a test')
        return _make_sql_insert(**args_dict).replace(
            filepath, new_filepath
        )
    else:
        metadata_db_handle.insert_rows(
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

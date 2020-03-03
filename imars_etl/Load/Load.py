import logging
import numbers

from imars_etl.drivers_metadata.get_metadata_driver_from_key\
    import get_metadata_driver_from_key
from imars_etl.Load.validate_args import validate_args
from imars_etl.util.config_logger import config_logger
from imars_etl.metadata_db.mysql import insert
from imars_etl.object_storage.imars_objects import imars_objects

LOAD_DEFAULTS = {  # defaults here instead of fn def for cli argparse usage
    'metadata_file_driver': get_metadata_driver_from_key('dhus_json'),
    'nohash': False,
    'noparse': False,
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
    dry_run, no_load=False, **kwargs
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

    args_dict.pop('ingest_key', None)  # rm this key used by validate_args only
    if no_load or dry_run:
        new_filepath = _dry_run_load_object(args_dict)
    else:
        new_filepath = _load_object(args_dict)

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
        return _load_metadata(args_dict, rows, fields)


def _dry_run_load_object(args_dict):
    wrapper = imars_objects()
    return wrapper.format_filepath(**args_dict)


def _load_object(args_dict):
    wrapper = imars_objects()
    return wrapper.load(**args_dict)


def _load_metadata(args_dict, rows, fields):
    insert(
        _make_sql_insert(**args_dict)
    )
    return _make_sql_where_clause(**args_dict)


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
    Creates SQL key & value strings with metadata from given args dict.

    returns
    -------
    keys : list of str
        eg: ['date_time', 'id', 'product_id']
    vals : list of str
        eg: ['2018-01-22T15:45:00', '1234', '10']
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


def _make_sql_where_clause(**kwargs):
    """
    Creates SQL WHERE ____ statement with metadata from given args dict
    """
    logger = logging.getLogger("imars_etl.{}".format(
        __name__,
        )
    )
    result = []
    for key in kwargs:
        val = kwargs[key]
        if key in VALID_FILE_TABLE_COLNAMES and key != "filepath":
            result.append('{}="{}"'.format(key, val))
    result = ' AND '.join(result)
    logger.debug(result)
    return result

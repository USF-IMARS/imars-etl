import json
import logging
import sys

from imars_etl.util.consts import ISO_8601_FMT
from imars_etl.exceptions.InputValidationError import InputValidationError


def unify_metadata(args_dict):
    """
    Combines metadata from file & json dicts into args_dict.
    Raises error if data is mismatched.
    """
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    # === file metadata
    if args_dict.get('metadata_file') is not None:
        file_metadata = _read_metadata_file(
            args_dict['metadata_file_driver'],
            args_dict['metadata_file']
        )
    else:
        file_metadata = {}

    # === json metadata
    try:  # add json args to args_dict
        json_metadata = json.loads(args_dict['json'])
        _dict_union_raise_on_conflict(args_dict, json_metadata)
    except TypeError:
        logger.debug("json str is empty")
        json_metadata = {}
    except KeyError:
        logger.info("args_dict['json'] is None")
        json_metadata = {}

    # === sql metadata
    try:  # add sql args to args_dict
        sql_metadata = sql_str_to_dict(args_dict['sql'])
        _dict_union_raise_on_conflict(args_dict, sql_metadata)
    except TypeError:
        logger.debug("sql str is empty")
        sql_metadata = {}
    except KeyError:
        logger.info("args_dict['sql'] is None")
        sql_metadata = {}

    args_dict = _union_dicts_raise_on_conflict(
        args_dict,
        file_metadata,
        json_metadata,
        sql_metadata
    )
    print('input metadata summary:\n{}\n'.format(args_dict))
    return args_dict


def sql_str_to_dict(sql_str):
    """
    Transforms sql selector string into key-val dict.
    Expects sql string in the a form like `a=1,col="val"`, which will transform
    into `{'a': 1, 'col': 'val'}`
    """
    result = {}
    if sql_str is None or len(sql_str) < 1:
        return result
    else:
        pairs = sql_str.split(",")
        for pair in pairs:
            key, val = pair.split('=')
            result[key] = val
        return result


def _read_metadata_file(driver, filepath):
    """reads metadata using driver on given filepath"""
    parser = driver(filepath)
    metad = {
        'uuid': parser.get_uuid(),
        'datetime': parser.get_datetime(),
    }

    if metad.get('datetime') is not None:
        metad['time'] = metad['datetime'].strftime(ISO_8601_FMT)

    return metad


def _union_dicts_raise_on_conflict(*args):
    """Union of all given dicts but raises err if any mismatched keyvals"""
    # check each dict against every dict that follows it
    # | AxB AxC AxD | BxC BxD | CxD |
    result_dict = {}
    for dict_i in args:
        result_dict = _dict_union_raise_on_conflict(result_dict, dict_i)
    return result_dict


def _dict_union_raise_on_conflict(dict_a, dict_b):
    """Union of a & b, but raises error if two keys w/ different vals"""
    for key in dict_b:
        if key in dict_a and dict_a[key] != dict_b[key]:
            raise InputValidationError(
                "Conflicting file metadata for key '{}':".format(key) +
                "\n\tval a : {}".format(dict_a[key]) +
                "\n\tval b : {}".format(dict_b[key])
            )
        else:
            dict_a[key] = dict_b[key]
    return dict_a

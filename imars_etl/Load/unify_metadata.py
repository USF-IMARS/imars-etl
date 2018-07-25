import json
import logging
import sys

from imars_etl.util.consts import ISO_8601_FMT
from imars_etl.exceptions.InputValidationError import InputValidationError


def unify_metadata(**kwargs):
    """
    Combines metadata from file & json dicts into kwargs.
    Raises error if data is mismatched.
    """
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    # === file metadata
    if kwargs.get('metadata_file') is not None:
        file_metadata = _read_metadata_file(
            kwargs['metadata_file_driver'],
            kwargs['metadata_file']
        )
    else:
        file_metadata = {}

    # === json metadata
    try:  # add json args to kwargs
        json_metadata = json.loads(kwargs['json'])
        _dict_union_raise_on_conflict(kwargs, json_metadata)
    except TypeError:
        logger.debug("json str is empty")
        json_metadata = {}
    except KeyError:
        logger.info("kwargs['json'] is None")
        json_metadata = {}

    # === sql metadata
    try:  # add sql args to kwargs
        sql_metadata = sql_str_to_dict(kwargs['sql'])
        _dict_union_raise_on_conflict(kwargs, sql_metadata)
    except TypeError:
        logger.debug("sql str is empty")
        sql_metadata = {}
    except KeyError:
        logger.info("kwargs['sql'] is None")
        sql_metadata = {}

    kwargs = _union_dicts_raise_on_conflict(
        kwargs,
        file_metadata,
        json_metadata,
        sql_metadata
    )
    print('input metadata summary:\n{}\n'.format(kwargs))
    return kwargs


def sql_str_to_dict(sql_str):
    """
    Transforms sql selector string into key-val dict.
    Expects sql string in the a form like `a=1 AND col="val"`, which transforms
    into `{'a': 1, 'col': 'val'}`.

    NOTE: 'AND' *is* case-sensitive here.
    """
    result = {}
    if sql_str is None or len(sql_str) < 1:
        return result
    else:
        pairs = sql_str.split(" AND ")
        for pair in pairs:
            key, val = pair.split('=')
            try:
                val = int(val)
            except ValueError:
                pass  # failure to convert to int just means it isn't an int.
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
    """
    Union of a & b, but raises error if two keys w/ different vals.
    NOTE: value comparison is done after converting values to strings.
        This is _probably_ ok for simple types (int & str), but could cause
        weird things if you use this to union dicts with fancy val types.
    """
    for key in dict_b:
        if key in dict_a and str(dict_a[key]) != str(dict_b[key]):
            val_a = dict_a[key]
            val_b = dict_b[key]
            raise InputValidationError(
                "Conflicting file metadata for key '{}':".format(key) +
                "\n\tval a : {} ({})".format(val_a, type(val_a)) +
                "\n\tval b : {} ({})".format(val_b, type(val_b))
            )
        else:
            dict_a[key] = dict_b[key]
    return dict_a

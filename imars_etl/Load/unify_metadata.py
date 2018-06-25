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
        logger.warn("args_dict['json'] is None?")
        json_metadata = {}

    # check for conflicts first
    _dict_union_raise_on_conflict(json_metadata, file_metadata)
    # then start saving result
    args_dict = _dict_union_raise_on_conflict(args_dict, json_metadata)
    args_dict = _dict_union_raise_on_conflict(args_dict, file_metadata)

    print('input metadata summary:\n{}\n'.format(args_dict))
    return args_dict


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

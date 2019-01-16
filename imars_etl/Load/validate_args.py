import os
import logging

from imars_etl.Load.get_hash import get_hash
from imars_etl.Load.hashcheck import hashcheck
from imars_etl.Load.metadata_constraints import ensure_constistent_metadata
from imars_etl.Load.metadata_constraints import _ensure_constistent_metadata
from imars_etl.Load.unify_metadata import unify_metadata
from imars_etl.Load.unify_metadata import _rm_dict_none_values
from imars_etl.util.timestrings import standardize_time_str


def validate_args(args_dict, DEFAULTS={}):
    """
    Returns properly formatted & complete arguments.
    Makes attempts to guess at filling in missing args.
    """
    logger = logging.getLogger("imars_etl.{}".format(
        __name__,
        )
    )
    keys_with_defaults = [
        'object_store',
        'metadata_conn_id',
        'nohash'
    ]
    for key in keys_with_defaults:
        if args_dict.get(key) is None:
            args_dict[key] = DEFAULTS.get(key)

    # remove keys with None values?
    args_dict = _rm_dict_none_values(args_dict)

    if args_dict.get('nohash', False) is False:
        logger.debug('ensuring hash in metadata...')
        args_dict = _ensure_constistent_metadata(
            args_dict,
            [
                ('multihash', ['filepath'], get_hash),
            ],
            raise_cannot_constrain=True
        )
        if not args_dict['duplicates_ok']:
            hashcheck(**args_dict)
    else:
        logger.debug('skipping file hashing.')

    if args_dict.get('time') is not None:
        args_dict['time'] = standardize_time_str(args_dict['time'])

    args_dict = ensure_constistent_metadata(
        args_dict,
        raise_cannot_constrain=False
    )

    # + basename & filename args:
    if args_dict.get('filepath') is not None:
        fpath = args_dict['filepath']
        dirname, fname = os.path.split(fpath)
        assert fname == fpath.split('/')[-1]
        fbase, ext = os.path.splitext(fname)
        assert fbase == fname.split('.')[0]
        args_dict.setdefault('filename', fname)
        args_dict.setdefault('basename', fbase)
        args_dict.setdefault('directory', dirname)
        args_dict.setdefault('ext', ext)
    # apply templating to some args:
    if args_dict.get('metadata_file') is not None:
        args_dict['metadata_file'] = \
            args_dict['metadata_file'].format(**args_dict)

    args_dict = unify_metadata(**args_dict)

    return args_dict

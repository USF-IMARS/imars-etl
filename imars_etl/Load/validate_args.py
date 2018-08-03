import sys
import logging

from imars_etl.Load.get_hash import get_hash
from imars_etl.Load.hashcheck import hashcheck
from imars_etl.Load.metadata_constraints import ensure_constistent_metadata
from imars_etl.Load.unify_metadata import unify_metadata
from imars_etl.Load.unify_metadata import _rm_dict_none_values


def validate_args(args_dict, DEFAULTS={}):
    """
    Returns properly formatted & complete arguments.
    Makes attempts to guess at filling in missing args.
    """
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    logger.setLevel(logging.INFO)

    keys_with_defaults = [
        'storage_driver',
        'nohash'
    ]
    for key in keys_with_defaults:
        if args_dict.get(key) is None:
            args_dict[key] = DEFAULTS.get(key)

    # remove keys with None values?
    args_dict = _rm_dict_none_values(args_dict)

    if args_dict.get('nohash', False) is False:
        logger.debug('ensuring hash in metadata...')
        args_dict = ensure_constistent_metadata(
            args_dict,
            [
                ('multihash', ['filepath'], get_hash),
            ],
            raise_cannot_constrain=True
        )
        hashcheck(**args_dict)
    else:
        logger.debug('skipping file hashing.')

    # === validate product name and id
    if args_dict.get('directory') is not None:
        # NOTE: this is probably not a hard requirement
        #   but it seems like a good safety precaution.
        if(
            args_dict.get('product_id') is None and
            args_dict.get('product_type_name') is None
        ):
            raise ValueError(
                "--product_id or --product_type_name must be" +
                " explicitly set if --directory is used."
            )

    args_dict = ensure_constistent_metadata(
        args_dict,
        raise_cannot_constrain=False
    )

    args_dict = unify_metadata(**args_dict)

    return args_dict

import sys
import logging
from datetime import datetime

from imars_etl.filepath.parse_filepath import parse_filepath
from imars_etl.filepath.parse_filepath import _set_unless_exists
from imars_etl.filepath.get_product_id import get_product_id
from imars_etl.filepath.get_product_name import get_product_name

from imars_etl.util.consts import ISO_8601_FMT


def validate_args(args_dict):
    """
    Returns properly formatted & complete arguments.
    Makes attempts to guess at filling in missing args.
    """
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    logger.setLevel(logging.INFO)
    # === validate product name and id
    if (  # require name or id for directory loading
        args_dict.get('directory') is not None and
        args_dict.get('product_id') is None and
        args_dict.get('product_type_name') is None
    ):
        # NOTE: this is probably not a hard requirement
        #   but it seems like a good safety precaution.
        raise ValueError(
            "--product_id or --product_type_name must be" +
            " explicitly set if --directory is used."
        )
    elif (  # fill id from name
        args_dict.get('product_id') is None and
        args_dict.get('product_type_name') is not None
    ):
        args_dict['product_id'] = get_product_id(
            args_dict['product_type_name']
        )
    elif (  # fill name from id
        args_dict.get('product_id') is not None and
        args_dict.get('product_type_name') is None
    ):
        args_dict['product_type_name'] = get_product_name(
            args_dict['product_id']
        )
    else:
        pass
        # TODO: ensure that given id and name match
        # assert(
        #     args_ns.product_id ==
        #     args_ns.get_product_id(args_ns.product_type_name)
        # )
        # assert(
        #   get_product_data_from_id(args_ns.product_id),
        #   ???
        # )

    logger.debug("pre-guess-args : " + str(args_dict))

    args_parsed = parse_filepath(**args_dict)
    for key in args_parsed.keys():
        _set_unless_exists(args_dict, key, args_parsed[key])

    logger.debug("post-guess-args: " + str(args_dict))

    try:
        dt = datetime.strptime(args_dict['time'], ISO_8601_FMT)
        logger.debug("full datetime parsed")
    except ValueError:
        dt = datetime.strptime(args_dict['time'], ISO_8601_FMT[:-3])
        logger.debug("partial datetime parsed (no seconds)")
    except TypeError as t_err:
        logger.error("{}\n\n".format(t_err))
        raise ValueError(
            "Could not determine datetime for product(s)." +
            " Please input more information by using more arguments" +
            " or try to debug using super-verbose mode -vvv."
        )
    args_dict['datetime'] = dt

    # create args['date_time'] from args['time']
    args_dict['date_time'] = args_dict['time']
    return args_dict

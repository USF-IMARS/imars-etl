"""
create a filepath from metadata
"""
import logging
import sys

from imars_etl.filepath.data import data


def get_imars_object_paths():
    """
    Returns a dict of all imars_object paths keyed by product name.

    example:
    {
        "test_test_test":{
            "//": "this is a fake type used for testing only",
            "basename": "simple_file_with_no_args.txt",
            "path"    : "/srv/imars-objects/test_test_test",
            "product_id": -1
        },
        "zip_wv2_ftp_ingest":{
            "basename": "wv2_%Y_%m_{tag}.zip",
            "path"    : "/srv/imars-objects/{product_type_name}",
            "product_id": 6
        },
        "att_wv2_m1bs":{
            "basename": "WV02_%Y%m%d%H%M%S-M1BS-{idNumber}_P{passNumber}.att",
            "path": "/srv/imars-objects/extra_data/WV02/%Y.%m",
            "product_id": 7
        }
    }
    """
    res = {}
    for product_id in data:
        res[product_id] = data[product_id]["imars_object_format"]
    return res


def format_filepath(
    hook=None,
    **kwargs
):
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    try:
        logger.debug("hook is {}".format(hook))
        return hook.format_filepath(**kwargs)
    except AttributeError:
        logger.debug('hook has no format_filepath method')
        product_type_name = kwargs.get("product_type_name")
        product_id = kwargs.get("product_id")
        forced_basename = kwargs.get("forced_basename")

        fullpath = _format_filepath_template(
            product_type_name=product_type_name,
            product_id=product_id,
            forced_basename=forced_basename
        )
        logger.info("formatting imars-obj path \n>>'{}'".format(fullpath))
        args_dict = dict(
            **kwargs
        )
        try:
            date_time = kwargs.get("date_time")
            return date_time.strftime(
                (fullpath).format(**args_dict)
            )
        except KeyError as k_err:
            logger.error(
                "cannot guess an argument required to make path. "
                " pass this argument manually using --json "
            )
            raise k_err


def _format_filepath_template(
    product_type_name=None,
    product_id=None,
    forced_basename=None
):
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    logger.setLevel(logging.INFO)
    logger.info("placing {} (#{})...".format(
        product_type_name,
        product_id)
    )
    for prod_name, prod_meta in get_imars_object_paths().items():
        logger.debug("is {} (#{})?".format(prod_name, prod_meta['product_id']))
        if (
                product_type_name == prod_name or
                product_id == prod_meta['product_id']
        ):  # if file type or id is given and matches a known type
            logger.debug('y!')

            if forced_basename is not None:
                _basename = forced_basename
            else:
                _basename = prod_meta['basename']

            try:  # set product_type_name if not already set
                # test = args['product_type_name']
                # TODO: check for match w/ prod_name & raise if not?
                pass
            except KeyError:
                product_type_name = prod_name

            return prod_meta['path']+"/"+_basename
        else:
            logger.debug("no.")
    else:
        # logger.debug(args)
        raise ValueError("could not identify product type")

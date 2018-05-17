"""
create a filepath from metadata
"""
import logging
import sys

from imars_etl.filepath.data import data

def get_imars_object_paths():
    """
    returns a dict of all imars_object paths keyed by product name.

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
            "basename": "WV02_%Y%m%d%H%M%S_0000000000000000_%y%b%d%H%M%S-M1BS-{idNumber}_P{passNumber}.att",  # NOTE: how to %b in all caps?
            "path": "/srv/imars-objects/extra_data/WV02/%Y.%m",
            "product_id": 7
        }
    }
    """
    res = {}
    for product_id in data:
        res[product_id] = data[product_id]["imars_object_format"]
    return res

def format_filepath(args, forced_basename=None):
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    fullpath = _format_filepath_template(
        product_type_name=args.get('product_type_name', None),
        product_id=args.get('product_id', None),
        forced_basename=forced_basename
    )
    logger.info("formatting imars-obj path \n>>'{}'".format(fullpath))
    return args['datetime'].strftime(
        (fullpath).format(**args)
    )

def _format_filepath_template(
        product_type_name,
        product_id,
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
                product_type_name == prod_name
                or product_id == prod_meta['product_id']
            ):  # if file type or id is given and matches a known type
            logger.debug('y!')

            if forced_basename is not None:
                _basename = forced_basename
            else:
                _basename = prod_meta['basename']

            try:  # set product_type_name if not already set
                test = product_type_name  # NOTE: this made more sense before:
                # test = args['product_type_name']
                # TODO: check for match w/ prod_name & raise if not?
            except KeyError as k_err:
                product_type_name = prod_name

            return prod_meta['path']+"/"+_basename
        else:
            logger.debug("no.")
    else:
        # logger.debug(args)
        raise ValueError("could not identify product type")

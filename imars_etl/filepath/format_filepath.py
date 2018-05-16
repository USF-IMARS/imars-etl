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
    """
    args are used to set metadata info that may be used in the formation of
    the path or basename.

    Returns
    ------------
    str
        path to file formed using given metadata in args
    """
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    logger.setLevel(logging.INFO)
    logger.info("placing {} (#{})...".format(
        args.get('product_type_name','???'),
        args.get('product_id',-999999))
    )
    for prod_name, prod_meta in get_imars_object_paths().items():
        logger.debug("is {} (#{})?".format(prod_name, prod_meta['product_id']))
        if (
                   args.get('product_type_name','') == prod_name
                or args.get('product_id',-999999) == prod_meta['product_id']
            ):  # if file type or id is given and matches a known type
            logger.debug('y!')

            if forced_basename is not None:
                _basename = forced_basename
            else:
                _basename = prod_meta['basename']

            try:  # set product_type_name if not already set
                test = args['product_type_name']
                # TODO: check for match w/ prod_name & raise if not?
            except KeyError as k_err:
                args['product_type_name'] = prod_name

            fullpath = prod_meta['path']+"/"+_basename
            logger.info("formatting imars-obj path \n>>'{}'".format(fullpath))
            return args['datetime'].strftime(
                (fullpath).format(**args)
            )
        else:
            logger.debug("no.")
    else:
        # logger.debug(args)
        raise ValueError("could not identify product type")

import logging

from imars_etl.filepath.formatter_hardcoded.data import data


def _get_imars_object_paths():
    """
    Returns a dict of all imars_object paths keyed by product name.

    example:
    {
        "test_test_test":{
            "//": "this is a fake type used for testing only",
            "basename": "simple_file_with_no_args.txt",
            "path"    : "test_test_test",
            "product_id": -1
        },
        "zip_wv2_ftp_ingest":{
            "basename": "wv2_%Y_%m_{tag}.zip",
            "path"    : "{product_type_name}",
            "product_id": 6
        },
        "att_wv2_m1bs":{
            "basename": "WV2_%Y%m%d%H%M%S-M1-{idNumber}_P{passNumber}.att",
            "path": "extra_data/WV02/%Y.%m",
            "product_id": 7
        }
    }
    """
    res = {}
    for product_id in data:
        res[product_id] = data[product_id]["imars_object_format"]
    return res


def get_product_filepath_template(
    product_type_name=None,
    product_id=None,
    forced_basename=None
):
    """returns filepath template string for given product type & id"""
    logger = logging.getLogger("imars_etl.{}".format(
        __name__,
        )
    )
    logger.info("placing {} (#{})...".format(
        product_type_name,
        product_id)
    )
    for prod_name, prod_meta in (
        _get_imars_object_paths().items()
    ):
        logger.debug(
            "is {} (#{})?".format(prod_name, prod_meta['product_id'])
        )
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

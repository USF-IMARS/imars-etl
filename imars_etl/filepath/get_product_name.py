import logging
import sys

from imars_etl.filepath.data import data


def get_product_name(product_id):
    """Get product name from given id"""
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    logger.setLevel(logging.INFO)
    logger.debug("get_data_from_pid({})".format(product_id))
    for product_short_name, product_data in data.items():
        pid = product_data["imars_object_format"]["product_id"]
        logger.debug("pid is {}?".format(pid))
        if pid == product_id:
            logger.debug("y!")
            return product_short_name
    else:
        raise KeyError("product_id {} not found".format(product_id))

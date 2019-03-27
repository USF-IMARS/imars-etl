import os
import copy
import logging
import sys

from imars_etl.Load.validate_args import validate_args
from imars_etl.config_logger import config_logger


def find(
    directory,
    verbose=0,
    **kwargs
):
    """
    Lists all files that match from a directory

    returns:
    -------
    filepath_list : str[]
        list of all matching file paths found

    Example usage:
        imars_etl.find(  TODO  )
    """
    config_logger(verbose)
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    # logger.debug("searching w/ '{}'...".format(fmt))
    kwargs["nohash"] = True  # TODO rm?
    # rm None values from kwargs (for set comparison later)
    kwargs = {k: v for k, v in kwargs.items() if v is not None}
    filepath_list = []
    matches = 0
    unmatches = 0
    for root, dirs, files in os.walk(directory):
        for filename in files:
            kwargs_copy = copy.deepcopy(kwargs)
            try:
                fpath = os.path.join(root, filename)
                kwargs_copy['filepath'] = fpath
                kwargs_copy = validate_args(kwargs_copy)

                logger.debug("="*40)
                logger.debug(fpath)
                logger.debug("="*40)
                # === throw exception if does not match
                # filepath required
                assert kwargs_copy['filepath'] is not None
                # inferred metadata (now in kwargs_copy) must at least contain
                # requirements passed to find (kwargs).
                logger.debug(" === "*3)
                logger.debug(kwargs)
                logger.debug(" === "*3)
                logger.debug(kwargs_copy)
                assert(set(kwargs.items()).issubset(set(kwargs_copy.items())))

                assert kwargs_copy['product_id'] is not None
                assert kwargs_copy['area_id'] is not None
                assert kwargs_copy['time'] is not None

                print(fpath)
                filepath_list.append(fpath)
                matches += 1
            except(SyntaxError, KeyError, AssertionError):
                # SyntaxError from parser,
                # KeyError from missing kwargs,
                # AssertionError from requirements above
                logger.debug("skipping {}...".format(fpath))
                unmatches += 1
    logger.info("{} matching files found out of {} searched.".format(
        matches, unmatches + matches
    ))
    return filepath_list

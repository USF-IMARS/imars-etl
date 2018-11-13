import os
import copy
import logging
import sys

from imars_etl.Load.validate_args import validate_args


def find(
    directory,
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
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    # logger.debug("searching w/ '{}'...".format(fmt))
    filepath_list = []
    matches = 0
    unmatches = 0
    for root, dirs, files in os.walk(directory):
        for filename in files:
            kwargs_copy = copy.deepcopy(kwargs)
            try:
                fpath = os.path.join(root, filename)
                kwargs_copy['filepath'] = fpath
                validate_args(kwargs_copy)

                # TODO: throw exception if does not match

                print(fpath)
                filepath_list.append(fpath)
                matches += 1
            except SyntaxError:
                logger.debug("skipping {}...".format(fpath))
                unmatches += 1
    logger.info("{} matching files found out of {} searched.".format(
        matches, unmatches + matches
    ))
    return filepath_list

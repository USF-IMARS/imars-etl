import os
import logging
import sys


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
    filepath_list = []
    # logger.debug("searching w/ '{}'...".format(fmt))
    loaded_count = 0
    skipped_count = 0
    for root, dirs, files in os.walk(directory):
        for filename in files:
            try:
                fpath = os.path.join(root, filename)
                # kwargs_copy['filepath'] = fpath
                print(fpath)
                filepath_list.append(fpath)
                loaded_count += 1
            except SyntaxError:
                logger.debug("skipping {}...".format(fpath))
                skipped_count += 1
    logger.info("{} files loaded, {} skipped.".format(
        loaded_count, skipped_count
    ))
    return filepath_list

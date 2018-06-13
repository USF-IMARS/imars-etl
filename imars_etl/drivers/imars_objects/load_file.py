import logging
import sys
import os
import shutil
import errno

from imars_etl.filepath.format_filepath import format_filepath


def load_file(args):
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    # logger.debug('_load(args)| args=\n\t{}'.format(args))
    ul_target = format_filepath(args)
    logger.debug(["cp", args['filepath'], ul_target])

    if not args.get('dry_run', False):  # don't actually load if test mode
        try:
            shutil.copy(args['filepath'], ul_target)
        except IOError as i_err:  # possible dir DNE
            # ENOENT(2): file does not exist or missing dest parent dir
            if i_err.errno != errno.ENOENT:
                raise i_err
            else:
                os.makedirs(os.path.dirname(ul_target))
                shutil.copy(args['filepath'], ul_target)

    return ul_target

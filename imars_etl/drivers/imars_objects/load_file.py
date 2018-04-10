import logging
import sys
import os
import shutil
import errno

from imars_etl.drivers.imars_objects.satfilename import satfilename

def load_file(args):
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    # logger.debug('_load(args)| args=\n\t{}'.format(args))
    ul_target = satfilename.get_name(args)
    logger.debug(["mv", args['filepath'], ul_target])

    if not args['dry_run']:  # don't actually move if test mode
        try:
            shutil.move(args['filepath'], ul_target)
        except IOError as i_err:  # possible dir DNE
            # ENOENT(2): file does not exist or missing dest parent dir
            if i_err.errno != errno.ENOENT:
                raise i_err
            else:
                os.makedirs(os.path.dirname(ul_target))
                shutil.move(args['filepath'], ul_target)

    return ul_target

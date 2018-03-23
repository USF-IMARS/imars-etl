import logging
import sys
import shutil

from imars_etl.drivers.imars_objects.satfilename import satfilename

def _load(args):
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    ul_target = satfilename.get_name(args)
    logger.debug(["mv", args['filepath'], ul_target])

    if not args['dry_run']:  # don't actuall move if test mode
        shutil.move(args['filepath'], ul_target)

    return ul_target

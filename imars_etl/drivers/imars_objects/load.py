import logging
import sys
import os

from imars_etl.drivers.imars_objects.satfilename import satfilename

def _load(args):
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    ul_target = satfilename.get_name(args)
    outp = ["mv", args['filepath'], ul_target]
    logging.debug(outp)
    if dry_run:
        # test mode
        return outp
    else:
        # === peform the load
        # move the file into imars-objects path @ ul_target
        os.move(args['filepath'], ul_target)
        return ul_target

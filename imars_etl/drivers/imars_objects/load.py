import logging
import os

from imars_etl.drivers.imars_objects.satfilename import satfilename

def _load(dry_run=False, **kwargs):
    logger = logging.getLogger(__name__)
    ul_target = satfilename.get_name(**kwargs)
    outp = ["mv", kwargs['filepath'], ul_target]
    logging.debug(outp)
    if dry_run:
        # test mode
        return outp
    else:
        # === peform the load
        # move the file into imars-objects path @ ul_target
        os.move(kwargs['filepath'], ul_target)
        return ul_target

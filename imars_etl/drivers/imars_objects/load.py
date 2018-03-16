import os

from imars_etl.drivers.imars_objects.satfilename import satfilename

def _load(dry_run=False, **kwargs):
    ul_target = satfilename.get_name(**kwargs)
    if dry_run:
        # test mode
        outp = ["mv", kwargs['filepath'], ul_target]
        print(outp)
        return outp
    else:
        # === peform the load
        # move the file into imars-objects path @ ul_target
        return os.move(kwargs['filepath'], ul_target)

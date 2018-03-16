from imars_etl.drivers.imars_objects.satfilename import satfilename

def _load(**kwargs):
    ul_target = satfilename.get_name(**kwargs)
    if kwargs.get('dry_run', False):
        # test only; do nothing
        outp = ["mv", kwargs['filepath'], ul_target]
        print(outp)
        return outp
    else:
        # peform the load
        # TODO
        return

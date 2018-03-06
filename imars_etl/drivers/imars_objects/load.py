from imars_etl.drivers.imars_objects.satfilename import satfilename

def _load(args):
    print("mv {} /srv/imars-objects/{}/{}".format(
        args.filepath,
        args.type,
        args.date
    ))
    print( satfilename(args.type, args.date) )

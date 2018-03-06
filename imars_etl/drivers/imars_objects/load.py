def _load(args):
    print("mv {} /srv/imars-objects/{}/{}".format(
        args.filepath,
        args.type,
        args.date
    ))
    print( satfilename(args.type, args.date) )

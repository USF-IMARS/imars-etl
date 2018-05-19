from imars_etl.filepath.data import data

def get_ingest_formats():
    """
    returns a dict of all ingest formats.

    example:
    {
        "zip_wv2_ftp_ingest.matts_wv2_ftp_ingest": "wv2_%Y_%m_{tag}.zip",
        "att_wv2_m1bs.att_from_zip_wv2_ftp_ingest": "%y%b%d%H%M%S-M1BS-{idNumber}_P{passNumber}.ATT",
    }
    """
    res = {}
    for product_id in data:
        for ingest_id in data[product_id]["ingest_formats"]:
            res[
                "{}.{}".format(product_id, ingest_id)
            ] = get_ingest_format(product_id, ingest_id)
    return res

def get_ingest_format(short_name, ingest_name=None):
    """
    returns
    -------
    ingest_fmt_str : str
        ingest path format string for given product short_name and ingest_name.
    ingest_name : str
        name of the ingest_key used. Only useful if ingest_name=None,
        else it just passes through unaltered.
    """
    if ingest_name is not None:
        try:  # use the one given fmt string
            return data[short_name]["ingest_formats"][ingest_name]["path_format"]
        except KeyError as k_err:
            raise KeyError("no ingest_key '{}' in product {}".format(
                ingest_name,
                short_name
            ))
    elif len(data[short_name]["ingest_formats"]) == 1:
        # if there is only 1 ingest_format then we must use that one
        ingest_key = list(data[short_name]["ingest_formats"].keys())[0]
        return data[short_name]["ingest_formats"][ingest_key]['path_format']
    else:
        # we don't know what ingest_format to use
        raise KeyError(
            "--ingest_key must be given for product '{}'".format(
                short_name
            )
        )

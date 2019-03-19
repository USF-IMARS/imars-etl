from imars_etl.filepath.formatter_hardcoded.get_ingest_format import \
    get_ingest_format as hardcoded_get_ingest_format
from imars_etl.filepath.formatter_hardcoded.get_ingest_format import \
    get_ingest_formats as hardcoded_get_ingest_formats


def get_ingest_formats():
    """
    Returns a dict of all ingest formats.

    example:
    {
        "zip_wv2_ftp_ingest.matts_wv2_ftp_ingest": "wv2_%Y_%m_{tag}.zip",
        "att_.from_zip_ingest": "%y%b%d%H%M%S-M1BS-{idNum}_P{passNumber}.ATT",
    }
    """
    res = hardcoded_get_ingest_formats()
    return res


def get_ingest_format(short_name, ingest_name=None):
    """
    Returns
    -------
    ingest_fmt_str : str
        ingest path format string for given product short_name and ingest_name.
    ingest_name : str
        name of the ingest_key used. Only useful if ingest_name=None,
        else it just passes through unaltered.
    """
    return hardcoded_get_ingest_format(short_name, ingest_name)

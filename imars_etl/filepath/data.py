"""
dict which maps `imars_product_metadata.product.short_name` to expected
   filename patterns. Used to infer metadata from the filepath.
"""
ISO_8601_FMT="%Y-%m-%dT%H:%M:%SZ"
# dict containing valid values for given variables that can be used in the
# filename patterns below.
# NOTE: this could be removed if #3 is implemented
valid_pattern_vars = {
    "area": [
    # TODO: fill this from the database automatically
    ],
    "tag":       ["_",  "*", ".zip"],  # * matches any str between edge strings (not inclusive)
    "idNumber":  ["BS-",  "*", "_P"],
    "passNumber":["_P", "*", ".ATT"]
}

# NOTE: can FIND_REGEX be removed by allowing imars-etl to search the dir
#       w/ INGESTABLE_FORMAT instead?
data = {
    "att_wv2_m1bs": {  # == short_name from imars_product_metadata db
        "ingest_formats": {
            "att_from_zip_wv2_ftp_ingest":{
                # "example": "16FEB12162518-M1BS-057522945010_01_P002.ATT",
                "find_regex": ".*/[0-3][0-9][A-Z]\{3\}[0-9]\{8\}-M1BS-[0-9_]*_P[0-9]\{3\}.ATT",
                "path_format": "%y%b%d%H%M%S-M1BS-{idNumber}_P{passNumber}.ATT"
                # TODO: update this to work w/ #3 :
                # "path_format": "%y%b%d%H%M%S-M1BS-{idNumber:12d}_{whatThis:2d}_P{passNumber:3d}.ATT"
            },
            # {...more could go here...}
        },
        "imars_object_format": {
            "path": "/srv/imars-objects/extra_data/WV02/%Y.%m",
            "basename": "WV02_%Y%m%d%H%M%S_0000000000000000_%y%b%d%H%M%S-M1BS-{idNumber}_P{passNumber}.att",
            # TODO: update this to work w/ #3 :
            # "basename": "WV02_%Y%m%d%H%M%S_0000000000000000_%y%b%d%H%M%S-M1BS-{idNumber:12d}_{whatThis:2d}_P{passNumber:3d}.att",
            "product_type_id": 7  # TODO: use metadata db for product_type_id
        },
    },
    "zip_wv2_ftp_ingest": {
        "ingest_formats": {
            "matts_wv2_ftp_ingest":{
                "find_regex": "/srv/imars-objects/ftp-ingest/wv2_*zip",
                "path_format": "wv2_%Y_%m_{tag}.zip",
            }
        },
        "imars_object_format":{
            "path": "/srv/imars-objects/zip_wv2_ftp_ingest",
            "basename": "wv2_%Y_%m_{tag}.zip",
            "product_type_id": 6
        },
    },
    "test_test_test":{
        "//": "this is a fake type used for testing only",
        "ingest_formats": {},
        "imars_object_format": {
            "path"    : "/srv/imars-objects/test_test_test",
            "basename": "simple_file_with_no_args.txt",
            "product_type_id": -1
        },
    },
    ### === others from the metadata db that in need of adding:
    # "png_chl_7d": {
    #     "name": "FGB_A1km_chlor_a_%Y%j_%Y%j_7D_MEAN.png"
    # },
    ### === legacy values from pre-metadata db times:
    # "l1a_lac_hdf_bz2":{
    #     "//": "zipped l1a (myd01) files from OB.DAAC",
    #     "basename": "A%Y%j%H%M00.L1A_LAC.x.hdf.bz2"
    # }
    # "myd01": {
    #     "//": "modis aqua l1. I *think* these files are the same as l1a_LAC," +
    #         + " but from LANCE.",
    #     "basename": "A%Y%j.%H%M.hdf"
    # }
    # "geo": {
    #     "basename": "A%Y%j%H%M00.GEO"
    # }
    # "l1b": {
    #     "basename": "A%Y%j%H%M00.L1B_LAC"
    # }
    # "hkm": {
    #     "basename": "A%Y%j%H%M00.L1B_HKM"
    # }
    # "qkm": {
    #     "basename": "A%Y%j%H%M00.L1B_QKM"
    # }
    # "l2": {
    #     "basename": "A%Y%j%H%M00.L2"
    # }
    # "l3": {
    #     "basename": ISO_8601_FMT+"_l3.nc"
    # }
    # "l3_pass": {
    #     "basename": ISO_8601_FMT+"_l3.nc"
    # }
    # "metadata-ini": {
    #     "basename": "metadata_"+ISO_8601_FMT+".ini"
    # }
    # "png": {
    #     "path": "/srv/imars-objects/modis_aqua_{region_shortname}/png_{variable_name}"
    #     "basename": ISO_8601_FMT + "_{variable_name}.png"
    # }
}

def get_imars_object_paths():
    """
    returns a dict of all imars_object paths keyed by product name.

    example:
    {
        "test_test_test":{
            "//": "this is a fake type used for testing only",
            "basename": "simple_file_with_no_args.txt",
            "path"    : "/srv/imars-objects/test_test_test",
            "product_type_id": -1
        },
        "zip_wv2_ftp_ingest":{
            "basename": "wv2_%Y_%m_{tag}.zip",
            "path"    : "/srv/imars-objects/{product_type_name}",
            "product_type_id": 6
        },
        "att_wv2_m1bs":{
            "basename": "WV02_%Y%m%d%H%M%S_0000000000000000_%y%b%d%H%M%S-M1BS-{idNumber}_P{passNumber}.att",  # NOTE: how to %b in all caps?
            "path": "/srv/imars-objects/extra_data/WV02/%Y.%m",
            "product_type_id": 7
        }
    }
    """
    res = {}
    for product_id in data:
        res[product_id] = data[product_id]["imars_object_format"]
    return res


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

def get_ingest_format(short_name, ingest_name):
    """
    returns ingest path format string for given product short_name and
    ingest_name.
    """
    return data[short_name]["ingest_formats"][ingest_name]["path_format"]

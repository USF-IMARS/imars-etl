"""
dict which maps `imars_product_metadata.product.short_name` to expected
   filename patterns. Used to infer metadata from the filepath.
"""
import logging
import sys

ISO_8601_FMT="%Y-%m-%dT%H:%M:%SZ"

# some const strings to make reading the dict keys below easier
INGEST_FMTS="ingest_formats"
I_OBJ_FMT="imars_object_format"
REGX="find_regex"
F_FMT="path_format"
PATH="path"
BASE="basename"
PID="product_type_id"

def wv2_ingest_prod(m1bs_or_p1bs, ext, pid, up_ext=True,
    ingest_id="from_zip_wv2_ftp_ingest"
):
    """
    shorthand to build a dict for wv2 ingest products because they all have many
    things in common.

    Parameters
    ----------
    m1bs_or_p1bs : char
        m || p to indicate m1bs or p1bs
    ext : str
        lowercase file extension (with leading '.')
    pid : int
        product_type_id
    up_ext : boolean
        if True we expect file extensions to be all upper-case on ingest.
        this is the case for all files except those in /GIS_FILES
    """
    WV2_OUT_PATH="/srv/imars-objects/extra_data/WV02/%Y.%m"

    m1bs_or_p1bs = m1bs_or_p1bs.upper()
    assert(m1bs_or_p1bs=='M' or m1bs_or_p1bs=='P')

    regx_rt = "[0-3][0-9][A-Z]{3}[0-9]{8}-" + m1bs_or_p1bs + "1BS-[0-9_]*_P[0-9]{3}"
    fmt_rt  = "%y%b%d%H%M%S-" + m1bs_or_p1bs + "1BS-{idNumber}_P{passNumber}"
    base_rt = "WV02_%Y%m%d%H%M%S_0000000000000000_%y%b%d%H%M%S-" + m1bs_or_p1bs + "1BS-{idNumber}_P{passNumber}"

    EXT = ext.upper()

    return {  # == short_name from imars_product_metadata db
        INGEST_FMTS: {
            ingest_id:{
                REGX:  regx_rt + EXT,
                F_FMT: fmt_rt  + EXT
                # TODO: update this to work w/ #3 :
                # "path_format": "%y%b%d%H%M%S-M1BS-{idNumber:12d}_{whatThis:2d}_P{passNumber:3d}.ATT"
            }
        },
        I_OBJ_FMT: {
            PATH: WV2_OUT_PATH,
            BASE: base_rt + ext,
            # TODO: update this to work w/ #3 :
            # "basename": "WV02_%Y%m%d%H%M%S_0000000000000000_%y%b%d%H%M%S-M1BS-{idNumber:12d}_{whatThis:2d}_P{passNumber:3d}.att",
            PID: pid  # TODO: use metadata db for product_type_id
        },
    }

# NOTE: can FIND_REGEX be removed by allowing imars-etl to search the dir
#       w/ INGESTABLE_FORMAT instead?
data = {
    "att_wv2_m1bs": wv2_ingest_prod(
        'M', '.att', 7, ingest_id="att_from_zip_wv2_ftp_ingest"
    ),
    "att_wv2_p1bs":  wv2_ingest_prod(
        'P', '.att', 8, ingest_id="att_from_zip_wv2_ftp_ingest"
    ),
    "eph_wv2_m1bs": wv2_ingest_prod('M', '.eph', 9),
    "geo_wv2_m1bs": wv2_ingest_prod('M', '.geo', 10),
    "imd_wv2_m1bs": wv2_ingest_prod('M', '.imd', 11),
    "ntf_wv2_m1bs": wv2_ingest_prod('M', '.ntf', 12),
    "rpb_wv2_m1bs": wv2_ingest_prod('M', '.rpb', 13),
    "til_wv2_m1bs": wv2_ingest_prod('M', '.til', 14),
    "xml_wv2_m1bs": wv2_ingest_prod('M', '.xml', 15),
    "jpg_wv2_m1bs": wv2_ingest_prod('M', '-BROWSE.jpg', 16),
    "txt_wv2_m1bs": wv2_ingest_prod('M', '_README.txt', 17),
    "shx_wv2_m1bs": wv2_ingest_prod('M', '_PIXEL_SHAPE.shx', 18, up_ext=False),
    "shp_wv2_m1bs": wv2_ingest_prod('M', '_PIXEL_SHAPE.shp', 19, up_ext=False),
    "prj_wv2_m1bs": wv2_ingest_prod('M', '_PIXEL_SHAPE.prj', 20, up_ext=False),
    "dbf_wv2_m1bs": wv2_ingest_prod('M', '_PIXEL_SHAPE.dbf', 21, up_ext=False),
    "eph_wv2_p1bs": wv2_ingest_prod('P', '.eph', 22),
    "geo_wv2_p1bs": wv2_ingest_prod('P', '.geo', 23),
    "imd_wv2_p1bs": wv2_ingest_prod('P', '.imd', 24),
    "ntf_wv2_p1bs": wv2_ingest_prod('P', '.ntf', 25),
    "rpb_wv2_p1bs": wv2_ingest_prod('P', '.rpb', 26),
    "til_wv2_p1bs": wv2_ingest_prod('P', '.til', 27),
    "xml_wv2_p1bs": wv2_ingest_prod('P', '.xml', 28),
    "jpg_wv2_p1bs": wv2_ingest_prod('P', '-BROWSE.jpg', 29),
    "txt_wv2_p1bs": wv2_ingest_prod('P', '_README.txt', 30),
    "shx_wv2_p1bs": wv2_ingest_prod('P', '_PIXEL_SHAPE.shx', 31, up_ext=False),
    "shp_wv2_p1bs": wv2_ingest_prod('P', '_PIXEL_SHAPE.shp', 32, up_ext=False),
    "dbf_wv2_p1bs": wv2_ingest_prod('P', '_PIXEL_SHAPE.dbf', 33, up_ext=False),
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
        "ingest_formats": {
            "file_w_date":{
                "//": "used to test auto date parsing from filename",
                "find_regex": "file_w_date_[0-9]{4}.txt",
                "path_format": "file_w_date_%Y.txt",
            }
        },
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

def get_product_name(product_type_id):
    """get product name from given id"""
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    logger.setLevel(logging.INFO)
    logger.debug("get_data_from_pid({})".format(product_type_id))
    for product_short_name, product_data in data.items():
        pid = product_data["imars_object_format"]["product_type_id"]
        logger.debug("pid is {}?".format(pid))
        if pid == product_type_id:
            logger.debug("y!")
            return product_short_name
    else:
        raise KeyError("product_type_id {} not found".format(product_type_id))

def get_product_id(product_type_name):
    """get product id from given name"""
    return data[product_type_name]["imars_object_format"]["product_type_id"]

def get_product_data_from_id(prod_id):
    """used to index product data by id number instead of short_name"""
    return data[get_product_name(prod_id)]

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

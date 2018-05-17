"""
dict which maps `imars_product_metadata.product.short_name` to expected
   filename patterns. Used to infer metadata from the filepath.
"""
import logging

ISO_8601_FMT="%Y-%m-%dT%H:%M:%SZ"

# some const strings to make reading the dict keys below easier
INGEST_FMTS="ingest_formats"
I_OBJ_FMT="imars_object_format"
F_FMT="path_format"
PATH="path"
BASE="basename"
PID="product_id"

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
        product_id
    up_ext : boolean
        if True we expect file extensions to be all upper-case on ingest.
        this is the case for all files except those in /GIS_FILES
    """
    WV2_OUT_PATH="/srv/imars-objects/extra_data/WV02/%Y.%m"

    m1bs_or_p1bs = m1bs_or_p1bs.upper()
    assert(m1bs_or_p1bs=='M' or m1bs_or_p1bs=='P')

    fmt_rt  = "%y%b%d%H%M%S-" + m1bs_or_p1bs + "1BS-{idNumber}_P{passNumber}"
    base_rt = "WV02_%Y%m%d%H%M%S_0000000000000000_%y%b%d%H%M%S-" + m1bs_or_p1bs + "1BS-{idNumber}_P{passNumber}"

    EXT = ext.upper()

    return {  # == short_name from imars_product_metadata db
        INGEST_FMTS: {
            ingest_id:{
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
            PID: pid  # TODO: use metadata db for product_id
        },
    }

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
                "path_format": "wv2_%Y-%m-%dT%H%M_{tag}.zip",
            }
        },
        "imars_object_format":{
            "path": "/srv/imars-objects/zip_wv2_ftp_ingest",
            "basename": "wv2_%Y-%m-%dT%H%M_{tag}.zip",
            "product_id": 6
        },
    },
    "test_test_test":{
        "//": "this is a fake type used for testing only",
        "ingest_formats": {
            "file_w_date":{
                "//": "used to test auto date parsing from filename",
                "path_format": "file_w_date_%Y.txt",
            }
        },
        "imars_object_format": {
            "path"    : "/srv/imars-objects/test_test_test",
            "basename": "simple_file_with_no_args.txt",
            "product_id": -1
        },
    },
    "test_fancy_format_test":{
        "//": "this is a fake type used for testing only",
        "ingest_formats": {
            "file_w_date":{
                "//": "used to parsing from filename",
                "path_format": "date_%Y%j.arg_{test_arg}.time_%H%S.woah",
            },
            "file_w_nothing":{
                "path_format": "fake_filepath.bs",
            }
        },
        "imars_object_format": {
            "path"    : "/srv/imars-objects/_fancy_{test_arg}_/%Y-%j",
            "basename": "arg_is_{test_arg}_time_is_%H%S.fancy_file",
            "product_id": -2
        },
    },

    # short_names come from MODAPS:
    #    https://modaps.modaps.eosdis.nasa.gov/services/about/products/c6/
    "myd01": {
        "//": "modis aqua l1. I *think* these files are the same as l1a_LAC,"
              " but from LANCE. " +
              "https://modaps.modaps.eosdis.nasa.gov/services/about/products/c6/MYD01.html",
        "imars_object_format":{
            "path": "/srv/imars-objects/modis_aqua_gom/myd01",
            "basename": "A%Y%j.%H%M.hdf",
            "product_id": 5
        },
        "ingest_formats": {
            "modis_std_ish":{
                "//": "sorta like https://lpdaac.usgs.gov/dataset_discovery/modis",
                "path_format": "A%Y%j.%H%M.hdf",
            }
        }
    },
    "zip_ntf_wv2_ftp_ingest": {
        "//": "older format from digital globe with mostly ntf files at root",
        "imars_object_format":{
            "path": "/srv/imars-objects/NONE",  # <- TODO
            "basename": "wv2_ntf_%Y-%m-%dT%H%M_{tag}.zip",
            "product_id": 34
        },
        "ingest_formats": {
            "imars_object_basename_fmt":{
                "path_format": "wv2_ntf_%Y-%m-%dT%H%M_{tag}.zip",
            }
        }
    },
    "myd0_otis_l2": {
        "//": "modis aqua l2 files using @dotis's gpt graph'",
        "imars_object_format":{
            "path": "/srv/imars-objects/{area_short_name}/myd0_otis_l2",
            "basename": "myd0_otis_l2_A%Y%j%H%M%S.hdf",
            "product_id": 35
        },
        "ingest_formats": {
            "dotis_cron_output":{
                "path_format": "A%Y%j%H%M%S.L2",
            }
        }
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

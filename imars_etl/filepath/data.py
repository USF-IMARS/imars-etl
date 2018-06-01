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
    # base_rt = "WV02_%Y%m%d%H%M%S_{???:16c}_%y%b%d%H%M%S-" + m1bs_or_p1bs + "1BS-{idNumber:d}_{???:2d}_P{passNumber}"

    EXT = ext.upper()

    return {  # == short_name from imars_product_metadata db
        INGEST_FMTS: {
            ingest_id:{
                F_FMT: fmt_rt  + EXT
                # "path_format": "%y%b%d%H%M%S-M1BS-{idNumber:12d}_{whatThis:2d}_P{passNumber:3d}.ATT"
            },
            # "old_imars_obj":{
            #     # F_FMT: "WV02_%Y%m%d%H%M%S_{unknownChars}_%y%b%d%H%M%S-" + m1bs_or_p1bs + "1BS-{idNumber:12d}_{otherNum:2d}_P{passNumber:3d}"+ext
            #     # NOTE: {junk} below is not junk (see above) it just needs to
            #     #       be ignored b/c https://bugs.python.org/issue4430
            #     F_FMT: "WV02_%Y%m%d%H%M%S_{junk}-" + m1bs_or_p1bs + "1BS-{idNumber:12}_{otherNum:2}_P{passNumber:0>3d}"+ext
            # }
        },
        I_OBJ_FMT: {
            PATH: WV2_OUT_PATH,
            BASE: base_rt + ext,
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
    "eph_wv2_m1bs": wv2_ingest_prod('M', '.eph', 33),
    "geo_wv2_m1bs": wv2_ingest_prod('M', '.geo', 9),
    "imd_wv2_m1bs": wv2_ingest_prod('M', '.imd', 10),
    "ntf_wv2_m1bs": wv2_ingest_prod('M', '.ntf', 11),
    "rpb_wv2_m1bs": wv2_ingest_prod('M', '.rpb', 12),
    "til_wv2_m1bs": wv2_ingest_prod('M', '.til', 13),
    "xml_wv2_m1bs": wv2_ingest_prod('M', '.xml', 14),
    "jpg_wv2_m1bs": wv2_ingest_prod('M', '-BROWSE.jpg', 15),
    "txt_wv2_m1bs": wv2_ingest_prod('M', '_README.txt', 16),
    "shx_wv2_m1bs": wv2_ingest_prod('M', '_PIXEL_SHAPE.shx', 17, up_ext=False),
    "shp_wv2_m1bs": wv2_ingest_prod('M', '_PIXEL_SHAPE.shp', 18, up_ext=False),
    "prj_wv2_m1bs": wv2_ingest_prod('M', '_PIXEL_SHAPE.prj', 19, up_ext=False),
    "dbf_wv2_m1bs": wv2_ingest_prod('M', '_PIXEL_SHAPE.dbf', 20, up_ext=False),
    "eph_wv2_p1bs": wv2_ingest_prod('P', '.eph', 21),
    "geo_wv2_p1bs": wv2_ingest_prod('P', '.geo', 22),
    "imd_wv2_p1bs": wv2_ingest_prod('P', '.imd', 23),
    "ntf_wv2_p1bs": wv2_ingest_prod('P', '.ntf', 24),
    "rpb_wv2_p1bs": wv2_ingest_prod('P', '.rpb', 25),
    "til_wv2_p1bs": wv2_ingest_prod('P', '.til', 26),
    "xml_wv2_p1bs": wv2_ingest_prod('P', '.xml', 27),
    "jpg_wv2_p1bs": wv2_ingest_prod('P', '-BROWSE.jpg', 28),
    "txt_wv2_p1bs": wv2_ingest_prod('P', '_README.txt', 29),
    "shx_wv2_p1bs": wv2_ingest_prod('P', '_PIXEL_SHAPE.shx', 30, up_ext=False),
    "shp_wv2_p1bs": wv2_ingest_prod('P', '_PIXEL_SHAPE.shp', 31, up_ext=False),
    "dbf_wv2_p1bs": wv2_ingest_prod('P', '_PIXEL_SHAPE.dbf', 32, up_ext=False),
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
                "//": "tests parsing from filename w/ args & date in it",
                "path_format": "date_%Y%j.arg_{test_arg}.time_%H%S.woah",
            },
            "file_w_nothing":{
                "path_format": "fake_filepath.bs",
            },
        },
        "imars_object_format": {
            "path"    : "/srv/imars-objects/_fancy_{test_arg}_/%Y-%j",
            "basename": "arg_is_{test_arg}_time_is_%H%S.fancy_file",
            "product_id": -2
        },
    },
    "test_number_format_test":{
        "//": "this is a fake type used for testing only",
        "ingest_formats": {
            "file_w_formatted_nums":{
                "//": "file format with numbers & format-specifiers",
                "path_format": "date_%Y.num_{test_num:3d}.time_%H.dude",
            },
        },
        "imars_object_format": {
            "path"    : "/srv/imars-objects/_fancy_{test_num:0>3d}_/%Y",
            "basename": "num_is_{test_num:0>4d}_time_is_%H.fancy_file",
            "product_id": -3
        },
    },

    # short_names come from MODAPS:
    #    https://modaps.modaps.eosdis.nasa.gov/services/about/products/c6/
    "myd01": {
        "//": "modis aqua l1. I *think* these files are the same as l1a_LAC,"
              " but from LANCE. " +
              "https://modaps.modaps.eosdis.nasa.gov/services/about/products/c6/MYD01.html",
        "imars_object_format":{
            "path": "/srv/imars-objects/{area_short_name}/myd01",
            "basename": "A%Y%j.%H%M.hdf",
            "product_id": 5
        },
        "ingest_formats": {
            "modis_std_ish":{
                "//": "sorta like https://lpdaac.usgs.gov/dataset_discovery/modis",
                "path_format": "A%Y%j.%H%M.hdf",
            },
           "old_imars_obj":{
               "//": "imars-obj path pre 2018-05",
              "path_format": "/srv/imars-objects/modis_aqua_{area_short_name}/myd01/A%Y%j.%H%M.hdf",
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

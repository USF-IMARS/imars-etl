"""
dict which maps `imars_product_metadata.product.short_name` to expected
   filename patterns. Used to infer metadata from the filepath.
"""

from imars_etl.util.timestrings import ISO_8601_FMT

# some const strings to make reading the dict keys below easier
INGEST_FMTS = "ingest_formats"
I_OBJ_FMT = "imars_object_format"
F_FMT = "path_format"
PATH = "path"
BASE = "basename"
PID = "product_id"


def wv2_ingest_prod(
    m1bs_or_p1bs, ext, pid, up_ext=True,
    ingest_id="from_zip_wv2_ftp_ingest"
):
    """
    Shorthand to build a dict for wv2 ingest products because they all have
    many things in common.

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
    WV2_OUT_PATH = "extra_data/WV02/%Y.%m"

    m1bs_or_p1bs = m1bs_or_p1bs.upper()
    assert(m1bs_or_p1bs == 'M' or m1bs_or_p1bs == 'P')

    fmt_rt = "%y%b%d%H%M%S-" + m1bs_or_p1bs + "1BS-{idNumber}_P{passNumber}"
    base_rt = (
        "WV02_%Y%m%d%H%M%S_0000000000000000_%y%b%d%H%M%S-" +
        m1bs_or_p1bs + "1BS-{idNumber}_P{passNumber}"
    )
    # base_rt = "WV02_%Y%m%d%H%M%S_{???:16c}_%y%b%d%H%M%S-" +
    # m1bs_or_p1bs + "1BS-{idNumber:d}_{???:2d}_P{passNumber}"

    EXT = ext.upper()

    return {  # == short_name from imars_product_metadata db
        INGEST_FMTS: {
            ingest_id: {
                F_FMT: fmt_rt + EXT
                # "path_format":
                # "%y%b%d%H%M%S-M1BS-{idNumber:12d}_{whatThis:2d}_" +
                # "P{passNumber:3d}.ATT"
            },
            # "old_imars_obj":{
            #     # F_FMT: "WV02_%Y%m%d%H%M%S_{unknownChars}_%y%b%d%H%M%S-" +
            #     m1bs_or_p1bs + "1BS-{idNumber:12d}_{otherNum:2d}_" +
            #     P{passNumber:3d}"+ext
            #     # NOTE: {junk} below is not junk (see above) it just needs to
            #     #       be ignored b/c https://bugs.python.org/issue4430
            #     F_FMT: "WV02_%Y%m%d%H%M%S_{junk}-" + m1bs_or_p1bs +
            #     "1BS-{idNumber:12}_{otherNum:2}_P{passNumber:0>3d}"+ext
            # }
        },
        I_OBJ_FMT: {
            PATH: WV2_OUT_PATH,
            BASE: base_rt + ext,
            # "basename": "WV02_%Y%m%d%H%M%S_0000000000000000_%y%b%d%H%M%S" +
            #          "-M1BS-{idNumber:12d}_{whatThis:2d}_P{passNumber:3d}.att",
            PID: pid  # TODO: use metadata db for product_id
        },
    }

data = {
    # NOTE: yes, these *could* be lumped together, but sometimes a processing
    #       method doesn't require the full file bundle, so we save some
    #       network cost by keeping them separate.
    "att_wv2_m1bs": wv2_ingest_prod(
        'M', '.att', 7, ingest_id="att_from_zip_wv2_ftp_ingest"
    ),
    "att_wv2_p1bs": wv2_ingest_prod(
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
            "matts_wv2_ftp_ingest": {
                # wv2_2018_09_17T013226_fl_ne_058438311_10_0.zip
                "path_format": "wv2_%Y_%m_%dT%H%M%S_{area_short_name}_{order_id:09d}_10_0.zip",
            }
        },
        "imars_object_format": {
            "path": "{area_short_name}/zip_wv2_ftp_ingest",
            "basename": "wv2_%Y-%m-%dT%H%M%S_{area_short_name}.zip",
            "product_id": 6
        },
    },
    "zip_wv3_ftp_ingest": {
        "ingest_formats": {
            "matts_wv2_ftp_ingest": {
                # wv2_2018_09_17T013226_fl_ne_058438311_10_0.zip
                "path_format": "wv3_%Y_%m_%dT%H%M%S_{area_short_name}_{order_id:09d}_10_0.zip",
            }
        },
        "imars_object_format": {
            "path": "{area_short_name}/zip_wv3_ftp_ingest",
            "basename": "wv3_%Y-%m-%dT%H%M%S_{area_short_name}.zip",
            "product_id": 47
        },
    },
    "test_test_test": {
        "//": "this is a fake type used for testing only",
        "ingest_formats": {
            "file_w_date": {
                "//": "used to test auto date parsing from filename",
                "path_format": "file_w_date_%Y.txt",
            }
        },
        "imars_object_format": {
            "path": "test_test_test",
            "basename": "simple_file_with_no_args.txt",
            "product_id": -1
        },
    },
    "test_fancy_format_test": {
        "//": "this is a fake type used for testing only",
        "ingest_formats": {
            "file_w_date": {
                "//": "tests parsing from filename w/ args & date in it",
                "path_format": "date_%Y%j.arg_{test_arg}.time_%H%S.woah",
            },
            "file_w_nothing": {
                "path_format": "fake_filepath.bs",
            },
        },
        "imars_object_format": {
            "path": "_fancy_{test_arg}_/%Y-%j",
            "basename": "arg_is_{test_arg}_time_is_%H%S.fancy_file",
            "product_id": -2
        },
    },
    "test_number_format_test": {
        "//": "this is a fake type used for testing only",
        "ingest_formats": {
            "file_w_formatted_nums": {
                "//": "file format with numbers & format-specifiers",
                "path_format": "date_%Y.num_{test_num:3d}.time_%H.dude",
            },
        },
        "imars_object_format": {
            "path": "_fancy_{test_num:0>3d}_/%Y",
            "basename": "num_is_{test_num:0>4d}_time_is_%H.fancy_file",
            "product_id": -3
        },
    },

    # short_names come from MODAPS:
    #    https://modaps.modaps.eosdis.nasa.gov/services/about/products/c6/
    "myd01": {
        "//": "modis aqua l1. I *think* these files are the same as l1a_LAC,"
              " but from LANCE. " +
              "https://modaps.modaps.eosdis.nasa.gov" +
              "/services/about/products/c6/MYD01.html",
        "imars_object_format": {
            "path": "{area_short_name}/myd01",
            "basename": "A%Y%j.%H%M.hdf",
            "product_id": 5
        },
        "ingest_formats": {
            "modis_std_ish": {
                "//": [
                    "sorta like " +
                    " https://lpdaac.usgs.gov/dataset_discovery/modis"
                ],
                "path_format": "A%Y%j.%H%M.hdf",
            },
            "old_imars_obj": {
                "//": "imars-obj path pre 2018-05",
                "path_format": (
                    "/srv/imars-objects/" +
                    "modis_aqua_{area_short_name}/myd01/A%Y%j.%H%M.hdf"
                ),
            }
        }
    },
    "zip_ntf_wv2_ftp_ingest": {
        "//": "older format from digital globe with mostly ntf files at root",
        "imars_object_format": {
            "path": "NONE",  # <- TODO
            "basename": "wv2_ntf_%Y-%m-%dT%H%M_{tag}.zip",
            "product_id": 34
        },
        "ingest_formats": {
            "imars_object_basename_fmt": {
                "path_format": "wv2_ntf_%Y-%m-%dT%H%M_{tag}.zip",
            }
        }
    },
    "myd0_otis_l2": {
        "//": "modis aqua l2 files using @dotis's gpt graph'",
        "imars_object_format": {
            "path": "{area_short_name}/myd0_otis_l2",
            "basename": "myd0_otis_l2_A%Y%j%H%M%S.hdf",
            "product_id": 35
        },
        "ingest_formats": {
            "dotis_cron_output": {
                "path_format": "A%Y%j%H%M%S.L2",
            }
        }
    },
    "s3a_ol_1_efr": {
        "//": "s3 efr files",
        "imars_object_format": {
            "path": "{area_short_name}/s3a_ol_1_efr",
            "basename": "S3A_OL_1_EFR____%Y%m%dT%H%M%S.SEN3",
            "product_id": 36
        },
        "ingest_formats": {
            "slstr": {
                "//": [
                    "https://sentinel.esa.int/web/sentinel/user-guides" +
                    "/sentinel-3-slstr/naming-convention",
                    "--------------------------------------------------",
                    "`path_format` below assumes the following : ",
                    '  * "timeliness" is "NR", meaning Near-Real_Time (NRT)',
                    '  * "platform" is "O" (operational)',
                    '  * "generating centre" is "SVL" ' +
                    '("Svalbard Satellite Core Ground Station")',
                    "--------------------------------------------------",
                    'additionally, note that instance ID for s3a EFR files',
                    ' is formatted with "frames" metadata'
                ],
                "path_format": (
                    "S3A_OL_1_EFR____%Y%m%dT%H%M%S" +
                    "_{end_date:08d}T{end_t:06d}_{ing_date:08d}T{ing_t:06d}" +
                    "_{duration:04d}_{cycle:03d}_{orbit:03d}_{frame:04d}" +
                    "_SVL_O_NR_{base_collection:03d}.SEN3"
                )
            },
            "dhus_abbrev": {
                "//": "S3 SEN3 file path format from DHUS (shortened)",
                "path_format": (
                    "S3A_OL_1_EFR____%Y%m%dT%H%M%S.SEN3"
                )
            }
        }
    },
    "tif_r_rs_wv2": {
        "//": "wv2 classification R_rs output tif",
        "imars_object_format": {
            "path": "{area_short_name}/tif_r_rs_wv2",
            "basename": "wv2_Rrs_%Y%m%dT%H%M%S.tif",
            "product_id": 37
        },
        "ingest_formats": {
            "airflow": {
                "path_format": (
                    "proc_wv2_classification_na_%Y%m%dT%H%M%S_output" +
                    "/{run_id:1d}_{loc}_Rrs.tif"
                )
            }
        }
    },
    "tif_rrs_wv2": {
        "//": "wv2 classification rrs output tif",
        "imars_object_format": {
            "path": "{area_short_name}/tif_rrs_wv2",
            "basename": "wv2_rrs_%Y%m%dT%H%M%S.tif",
            "product_id": 38
        },
        "ingest_formats": {
            "airflow": {
                "path_format": (
                    "proc_wv2_classification_na_%Y%m%dT%H%M%S_output" +
                    "/{:1d}_{loc}_rrs.tif"
                )
            }
        }
    },
    "tif_bathy_wv2": {
        "//": "wv2 classification rrs output tif",
        "imars_object_format": {
            "path": "{area_short_name}/tif_bathy_wv2",
            "basename": "wv2_bathy_%Y%m%dT%H%M%S.tif",
            "product_id": 39
        },
        "ingest_formats": {
            "airflow": {
                "path_format": (
                    "proc_wv2_classification_na_%Y%m%dT%H%M%S_output" +
                    "/{:1d}_{loc}_Bathy.tif"
                )
            }
        }
    },
    "tif_classification": {
        "//": "wv2 classification rrs output tif",
        "imars_object_format": {
            "path": "{area_short_name}/tif_classification",
            "basename": "wv2_classification_%Y%m%dT%H%M%S.tif",
            "product_id": 40
        },
        "ingest_formats": {
            "airflow-filt": {
                "path_format": (
                    "proc_wv2_classification_na_%Y%m%dT%H%M%S_output/" +
                    "{:1d}_{loc}_DT_filt_{id_num}_{filter}_{stat}.tif"
                )
            },
            "airflow-nofilt": {
                "path_format": (
                    "proc_wv2_classification_na_%Y%m%dT%H%M%S_output/" +
                    "{:1d}_{loc}_DT_nofilt_{id_num}.tif"
                )
            }
        }
    },
    "zip_s3a_ol_1_efr": {
        "//": "s3 zip ingests",
        "imars_object_format": {
            "path": "{area_short_name}/zip_s3a_ol_1_efr",
            "basename": "S3A_OL_1_EFR____%Y%m%dT%H%M%S.zip",
            "product_id": 41
        },
        "ingest_formats": {
            "slstr": {
                "//": [
                    "https://sentinel.esa.int/web/sentinel/user-guides" +
                    "/sentinel-3-slstr/naming-convention",
                    "--------------------------------------------------",
                    "`path_format` below assumes the following : ",
                    '  * "timeliness" is "NR", meaning Near-Real_Time (NRT)',
                    '  * "platform" is "O" (operational)',
                    '  * "generating centre" is "SVL" ' +
                    '("Svalbard Satellite Core Ground Station")',
                    "--------------------------------------------------",
                    'additionally, note that instance ID for s3a EFR files',
                    ' is formatted with "frames" metadata'
                ],
                "path_format": (
                    "S3A_OL_1_EFR____%Y%m%dT%H%M%S" +
                    "_{end_date:08d}T{end_t:06d}_{ing_date:08d}T{ing_t:06d}_" +
                    "{duration:04d}_{cycle:03d}_{orbit:03d}_{frame:04d}_SVL_O_NR" +
                    "_{base_collection:03d}.zip"
                )
            }
        }
    },
    "chlor_a_l3_pass": {
        "//": "modis aqua l3 from @dotis's airflow'",
        "imars_object_format": {
            "path": "{area_short_name}/chlor_a_l3_pass",
            "basename": "chlor_a_l3_pass_A%Y%j%H%M%S.hdf",
            "product_id": 42
        },
        "ingest_formats": {
            "dotis_cron_output": {
                "path_format": "A%Y%j%H%M%S.L3",  # ???
            }
        }
    },
    "a1km_chlor_a_7d_mean_png": {
        "//": "modis aqua png from @dotis",
        "imars_object_format": {
            "path":
                "modis_aqua_{area_short_name}/png_chl_7d",
            "basename": "FGB_A1km_chlor_a_%Y%j_{date_2:06}_7D_MEAN.png",
            "product_id": 43
        },
        "ingest_formats": {
            "dotis_cron_output": {
                "path_format":
                    "FGB_A1km_chlor_a_%Y%j_{date_2:06d}_7D_MEAN.png",
            }
        }
    },
    "a1km_chlor_a_7d_anom_png": {
        "//": "modis aqua png from @dotis",
        "imars_object_format": {
            "path":
                "modis_aqua_{area_short_name}/png_chl_7d",
            "basename": "FGB_A1km_chlor_a_%Y%j_{date_2:06d}_7D_ANOM.png",
            "product_id": 44
        },
        "ingest_formats": {
            "dotis_cron_output": {
                "path_format":
                    "FGB_A1km_chlor_a_%Y%j_{date_2:06d}_7D_ANOM.png",
            }
        }
    },
    "a1km_sst_7d_mean_png": {
        "//": "modis aqua png from @dotis",
        "imars_object_format": {
            "path": (
                "modis_aqua_{area_short_name}/PNG_AQUA/SST/"
                "/PNG_AQUA/SST"
            ),
            "basename": "FGB_A1km_chlor_a_%Y%j_{date_2:06d}_7D_MEAN.png",
            "product_id": 45
        },
        "ingest_formats": {
            "dotis_cron_output": {
                "path_format":
                    "FGB_A1km_sst_%Y%j_{date_2:06d}_7D_MEAN.png",
            }
        }
    },
    "a1km_sst_7d_anom_png": {
        "//": "modis aqua png from @dotis",
        "imars_object_format": {
            "path": (
                "modis_aqua_{area_short_name}/PNG_AQUA/SST/"
                "PNG_AQUA/SST"
            ),
            "basename": "FGB_A1km_chlor_a_%Y%j_{date_2:06d}_7D_ANOM.png",
            "product_id": 46
        },
        "ingest_formats": {
            "dotis_cron_output": {
                "path_format":
                    "FGB_A1km_sst_%Y%j_{date_2:06d}_7D_ANOM.png",
            }
        }
    },
    "chlor_a_s3a_pass": {
        "//": "sentinel 3",
        "imars_object_format": {
            "path": (
                "{area_short_name}/chlor_a_s3_pass/"
            ),
            "basename": "chlor_a_s3_pass_"+ISO_8601_FMT,
            "product_id": 48
        },
        "ingest_formats": {}
    },
    "s3a_ol_1_efr_l2": {
        "//": "sentinel 3 chlor a level 2",
        "imars_object_format": {
            "path": (
                "{area_short_name}/s3a_ol_1_efr_l2/"
            ),
            "basename": "s3a_ol_1_efr_l2_"+ISO_8601_FMT,
            "product_id": 49
        },
        "ingest_formats": {
            "airflow": {
                "//": (
                    "eg: /srv/imars-objects/airflow_tmp/" +
                    "processing_s3_chloro_a__florida_20180622T162525000000_" +
                    "l2_file",
                ),
                "path_format": (
                    "processing__s3_chloro_a_{region_shortname}_" +
                    "%Y%m%dT%H%M%S%f_l2_file"
                )
            }
        }
    },
    "s3a_ol_1_efr_l3": {
        "//": "sentinel 3 chlor a level 3",
        "imars_object_format": {
            "path": (
                "{area_short_name}/s3a_ol_1_efr_l3/"
            ),
            "basename": "s3a_ol_1_efr_l3_"+ISO_8601_FMT,
            "product_id": 50
        },
        "ingest_formats": {
            "airflow": {
                "//": (
                    "eg: /srv/imars-objects/airflow_tmp/" +
                    "processing_s3_chloro_a__florida_20180622T162525000000_" +
                    "l3_file",
                ),
                "path_format": (
                    "processing__s3_chloro_a_{region_shortname}_" +
                    "%Y%m%dT%H%M%S%f_l3_file"
                )
            }
        }
    },


    # === others from the metadata db that in need of adding:
    # "png_chl_7d": {
    #     "name": "FGB_A1km_chlor_a_%Y%j_%Y%j_7D_MEAN.png"
    # },
    # === legacy values from pre-metadata db times:
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
    # "metadata-ini": {
    #     "basename": "metadata_"+ISO_8601_FMT+".ini"
    # }
    # "png": {
    #     "path": (
    #         "modis_aqua_{region_shortname}" +
    #         "/png_{variable_name}"
    #     ),
    #     "basename": ISO_8601_FMT + "_{variable_name}.png"
    # }
}

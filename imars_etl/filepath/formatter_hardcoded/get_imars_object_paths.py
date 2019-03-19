from imars_etl.filepath.formatter_hardcoded.data import data


def get_imars_object_paths():
    """
    Returns a dict of all imars_object paths keyed by product name.

    example:
    {
        "test_test_test":{
            "//": "this is a fake type used for testing only",
            "basename": "simple_file_with_no_args.txt",
            "path"    : "test_test_test",
            "product_id": -1
        },
        "zip_wv2_ftp_ingest":{
            "basename": "wv2_%Y_%m_{tag}.zip",
            "path"    : "{product_type_name}",
            "product_id": 6
        },
        "att_wv2_m1bs":{
            "basename": "WV2_%Y%m%d%H%M%S-M1-{idNumber}_P{passNumber}.att",
            "path": "extra_data/WV02/%Y.%m",
            "product_id": 7
        }
    }
    """
    res = {}
    for product_id in data:
        res[product_id] = data[product_id]["imars_object_format"]
    return res

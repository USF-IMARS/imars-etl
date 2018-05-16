from imars_etl.filepath.data import data

def get_imars_object_paths():
    """
    returns a dict of all imars_object paths keyed by product name.

    example:
    {
        "test_test_test":{
            "//": "this is a fake type used for testing only",
            "basename": "simple_file_with_no_args.txt",
            "path"    : "/srv/imars-objects/test_test_test",
            "product_id": -1
        },
        "zip_wv2_ftp_ingest":{
            "basename": "wv2_%Y_%m_{tag}.zip",
            "path"    : "/srv/imars-objects/{product_type_name}",
            "product_id": 6
        },
        "att_wv2_m1bs":{
            "basename": "WV02_%Y%m%d%H%M%S_0000000000000000_%y%b%d%H%M%S-M1BS-{idNumber}_P{passNumber}.att",  # NOTE: how to %b in all caps?
            "path": "/srv/imars-objects/extra_data/WV02/%Y.%m",
            "product_id": 7
        }
    }
    """
    res = {}
    for product_id in data:
        res[product_id] = data[product_id]["imars_object_format"]
    return res

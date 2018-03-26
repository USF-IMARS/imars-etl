"""
dict which maps `imars_product_metadata.product.short_name` to expected
   filename patterns. Used to infer metadata from the filepath.
"""
filename_patterns = {
    "zip_wv2_ftp_ingest": "wv2_%Y_%m_{tag}.zip",
    "att_wv2_m1bs": "%y%b%d%H%M%S-M1BS-{idNumber}_P{passNumber}.ATT"
    # eg: 16FEB12162518-M1BS-057522945010_01_P002.ATT
}
# dict containing valid values for given variables that can be used in the
# filename patters above
valid_pattern_vars = {
    "area": [
    # TODO: fill this from the database automatically
    ],
    "tag":       ["_",  "*", ".zip"],  # * matches any str between edge strings (not inclusive)
    "idNumber":  ["BS-",  "*", "_P"],
    "passNumber":["_P", "*", ".ATT"]
}

# NOTE: can FIND_REGEX be replaced by allowing imars-etl to search the dir
#       w/ INGESTABLE_FORMAT instead? (see #3)
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
            "basename": "WV02_%Y%m%d%H%M%S_0000000000000000_%y%b%d%H%M%S-M1BS-{idNumber:12d}_{whatThis:2d}_P{passNumber:3d}.att"
        }
    },
    "zip_wv2_ftp_ingest": {
        "ingest_formats": {
            "wv2 ftp ingest for matt":{
                "find_regex": "/srv/imars-objects/ftp-ingest/wv2_*zip",
                "path_format": "wv2_%Y_%m_{tag}.zip",
            }
        },
        "imars_object_format":{
            "path": "/srv/imars-objects/zip_wv2_ftp_ingest",
            "basename": "wv2_%Y_%m_{tag}.zip",
        }
    }
}

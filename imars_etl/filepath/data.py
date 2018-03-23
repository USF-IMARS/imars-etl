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
    "idNumber":  ["-M1BS-",  "*", "_P"],
    "passNumber":["_P", "*", ".ATT"]
}

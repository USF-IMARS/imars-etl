# dict which maps `imars_product_metadata.product.short_name` to expected
#   filename patterns
filename_patterns = {
    "zip_wv2_ftp_ingest": "wv2_%Y_%m_{tag}.zip"
}
# dict containing valid values for given variables that can be used in the
# filename patters above
valid_pattern_vars = {
    "area": [  # TODO: fill this from the database automatically

    ],
    "tag": ["_", "*", ".zip"]  # * matches any str between edge strings (not inclusive)
}

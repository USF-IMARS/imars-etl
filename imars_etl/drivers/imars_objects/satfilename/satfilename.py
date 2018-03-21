"""
This file defines various functions to build filepaths in a standard way.
Some general rules/assumptions that summarize behavior here:

### directory structure:
* the root data directory is split up by region
* each "region subdirectory" contains a common list of product directories
* the "product subdirectories" should contain only one type of product (and one filetype).
    - Products from different sources, satellites, or processing methods should not share a product directory unless the products are identical.
    - "Products" made up of multiple filetypes must be split into multiple directories.
* no directory structure should exist beyond the "product subdirectories"

### defining "product"
* a "product" in this context is a group of files that have all metadata (processing/source provenence, region, etc) in common except for their datetime (and metadata affected by different datetime like satellite location or actual bounding box).
* different versions of products (ie if geo files are being generated in a new way) should be separated out into a new product directory, not lumped in with the older product.
    - Appending `_v2` or a more descriptive name to the end of the product directory as needed.
    - If the new product version *really* wants to include the older files, sym-links should be created to link to the older version's files (eg `ln -s ./2017-02-13_v2.GEO ../geo/2017-02-13.GEO`).
* empty directories should be deleted and created only when needed.

### filenames:
* filenames within a product directory should all conform to the same pattern
* filenames should include:
    - the datetime of the product (preferably in ISO 8601 format)
    - something to identify the "product type"
* the datetime should be the first part of the filename

### Example directory structure:
```
/root-data-directory
    /region1
        /myd01
        /mod01
        /myd03
        /m0d03
        /geo
        /geo_v2
    /region2
        /myd01
        /myd03
        /geo_v2
```
Note the common product directories and the two `geo` directories where a new version was separated out into a new product.
Filenames in `geo` and `geo_v2` are probably similar, but shoud not be identical.
"""

from imars_etl.drivers.imars_objects.satfilename.BaseSatFilepath import BaseSatFilepath

ISO_8601_FMT="%Y-%m-%dT%H:%M:%SZ"

PATH_ROOT="/srv/imars-objects/{region}/{product_type_id}/"
# TODO: create this from the metadata database rather than duplicating it here.
_products = {
    # "product_type_id": {
    #   "basename": "A%Y%j%H%M00.L1A_LAC.x.hdf.bz2"
    #   "path"    : "/srv/imars-objects/{region}/{product_type_id}/"
    # }
    "zip_wv2_ftp_ingest":{
        "basename": "wv2_%Y_%m_{tag}.zip",
        "path"    : "/srv/imars-objects/{product_type_name}",
        "product_type_id": 6
    },
    "att_wv2_m1bs":{
        "basename": "WV02_%Y%m%d%H%M%S_{catalog_id}_%y%b%d%H%M%S-M1BS-{id_number}_{two_numbers}_P{pass_number}.att",  # NOTE: how to %b in all caps?
        "path": "/srv/imars-objects/extra_data/WV02/%Y.%m",
        "product_type_id": 7
    }
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

def get_name(forced_basename=None, **kwargs):
    """
    kwargs are used to set metadata info that may be used in the formation of
    the path or basename.

    Returns
    ------------
    str
        path to file formed using given metadata in kwargs
    """
    print("placing {} (#{})...".format(
        kwargs.get('product_type_name','???'),
        kwargs.get('product_type_id',-999999))
    )
    for prod_name in _products:
        prod_meta = _products[prod_name]
        print("is {} (#{})?".format(prod_name, prod_meta['product_type_id']))
        if (
                   kwargs.get('product_type_name','') == prod_name
                or kwargs.get('product_type_id',-999999) == prod_meta['product_type_id']
            ):  # if file type or id is given and matches a known type
            print('y!')

            if forced_basename is not None:
                _basename = forced_basename
            else:
                _basename = prod_meta['basename']

            try:  # set product_type_name if not already set
                test = kwargs['product_type_name']
                # TODO: check for match w/ prod_name & raise if not?
            except KeyError as k_err:
                kwargs['product_type_name'] = prod_name

            return kwargs['datetime'].strftime(
                (prod_meta['path']+"/"+_basename).format(**kwargs)
            )
        else:
            print("no.")
    else:
        raise ValueError("could not identify product type")

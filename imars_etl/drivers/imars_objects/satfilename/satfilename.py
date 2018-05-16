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
import logging
import sys

from imars_etl.filepath.get_imars_object_paths import get_imars_object_paths

def get_name(args, forced_basename=None):
    """
    args are used to set metadata info that may be used in the formation of
    the path or basename.

    Returns
    ------------
    str
        path to file formed using given metadata in args
    """
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    logger.setLevel(logging.INFO)
    logger.info("placing {} (#{})...".format(
        args.get('product_type_name','???'),
        args.get('product_id',-999999))
    )
    for prod_name, prod_meta in get_imars_object_paths().items():
        logger.debug("is {} (#{})?".format(prod_name, prod_meta['product_id']))
        if (
                   args.get('product_type_name','') == prod_name
                or args.get('product_id',-999999) == prod_meta['product_id']
            ):  # if file type or id is given and matches a known type
            logger.debug('y!')

            if forced_basename is not None:
                _basename = forced_basename
            else:
                _basename = prod_meta['basename']

            try:  # set product_type_name if not already set
                test = args['product_type_name']
                # TODO: check for match w/ prod_name & raise if not?
            except KeyError as k_err:
                args['product_type_name'] = prod_name

            fullpath = prod_meta['path']+"/"+_basename
            logger.info("formatting imars-obj path \n>>'{}'".format(fullpath))
            return args['datetime'].strftime(
                (fullpath).format(**args)
            )
        else:
            logger.debug("no.")
    else:
        # logger.debug(args)
        raise ValueError("could not identify product type")

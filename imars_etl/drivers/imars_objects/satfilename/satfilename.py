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

# imars-etl
Tools for `extract` and `load` for IMaRS ETL (Extract, Transform, Load) operations.

This is a relatively light wrapper designed to help make loading and extracting files from IMaRS systems easier for end-users.

Usernames and passwords for connecting to IMaRS systems *might* be hard-coded here, so this repo should remain private.

# Overview
## Load
In this context "load" roughly means "upload my file to the IMaRS data warehouse".

To load a file you must provide the file and all required metadata.
Requirements are specified by the `file` table schema in [imars_puppet/.../product_metadata.sql](https://github.com/USF-IMARS/imars_puppet/blob/test/modules/role/files/sql/product_metadata.sql).
The current minimum metadata are requirements are:
* `date_time` : (start) date-time string of the product time granule
* `product_id` : identifier from the `product` table for this product
* `area_id` : identifier from the `area` table for this product

## Extract
In this context "extract" roughly means "download a file from IMaRS data warehouse".
To extract a file you must provide metadata to describe the file.
If your request returns multiple files, the script will prompt you for more information.
SQL queries can currently be tested in our blazer web GUI at [imars-physalis:3000](http://imars-physalis.marine.usf.edu:3000/).

# Usage
The "extract" and "load" commands are used to "download" and "upload" files and corresponding metadata.
The IMaRS-ETL tool abstracts the details of this process so that the user need not worry about the data lakes, databases, or fileshares under the hood.

## bash CLI
### extract
```bash
$ /opt/imars-etl/imars-etl.py extract --sql "area_id=1"
$ imars-etl.py -v extract 'date_time LIKE "2018-01-%"'
$ imars-etl extract 'area_id=1 && product_id=1'
$ imars-etl -vvv extract 'date_time < "2018-01-01" AND date_time > "2018-01-07"'

# Not yet implemented (short_name values (see issue #1)):
# you@computer:~/$ imars-etl extract --satellite aqua --time 2017-01-02T13:45 --instrument modis

# Not yet implemented (extract multiple by setting max (default is 1))
# you@computer:~/$ imars-etl extract --max 10 --satellite wv2 --time_start  2017-01-02T13:45 --time_end  2017-01-03T18:00

# Not yet implemented (output directory):
# you@computer:~/$ imars-etl extract --satellite wv2 --time 2017-02-02T13:45 --output /home/you/my_dir/

# Not yet implemented (list of products in file)
# you@computer:~/$ imars-etl extract --infile ./list_of_products.txt

# Not yet implemented (output links to network-available file rather than copy):
# you@computer:~/$ imars-etl extract --link "product_id=3 AND date_time='2019-04-24 11:55:03.591485'"
# you@computer:~/$ imars-etl extract --link --max inf "product_id=3 AND date_time LIKE '2018%'"
```
### load
```bash
# use imars_etl find to load a directory:
imars-etl find -p 14 /srv/imars-objects/big_bend/wv2/2014 \
    | xargs -n 1 imars-etl load

ls /srv/imars-objects/ftp-ingest/wv3_2018_09_17T0336* \
    | xargs -n 1 imars-etl -v load --nohash --dry_run

# load all wv2 xml files from 2014 and call them area=6 (big_bend)
ls /srv/imars-objects/big_bend/wv2/2014/*
    | xargs -n 1 imars-etl -v load \
    -p 14 \
    -j '{"status_id":3, "area_id":6}' \
    --object_store no_upload \
    -l "WV02_%Y%m%d%H%M%S_{junk}-M1BS-{idNumber:12}_{otherNum:2}_P{passNumber:0>3d}.xml" \
    --metadata_file_driver wv2_xml  \
    --metadata_file "{filepath}"  \
    --nohash

# same as above but for ntf files:
ls /srv/imars-objects/big_bend/wv2/2014/*
    | xargs -n 1 imars-etl -v load \
    -p 11 \
    -j '{"status_id":3, "area_id":6}' \
    --object_store no_upload \
    -l "WV02_%Y%m%d%H%M%S_{junk}-M1BS-{idNumber:12}_{otherNum:2}_P{passNumber:0>3d}.ntf" \
    --metadata_file_driver wv2_xml  \
    --metadata_file "{directory}/{basename}.xml"  \
    --nohash

# practice loading a WV2 .zip file (pid 6) from DigitalGlobe:
#     NOTE: use `--dry_run` to practice
#     NOTE: `area_id:7` in the json must match fl_se
imars-etl -v load \
    -p 6 \
    -j '{"status_id":3, "area_id":7}' \
    --object_store no_upload \
    --dry_run \
    /srv/imars-objects/ftp-ingest/wv2_2018_10_08T115750_fl_se_058523212_10_0.zip

# manually enter the time
$ imars-etl load --time "2017-01-02T13:45" --product_id 4 /path/to/file.hdf

# auto-parse info (date) from filename using info from `filepanther.data`
$ imars-etl load --product_id 4 /path/to/file/wv2_2012_02_myChunk.zip

# use product short_name instead of id:
you@computer:~/$ imars-etl load --satellite aqua --time 2017-01-02T13:45 --instrument modis /path/to/file.hdf

# load all matching files from a dir
you@comp:~/$ ls /tmp/myDir/ | xargs -n 1 imars-etl load --ingest_name matts_wv2_ftp_ingest -p 6

# load with metadata read from ESA DHUS json file:
you@comp:~/$ imars-etl load --metadata_file myfile_meta.json myfilepath.SEN3
```

## python API
(not yet implemented)
### extract
```python
from imars_etl import extract
local_filepath = extract({
    "sql": "product_id=3 AND status_id=3"
})
```
### load
```python
from imars_etl import load
my_local_file_path = "/tmp/myfile.png"
load({
    "filepath": my_local_file_path,
    "product_id": 6,
    "area_id": 2,
    "date_time": "2018-03-06T17:14",
    "status_id": 3
})
```

```python
import imars_etl
args={'filepath': '/srv/imars-objects/airflow_tmp/processing_s3_chloro_a__florida_20180622T162525000000_l2_file', 'json': '{"area_short_name":"florida"}', 'sql': "product_id=49 AND area_id=12 AND date_time=2018-06-22T16:25:25+00:00"}
imars_etl.load(**args)
```

# Installation
See INSTALL.md

# Technical Details
## Backends
Currently only one object storage & metadata db are supported.
### Object Storage
The primary object storage method is a custom-coded NFS+autofs kludge managed via puppet called imars-objects.
More detail in [IMaRS-docs/.../imars-objects/](https://github.com/USF-IMARS/IMaRS-docs/tree/master/docs/management_data/imars-objects)

Object storage in a cloud provider or distrubuted file system is on the todo list; IPFS would be my first choice.

### Metadata DB
The primary metadata db is a mysql db living on imars-sql-hydra and managed by puppet.
More detail at [IMaRS-docs/.../imars_product_metadata.md](https://github.com/USF-IMARS/IMaRS-docs/blob/master/docs/management_data/imars_product_metadata.md).

Expansion into other metdataDBs could potentially create problems of duplicate or conflicting records.
However, the addition of ingest-only databases like NASA's CMR could be a very powerful way to access off-site files.

Complications of a multi-db metdata search are explored a bit in [this gist](https://gist.github.com/7yl4r/966222ce6a8557ab79b079ff17433960).

## Object Storage & Metadata connections
Connections are managed using apache-airflow hooks.
Connections can be installed using the `airflow connections` command.
Within the code connections are wrapped in a few ways to unify backend
interactions down to two interfaces: "object_storage" and "metadata_db".

The hooks provided by airflow often have differing interfaces.
Multiple HookWrappers are provided here to wrap around one or more hook classes
and provide a consistent interface to object_storage or metadata_db backends.
Two HookHandlers (ObjectStoreHookHandler and MetadataDBHookHandler) are provided
to serve as the topmost interface.
HookHandlers apply the appropriate wrappers to hooks to provide either a
metadata or object-store interface, so users don't have to worry about
HookHandlers.

```
# OSI-like model:

        |-------------------------------------+-----------------------------|
methods | .load() .extract()                  | .get_first() .get_records() |
        | .format_filepath()                  | .insert_rows()              |
        |-------------------------------------|-----------------------------|
Handles | ObjectStoreHookHandler              | MetadataDBHookHandler       |
        |---------------+---------------------|-----------------------------|
Wrappers| FSHookWrapper | DataLakeHookWrapper |                             |
        |---------------|-----+---------------|-----------------------------|
Hooks   |       FS      | S3  | AzureDataLake | DbAPIHook                   |
        |---------------|-----|---------------|------------+-------+--------|
Backends| HD| NFS| FUSE | S3  | Azure         | MySQL      | MsSQL | SQLite |
        |---------------+-----+---------------+------------+-------+--------+
```

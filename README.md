# imars-etl
Tools for `extract` and `load` for IMaRS ETL (Extract, Transform, Load) operations.

This is a relatively light wrapper designed to help make loading and extracting files from IMaRS systems easier for end-users.

Usernames and passwords for connecting to IMaRS systems *might* be hard-coded here, so this repo should remain private.

# Overview
## Load
In this context "load" roughly means "upload my file to the IMaRS data warehouse".
To load a file you must also provide all required metadata.

## Extract
In this context "extract" roughly means "download a file from IMaRS data warehouse".
To extract a file you must provide metadata to describe the file.
If your request returns multiple files, the script will prompt you for more information.

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
# you@computer:!/$ imars-etl extract --satellite wv2 --time 2017-02-02T13:45 --output /home/you/my_dir/

# Not yet implemented (list of products in file)
# you@computer:!/$ imars-etl extract --infile ./list_of_products.txt
```
### load
```bash
# load all wv2 xml files from 2014 and call them area=6 (big_bend)
python3 -m imars_etl -v load \
    -d /srv/imars-objects/big_bend/wv2/2014 \
    -p 14 \
    -j '{"status_id":3, "area_id":6}' \
    --object_store no_upload \
    -l "WV02_%Y%m%d%H%M%S_{junk}-M1BS-{idNumber:12}_{otherNum:2}_P{passNumber:0>3d}.xml" \
    --metadata_file_driver wv2_xml  \
    --metadata_file "{filepath}"  \
    --nohash

# same as above but for ntf files:
python3 -m imars_etl -v load \
    -d /srv/imars-objects/big_bend/wv2/2014 \
    -p 11 \
    -j '{"status_id":3, "area_id":6}' \
    --object_store no_upload \
    -l "WV02_%Y%m%d%H%M%S_{junk}-M1BS-{idNumber:12}_{otherNum:2}_P{passNumber:0>3d}.ntf" \
    --metadata_file_driver wv2_xml  \
    --metadata_file "{directory}/{basename}.xml"  \
    --nohash

$ imars-etl load --area 1 --time "2017-01-02T13:45" --product_id 4 --filepath /path/to/file.hdf

# auto-parse info (date) from filename using info from `imars_etl.filepath.data`
$ imars-etl load --area 1 --product_id 4 --filepath /path/to/file/wv2_2012_02_myChunk.zip

# use product short_name instead of id:
you@computer:~/$ imars-etl load --satellite aqua --time 2017-01-02T13:45 --instrument modis /path/to/file.hdf

# load all matching files from a dir
you@comp:~/$ imars-etl load --ingest_name matts_wv2_ftp_ingest -p 6 --directory /tmp/myDir

# load with metadata read from ESA DHUS json file:
you@comp:~/$ imars-etl load --filepath myfilepath.SEN3 \
    --metadata_file myfile_meta.json
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

# Installation
See INSTALL.md

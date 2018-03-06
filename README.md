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
$ imars-etl extract 'area_id=1 && product_type_id=1'
$ imars-etl -vvv extract 'date_time < "2018-01-01" AND date_time > "2018-01-07"'

# Not yet implemented (short_name values (see issue #1)):
# you@computer:~/$ imars-etl extract --satellite aqua --time 2017-01-02T13:45 --instrument modis
```
### load
```bash
$ imars-etl load --area 1 --date "2017-01-02T13:45" --type 4 --filepath /path/to/file.hdf

# auto-parse info (date) from filename using info from `imars_etl.filepath.data`
$ imars-etl load --area 1 --type 4 --filepath /path/to/file/wv2_2012_02_myChunk.zip

# Not yet implemented (short_name values (see issue #1)):
# you@computer:~/$ imars-etl load --satellite aqua --time 2017-01-02T13:45 --instrument modis /path/to/file.hdf
```

## python API
(not yet implemented)
### extract
```python
from imars_etl import extract
local_filepath = extract({
    "sql": "product_type_id=3 AND status=3"
})
```
### load
```python
from imars_etl import load
my_local_file_path = "/tmp/myfile.png"
load({
    "filepath": my_local_file_path,
    "product_type_id": 6,
    "area_id": 2,
    "date_time": "2018-03-06T17:14",
    "status": 3
})
```

# imars-etl
Tools for `extract` and `load` for IMaRS ETL (Extract, Transform, Load) operations.

This is a relatively light wrapper designed to help make loading and extracting files from IMaRS systems easier for end-users.

Usernames and passwords for connecting to IMaRS systems *might* be hard-coded here, so this repo should remain private.

## Usage
The "extract" and "load" commands are used to "download" and "upload" files and corresponding metadata.
The IMaRS-ETL tool abstracts the details of this process so that the user need not worry about the data lakes, databases, or fileshares under the hood.

### Extract
In this context "extract" roughly means "download a file from IMaRS data warehouse".
To extract a file you must provide metadata to describe the file.
If your request returns multiple files, the script will prompt you for more information.

Examples:

```
you@computer:~/$ /opt/imars-etl/imars-etl.py extract --sql "area_id=1"
you@computer:~/$ imars-etl.py -v extract 'date_time LIKE "2018-01-%"'
you@computer:~/$ imars-etl extract 'area_id=1 && product_type_id=1'
you@computer:~/$ imars-etl -vvv extract 'date_time < "2018-01-01" AND date_time > "2018-01-07"'

# Not yet implemented (short_name values (see issue #1)):
# you@computer:~/$ imars-etl extract --satellite aqua --time 2017-01-02T13:45 --instrument modis
```

### Load
In this context "load" roughly means "upload my file to the IMaRS data warehouse".
To load a file you must also provide all required metadata.

Examples:

```
you@computer:~/$ imars-etl load --area 1 --date "2017-01-02T13:45" --type 4 --filepath /path/to/file.hdf

# auto-parse info (date) from filename using info from `imars_etl.filepath.data`
you@computer:~/$ imars-etl load --area 1 --type 4 --filepath /path/to/file/wv2_2012_02_myChunk.zip

# Not yet implemented (short_name values (see issue #1)):
# you@computer:~/$ imars-etl load --satellite aqua --time 2017-01-02T13:45 --instrument modis /path/to/file.hdf
```

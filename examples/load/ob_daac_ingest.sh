#!/bin/bash
# This example loads all OB.DAAC files in the OUTPUT_PATH.
#
# contacts: Tylar Murray, Dan Otis

OUTPUT_PATH="./"
# this was the output path used before integrating with imars-etl:
#OUTPUT_PATH="/home1/scratch/epa/satellite/modis/GOM/L2_MODA_sub/"

# example files paths:
# ${OUTPUT_PATH}A2019316185000.L2_LAC_OC.nc
# ${OUTPUT_PATH}A2019316185000.L2_LAC_SST.nc
# ${OUTPUT_PATH}A2019316185500.L2_LAC_OC.nc
# ${OUTPUT_PATH}A2019316185500.L2_LAC_SST.nc

# === ingest with `imars-etl load`:
# this string can be anything; it describes the origin of the file
# that we are loading. If you are copying this script **please** 
# change it.
PROVENANCE_STR="example_modis_dotis_ingest_l2_v1"

# here we assume the area is Gulf of Mexico
GOM_AREA_ID=1

# we load the SST product, and then the OC product
SST_PRODUCT_ID=51
OC_PRODUCT_ID=41

ls ${OUTPUT_PATH}A*.L2_LAC_SST.nc \
	| xargs -n 1 imars-etl load \
	--load_format 'A%Y%j%H%M%S.{obdaac_id}.nc' \
	--sql "area_id=$GOM_AREA_ID AND \
		product_id=$SST_PRODUCT_ID AND \
		provenance=\"$PROVENANCE_STR\"\
	"
ls ${OUTPUT_PATH}A*.L2_LAC_OC.nc \
	| xargs -n 1 imars-etl load \
	--load_format 'A%Y%j%H%M%S.{obdaac_id}.nc' \
	--sql "area_id=$GOM_AREA_ID AND \
		product_id=$OC_PRODUCT_ID AND \
		provenance=\"$PROVENANCE_STR\"\
	"

# explaination: the above bash commands use the ls, pipe `|`, and xargs
#	concepts. You should review these individually but to summarize:
#	1. `ls` lists all the files
#	2. the list is piped to xargs
#	3. xargs splits up the list to pass one argument at a time
#	4. imars-etl load consumes one argument at a time to load the file
#		4a. the SQL passed to imars-etl describes the metadata of the file
#		4b. additional metadata may be automatically read from the file path & file contents.
#			4.b.i. the file path format is specified by --load_format

# === next steps:

# now that they are loaded we can delete the local files...
# rm ./*.nc

# ...and get them back later using `imars-etl extract`:
# imars-etl extract 'WHERE area_id=1 AND product_id=51 AND datetime="2019-11-15 18:55:00"'

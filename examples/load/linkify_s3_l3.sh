#!/bin/bash 
# Loads all files in the S3_map directory.
# this is a less verbose version of examples/load/s3_linkify.sh

FILES=/home1/cestes/s3proc/S3_map/*
for FPATH in $FILES
do
	if [[ -L "$FPATH" ]]
	then
		echo "$FPATH is already a symlink"
	else
		/opt/imars_etl/scripts/linkify_file.sh \
			48 \
			9 \
			S3A_OL_1_EFR____%Y%m%dT%H%M%S_2.map.tif \
			tylar_manual_ingest-2019-11 \
			$FPATH
	fi
done

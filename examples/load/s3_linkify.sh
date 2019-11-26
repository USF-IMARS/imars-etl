#!/bin/bash
# ingest all the files in /home1/cestes/s3proc/S3_zip and replace them with symlinks

set -e

FMT_STR=

FILES=/home1/cestes/s3proc/S3_zip/*
for FPATH in $FILES
do
	if [[ -L "$FPATH" ]]
	then
		echo "$FPATH is already a symlink"
	else
		# P_ID (36,'s3a_ol_1_efr',    'Sentinel-3 L1B OFR .SEN3 zip',NULL,NULL,'sentinel-3','ocli','3min','300m'),
		# Area 9 = monroe	
		P_ID=36
		A_ID=9
		FMT_STR="S3{sat_id}_OL_1_EFR____%Y%m%dT%H%M%S_{end_date:08d}T{end_t:06d}_{ing_date:08d}T{ing_t:06d}_{duration:04d}_{cycle:03d}_{orbit:03d}_{frame:04d}_{proc_location}_{platform}_{timeliness}_{base_collection:03d}.zip"
		PROV=linkify-ingest_tylar_2019-11_v2

		SQL="product_id=$P_ID AND area_id=$A_ID AND provenance=\"$PROV\""

		echo sql: $SQL

		imars-etl load \
			--sql "$SQL" \
			--load_format "$FMT_STR" \
			$FPATH

		rm $FPATH
		ln -s /srv/imars-objects/monroe/s3a_ol_1_efr/$FPATH $FPATH
	fi
done


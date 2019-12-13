#!/bin/bash 
# Loads all wv2 ntf files from pgc ingest.

FILES=$(find /thing2/imars-objects/pgc_batches/imagery/ -name 'WV02*M1BS*.ntf')
for FPATH in $FILES
do
	if [[ -L "$FPATH" ]]
	then
		echo "$FPATH is already a symlink"
	else
    # TODO: this can't work because we don't know the area
		# prod # 11 
		# area # ??? 
		#/opt/imars_etl/scripts/linkify_file.sh \
		#	11 \
		#	??? \
		#	S3A_OL_1_EFR____%Y%m%dT%H%M%S_2.map.tif \
		#	tylar_manual_ingest-2019-11 \
		#	$FPATH
	fi
done

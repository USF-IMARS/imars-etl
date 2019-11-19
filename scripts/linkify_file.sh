#!/bin/bash
# USAGE: linkify_file.sh $P_ID $A_ID $FMT_STR $PROVENANCE $filepath
# EXAMPLES:
#	linkify_file.sh 51 1 'A%Y%j%H%M%S.{obdaac_id}.nc' custom_linkified_file_tylar_v1 ./A2018301132358.LAC_L2_SST.nc

P_ID=$1
A_ID=$2
FMT_STR=$3
PROV=$4
FPATH=$5

set -e

SQL="product_id=$P_ID AND area_id=$A_ID and provenance=\"$PROV\""

# imars-etl load \
	--sql "$SQL" \
	--load_format "$FMT_STR" \
	FPATH
	
rm $FPATH
NEW_FPATH=$(imars-etl extract --method link --sql "$SQL")
mv $NEW_FPATH $FPATH

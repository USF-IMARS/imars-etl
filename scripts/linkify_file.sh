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

SQL="product_id=$P_ID AND area_id=$A_ID AND provenance=\"$PROV\""

echo "loading SQL : $SQL"

FILE_SQL=$( \
	imars-etl -q load \
		--sql "$SQL" \
		--load_format "$FMT_STR" \
		$FPATH \
)

echo "new file SQL: $FILE_SQL"

TMP_PATH=/tmp/${FPATH}
mv $FPATH $TMP_PATH
NEW_FPATH=$(imars-etl -q extract --method link "$FILE_SQL")

if [[ $NEW_FPATH -ef $FPATH ]]
then
	echo "verified new link is correct"
else
	echo "fixing new link path"
	mv $NEW_FPATH $FPATH
fi

rm $TMP_PATH

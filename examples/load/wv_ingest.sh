#!/bin/bash 
# Loads all wv2 ntf files from pgc ingest.


find /srv/imars-objects/tpa_gs/migration/monroe/ -name 'WV02*M1BS*.ntf' \
	| xargs -n 1 imars-etl load \
		--duplicates_ok
		--sql 'product_id=11 AND area_id=9 AND provenance="tylar_manual_ingest-2020-03"' \
		--metadata_file_driver wv2_xml  \
		--metadata_file "{directory}/{basename}.xml" \
		--load_format 'WV02_%Y%m%d%H%M%S_{unknown_number}_%y%b%d%H%M%S-M1BS-{idNumber}_P{passNumber}.ntf' \
		--no_load
		
find /srv/imars-objects/tpa_gs/migration/monroe/ -name 'WV02*M1BS*.xml' \
	| xargs -n 1 imars-etl load \
		--duplicates_ok
		--sql 'product_id=14 AND area_id=9 AND provenance="tylar_manual_ingest-2020-03"' \
		--metadata_file_driver wv2_xml  \
		--metadata_file "{directory}/{basename}.xml" \
		--load_format 'WV02_%Y%m%d%H%M%S_{unknown_number}_%y%b%d%H%M%S-M1BS-{idNumber}_P{passNumber}.ntf' \
		--no_load

#!/bin/bash 
# Loads all wv2 ntf files from pgc ingest.

find /srv/imars-objects/tpa_gs/migration/alab/ -name 'WV02*M1BS*.ntf' \
	| xargs -n 1 imars-etl load \
		--sql 'product_id=11 AND area_id=19 AND provenance="tylar_manual_ingest-2020-02"' \
		--metadata_file_driver wv2_xml  \
		--metadata_file "{directory}/{basename}.xml"

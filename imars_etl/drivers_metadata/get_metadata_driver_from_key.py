from imars_etl.drivers_metadata import dhus_json
from imars_etl.drivers_metadata.wv2_xml import wv2_xml
# map from input strings to load_file functions for each backend
DRIVER_MAP_DICT = {
    'dhus_json': dhus_json.Parser,
    'wv2_xml': wv2_xml.Parser,
}


def get_metadata_driver_from_key(key):
    return DRIVER_MAP_DICT[key]

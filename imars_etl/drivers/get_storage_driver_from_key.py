from imars_etl.drivers.IMaRSObjectsObjectHook import IMaRSObjectsObjectHook
from imars_etl.drivers.NoBackendObjectHook import NoBackendObjectHook

# map from input strings to load_file functions for each backend
DRIVER_MAP_DICT = {
    'imars_objects': IMaRSObjectsObjectHook().load,
    'no_upload': NoBackendObjectHook().load,
}


def get_storage_driver_from_key(key):
    return DRIVER_MAP_DICT[key]

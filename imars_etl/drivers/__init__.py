from imars_etl.drivers.imars_objects.load_file import load_file
from imars_etl.drivers.NoBackendStorageDriver import NoBackendStorageDriver

# map from input strings to load_file functions for each backend
DRIVER_MAP_DICT = {
    'imars_objects': load_file,
    'no_upload': NoBackendStorageDriver().load,
}

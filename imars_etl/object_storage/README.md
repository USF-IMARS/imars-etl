An object storage "ObjectHook" is used to provide an interface from imars_etl to
an object storage backend.

Hooks should implement methods to align with interface of the
airflow.contrib.hooks.azure_data_lake hook:

# NOTE: methods below are outdated
# TODO: update these
* extract : returns a path the locally-accessible version of the file (and
    downloads the file if needed).
* load    : uploads a file to the backend.

An object storage "ObjectHook" is used to provide an interface from imars_etl to
an object storage backend.

Drivers should implement the following methods:

* extract : returns a path the locally-accessible version of the file (and
    downloads the file if needed).
* load    : uploads a file to the backend.

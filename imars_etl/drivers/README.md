A "driver" is used to provide an interface from imars_etl to a backend.

Drivers should implement the following methods:

* extract : returns a path the locally-accessible version of the file (downloads the file if needed).
* load    : adds a file to the backend.

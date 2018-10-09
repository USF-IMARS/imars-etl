connections = {
    # === object storage connections:
    # NOTE: No connection required for IMaRSObjectsObjectHook because it
    #       assumes the NFS connections are already set up.
    # "imars_etl_azure_lake_conn_id": {}  # TODO: create this?
    # === metadata database connections:
    "imars_metadata_database_default": dict(
        conn_id="imars_metadata_database_default",
        conn_type="mysql",
        host="192.168.1.41",
        login="imars_bot",
        password="***REMOVED***",
        schema="imars_product_metadata",
        port="3306",
        # extra=None,
        # uri=None,
    )
}

from airflow import settings
from airflow.models import Connection
from sqlalchemy.orm import exc

# TODO: mv this to private file
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


def setup_connections():
    """
    Sets up all default connections read in from dict above.
    """
    session = settings.Session()
    for conn_key, conn_kwargs in connections.items():
        if not has_connection(session, conn_kwargs['conn_id']):
            add_connection(session, **conn_kwargs)


def has_connection(session, conn_id):
    try:
        (
            session.query(Connection)
            .filter(Connection.conn_id == conn_id)
            .one()
        )
    except exc.NoResultFound:
        return False
    return True


def add_connection(session, **kwargs):
    """
    conn_id, conn_type, extra, host, login,
    password, port, schema, uri
    """
    session.add(Connection(**kwargs))
    session.commit()

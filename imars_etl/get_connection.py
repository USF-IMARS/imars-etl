from airflow import settings
from airflow.models import Connection

BUILT_IN_CONNECTIONS = {
    "imars_objects": {},  # TODO: these should be Connection-like objects
    "no_backend": {},
}


def get_connection(conn_id):
    """get connection object by id string"""
    if conn_id in BUILT_IN_CONNECTIONS:
        return BUILT_IN_CONNECTIONS[conn_id]

    session = settings.Session()
    return (
        session.query(Connection)
        .filter(Connection.conn_id == conn_id)
        .one()
    )

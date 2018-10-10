from airflow import settings
from airflow.models import Connection

from imars_etl.object_storage.IMaRSObjectsObjectHook \
    import IMaRSObjectsObjectHook
from imars_etl.object_storage.NoBackendObjectHook \
    import NoBackendObjectHook

BUILT_IN_CONNECTIONS = {
    "imars_objects": IMaRSObjectsObjectHook,
    "no_backend": NoBackendObjectHook,
}


def get_hook(conn_id):
    """get connection object by id string"""
    if conn_id in BUILT_IN_CONNECTIONS:
        return BUILT_IN_CONNECTIONS[conn_id]()

    session = settings.Session()
    return (
        session.query(Connection)
        .filter(Connection.conn_id == conn_id)
        .one()
    ).get_hook()

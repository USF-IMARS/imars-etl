import logging
import sys

from airflow import settings
from airflow.models import Connection

from imars_etl.object_storage.IMaRSObjectsObjectHook \
    import IMaRSObjectsObjectHook
from imars_etl.object_storage.NoBackendObjectHook \
    import NoBackendObjectHook

BUILT_IN_CONNECTIONS = {
    "imars_objects": IMaRSObjectsObjectHook,
    "no_backend": NoBackendObjectHook,
    "no_upload": NoBackendObjectHook,
}


def get_hook(conn_id):
    """get connection object by id string"""
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    logger.debug("getting hook for conn_id '{}'".format(conn_id))
    if conn_id in BUILT_IN_CONNECTIONS:
        return BUILT_IN_CONNECTIONS[conn_id]()

    session = settings.Session()
    return (
        session.query(Connection)
        .filter(Connection.conn_id == conn_id)
        .one()
    ).get_hook()


def get_hooks_list():
    """get list of all hooks available"""
    return (
        list(settings.Session().query(Connection))
        .append(BUILT_IN_CONNECTIONS.keys())
    )

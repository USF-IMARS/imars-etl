import logging
import sys

from airflow import settings
from airflow.models import Connection

from imars_etl.object_storage.HookFallbackChain \
    import HookFallbackChain
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
    """
    Get hook object by id string.
    """
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    logger.info("getting hook for conn_id '{}'".format(conn_id))

    # check for built-in
    if conn_id in BUILT_IN_CONNECTIONS:
        return BUILT_IN_CONNECTIONS[conn_id]()
    else:
        logger.debug("not built-in".format(conn_id))

    # check for fallback chain
    if conn_id.startswith("hook_fallback_chain."):
        hook_conn_ids = conn_id.split('.')[1:]
        assert(  # no multi-chain funny business.
            "hook_fallback_chain" not in hook_conn_ids
        )
        hooks = [get_hook(c_id) for c_id in hook_conn_ids]
        return HookFallbackChain(hooks)
    else:
        logger.debug("not a fallback chain")

    # fetch encrypted connection from airflow:
    session = settings.Session()
    result = (
        session.query(Connection)
        .filter(Connection.conn_id == conn_id)
        .one()
    ).get_hook()

    return result


def get_hooks_list():
    """get list of all hooks available"""
    return (
        list(settings.Session().query(Connection))
        .append(BUILT_IN_CONNECTIONS.keys())
    )

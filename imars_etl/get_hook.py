import logging
import sys

from sqlalchemy.orm.exc import NoResultFound
from airflow import settings
from airflow.models import Connection
from airflow.contrib.hooks.fs_hook import FSHook

from imars_etl.object_storage.HookFallbackChain \
    import HookFallbackChain
from imars_etl.object_storage.NoBackendObjectHook \
    import NoBackendObjectHook

DEFAULT_OBJ_STORE_CONN_ID = "imars_objects"
DEFAULT_METADATA_DB_CONN_ID = "imars_metadata"

BUILT_IN_CONNECTIONS = {
    "no_backend": NoBackendObjectHook,
    "no_upload": NoBackendObjectHook,
}


def get_hook(conn_id):
    """
    Get hook object by id string.
    """
    # TODO: separate object_storage & metadata_db hooks
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    logger.info("getting hook for conn_id '{}'".format(conn_id))

    # check for built-in
    if conn_id in BUILT_IN_CONNECTIONS:
        return BUILT_IN_CONNECTIONS[conn_id]()
    else:
        logger.debug("hook not built-in".format(conn_id))

    # check for fallback chain
    if conn_id.startswith("hook_fallback_chain."):
        hook_conn_ids = conn_id.split('.')[1:]
        assert(  # no multi-chain funny business.
            "hook_fallback_chain" not in hook_conn_ids
        )
        logger.debug("Hook is fallback chain. Decending into chain.")
        hooks = []
        for c_id in hook_conn_ids:
            try:
                hooks.append(get_hook(c_id))
            except NoResultFound:
                logger.warn("Chained connection '{}' not found.".format(c_id))
        return HookFallbackChain(hooks)
    else:
        logger.debug("hook not a fallback chain")

    # fetch encrypted connection from airflow:
    session = settings.Session()
    conn = (
        session.query(Connection)
        .filter(Connection.conn_id == conn_id)
        .one()
    )
    logger.debug("conn from airflow: {}".format(conn))
    hook = conn.get_hook()
    if hook is None:
        logger.debug("hook not airflow-official")
        hook = _get_supplemental_hook(conn)
    logger.debug("hook from airflow: {}".format(hook))
    return hook


def get_hooks_list():
    """get list of all hooks available"""
    return (
        list(settings.Session().query(Connection))
        .append(BUILT_IN_CONNECTIONS.keys())
    )


def _get_supplemental_hook(conn):
    """ fills gaps left by airflow.models.Connection.get_hook() """
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    if conn.conn_type == 'fs':
        logger.debug('fs hook')
        return FSHook(conn_id=conn.conn_id)
    else:
        logger.debug('hook not found for conn')
        raise ValueError("cannot get hook for connection {}".format(conn))

import logging
import pprint

from sqlalchemy.orm.exc import NoResultFound
from airflow import settings
from airflow.models import Connection

from airflow.hooks.http_hook import HttpHook
from airflow.contrib.hooks.fs_hook import FSHook

from imars_etl.object_storage.NoBackendObjectHook \
    import NoBackendObjectHook

logging.getLogger("airflow").setLevel(logging.WARNING)
logging.getLogger("airflow").propagate = False


class BaseHookHandler(object):
    def __init__(self, hook_conn_id, wrapper_classes, built_in_hooks={}):
        self.hooks_list = get_hook_list(hook_conn_id)
        self.wrapper_classes = wrapper_classes

    def try_hooks_n_wrappers(
        self,
        method,
        m_args=[],
        m_kwargs={},
    ):
        """
        try_hooks_n_wrappers tries to use each connection hook object in given
        list with each wrapper class in given list to accomplish the given
        method. args & kwargs for the method are passed as m_args & m_kwargs.

        try_hooks_n_wrappers will stop on the first hook that works with any
        wrapper, and will stop on the first wrapper that works with a hook,
        so the order of these lists can matter.

        example:
        --------
        try_hooks_n_wrappers(
            method='get_first',
            hooks=[MySQLHook(), SQLiteHook()],
            wrappers=[DbAPIHook, JSONFileHook],
            m_args=[sql],
            m_kwargs={}
        )

        parameters:
        -----------
        method: string
            method to try using on the wrappers
        hooks : list of airflow hooks
            list of hooks to try using (in order)
        wrappers : list of wrappers to attempt (in order)
            list of wrappers to attempt using on each hook
        m_args : list
            args to pass to method `method(*args)`
        m_kwargs : dict
            kwargs to pass to method `method(**kwargs)`
        """
        logger = logging.getLogger("imars_etl.{}".format(
            __name__,
        ))
        err_msg = ""
        for hook in self.hooks_list:
            err_msg += "\n\thook: {}".format(hook)
            try:  # directly
                return getattr(hook, method)(*m_args, **m_kwargs)
            except Exception as unwr_exc:  # with wrappers
                try:
                    self.handle_exception(unwr_exc, m_args, m_kwargs)
                    return
                except:
                    err_msg = _append_err_msg(
                        err_msg, "unwrapped", hook, unwr_exc
                    )
            for wrapper in self.wrapper_classes:
                try:  # try this wrapper
                    return getattr(wrapper(hook), method)(
                        *m_args, **m_kwargs
                    )
                except Exception as wr_exc:  # wrapper did not work
                    try:
                        self.handle_exception(
                            wr_exc, m_args, m_kwargs
                        )
                        return
                    except:
                        err_msg = _append_err_msg(
                            err_msg, wrapper, hook, wr_exc
                        )
                        logger.error(err_msg)

                        # BR = "\n" + "X"*80
                        # logger.trace(
                        #     "{}( {} ) Trace:".format(wrapper, hook) +
                        #     BR
                        # )
                        # logger.trace(
                        #     traceback.print_stack()
                        # )
                        # logger.trace(BR)
                        continue
        else:
            logger.info("\n\t   hooks:\n{}\n\twrappers:\n{}".format(
                '\n\t\t'.join(self.hooks_list),
                '\n\t\t'.join(self.wrapper_classes),
            ))
            TAB_LEVEL = 2*4  # assumes tab display width is 4
            logger.info("\n\t   m_args:{}\n\tm_kwargs:{}".format(
                pprint.pformat(m_args, indent=TAB_LEVEL),
                pprint.pformat(m_kwargs, indent=TAB_LEVEL),
            ))
            raise RuntimeError(
                "Failed to execute method '{}'. ".format(method) +
                "All possible combinations of Hooks and Hook Wrappers Failed. "
                "At least one of these should have worked. \n"
                " === Attempts:{}".format(err_msg) +
                " \n\nCHECK `m_args` & `m_kwargs` ABOVE FOR FUNNY BUSINESS."
            )

    def handle_exception(self, exc, args_dict):
        """
        Re-raises if exception is not ok.
        Returns None if exception can be safely ignored
        and requested operation is complete or otherwise not needed.

        Override to catch & ignore exceptions like IntegrityError.
        """
        raise


BUILT_IN_CONNECTIONS = {
    "no_backend": NoBackendObjectHook,
    "no_upload": NoBackendObjectHook,
}


def _append_err_msg(err_msg, wrapper, hook, wr_exc):
    err_msg += "\n\t\t{}( {} ):\n\t\t\t{}".format(
        wrapper, hook, wr_exc
    )
    err_msg += (
        "\n\t\t\t" +
        str(wr_exc.__traceback__.tb_frame.f_code)
    )
    return err_msg


def get_hook_list(conn_id):
    logger = logging.getLogger("imars_etl.{}".format(
        __name__,
        )
    )
    logger.debug("getting hook for conn_id '{}'".format(conn_id))

    # check for fallback chain
    if conn_id.startswith("fallback_chain."):
        hook_conn_ids = conn_id.split('.')[1:]
        assert(  # no multi-chain funny business.
            "fallback_chain" not in hook_conn_ids
        )
        logger.debug("Hook is fallback chain. Decending into chain.")
        hooks = []
        for c_id in hook_conn_ids:
            try:
                hooks.append(_get_hook(c_id))
            except NoResultFound:
                logger.debug(
                    "Chained connection '{}' not found.".format(c_id)
                )
        return hooks
    else:
        logger.debug("hook not a fallback chain")

    return [_get_hook(conn_id)]


def _get_hook(conn_id):
    """
    Get hook object by id string.
    """
    # TODO: separate object_storage & metadata_db hooks
    logger = logging.getLogger("imars_etl.{}".format(
        __name__,
        )
    )

    # check for built-in
    if conn_id in BUILT_IN_CONNECTIONS:
        return BUILT_IN_CONNECTIONS[conn_id]()
    else:
        logger.debug("hook not built-in".format(conn_id))

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
    logger = logging.getLogger("imars_etl.{}".format(
        __name__,
        )
    )
    if conn.conn_type == 'fs':
        logger.debug('fs hook')
        return FSHook(conn_id=conn.conn_id)
    elif conn.conn_type == 'http':
        logger.debug('http hook')
        return HttpHook(
            method='GET',  # TODO: this should come from ...somewhere
            http_conn_id=conn.conn_id,
        )
    else:
        logger.debug('hook not found for conn')
        raise ValueError("cannot get hook for connection {}".format(conn))

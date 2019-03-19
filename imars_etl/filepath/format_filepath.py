"""
create a filepath from metadata
"""
import logging


def format_filepath(
    object_storage_hook=None,
    **kwargs
):
    logger = logging.getLogger("imars_etl.{}".format(
        __name__,
        )
    )
    try:
        logger.debug("hook is {}".format(object_storage_hook))
        return object_storage_hook.format_filepath(**kwargs)
    except AttributeError:
        logger.debug('hook has no format_filepath method')
        raise

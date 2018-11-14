"""
create a filepath from metadata
"""
import logging
import sys


def format_filepath(
    hook=None,
    **kwargs
):
    logger = logging.getLogger("imars_etl.{}".format(
        __name__,
        )
    )
    try:
        logger.debug("hook is {}".format(hook))
        return hook.format_filepath(**kwargs)
    except AttributeError:
        logger.debug('hook has no format_filepath method')
        raise

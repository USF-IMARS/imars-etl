"""
create a filepath from metadata
"""
import logging
import sys


def format_filepath(
    hook=None,
    **kwargs
):
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    try:
        logger.debug("hook is {}".format(hook))
        return hook.format_filepath(**kwargs)
    except AttributeError:
        logger.debug('hook has no format_filepath method')
        raise

import logging
import sys


def standardize_time_str(timestr):
    """
        pads timestr if it is a shortened version of ISO_8601_FMT.
        This means that leaving off hours, minutes, seconds, ms assumes that
        they are zero.
    """
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    logger.setLevel(logging.INFO)
    BASEDATE = 'YYYY-MM-DDT00:00:00'
    if len(timestr) < len(BASEDATE):
        logger.debug("partial datetime found")
        return timestr + BASEDATE[len(timestr):]
    elif len(timestr) > len(BASEDATE):
        logger.debug("too long datetime trimmed")
        return timestr[:len(BASEDATE)]
    else:  # lengths ==
        logger.debug("time str is just right.")
        return timestr

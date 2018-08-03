from datetime import datetime
import logging
import sys

ISO_8601_FMT = "%Y-%m-%dT%H:%M:%S.%f"
ISO_8601_SPACEY_FMT = ISO_8601_FMT.replace("T", " ")


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
    timestr = timestr.replace(" ", "T")
    BASEDATE = 'YYYY-MM-DDT00:00:00.000000'
    if len(timestr) < len(BASEDATE):
        logger.debug("partial datetime found")
        return timestr + BASEDATE[len(timestr):]
    elif len(timestr) > len(BASEDATE):
        logger.debug("too long datetime trimmed")
        return timestr[:len(BASEDATE)]
    else:  # lengths ==
        logger.debug("time str is just right.")
        return timestr


def iso8601strptime(dtstr):
    dtstr = standardize_time_str(dtstr)
    dtstr = dtstr.replace(" ", "T")  # in case it is spacey
    return datetime.strptime(dtstr, ISO_8601_FMT)

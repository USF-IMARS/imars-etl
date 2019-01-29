from datetime import datetime
from datetime import timedelta
import logging

ISO_8601_FMT = "%Y-%m-%dT%H:%M:%S.%f"
ISO_8601_SPACEY_FMT = ISO_8601_FMT.replace("T", " ")


def standardize_time_str(timestr):
    """
        pads timestr if it is a shortened version of ISO_8601_FMT.
        This means that leaving off hours, minutes, seconds, ms assumes that
        they are zero.
    """
    logger = logging.getLogger("imars_etl.{}".format(
        __name__,
        )
    )
    timestr = timestr.strip(" \"'")
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


def UTCify(tz_date_str):
    """
    removes timezone-shift suffix in form +00:00

    returns stripped datestring & datetime.timedelta

    eg
    UTCify('')
    """
    logger = logging.getLogger("imars_etl.{}".format(
        __name__,
        )
    )
    tz_date_str = tz_date_str.strip()
    tz_date_str = tz_date_str.strip('\"')
    tz_date_str = tz_date_str.strip('\'')
    if tz_date_str[-3] == ':' and tz_date_str[-6] in ['-', '+']:
        # shift suffix detected
        sign = tz_date_str[-6]
        hrs = int(sign + tz_date_str[-5:-3])
        mins = int(sign + tz_date_str[-2:])
        shift = timedelta(
            hours=hrs,
            minutes=mins,
        )
        tz_date_str = tz_date_str[:-6]
        logger.debug("timeshift detected: {} + {}:{}".format(
            tz_date_str, hrs, mins
        ))
    else:
        # pass through unchanged with 0 time shift
        shift = timedelta(0)
    return tz_date_str, shift


def iso8601strptime(dtstr):
    dtstr, time_shift = UTCify(dtstr)
    dtstr = standardize_time_str(dtstr)
    dtstr = dtstr.replace(" ", "T")  # in case it is spacey
    dt = datetime.strptime(dtstr, ISO_8601_FMT)
    return dt + time_shift

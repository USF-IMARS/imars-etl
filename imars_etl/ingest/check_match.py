from datetime import datetime
import logging
import re
import os

from imars_etl.ingest.filepath_data import valid_pattern_vars

def check_match(filename, pattern):
    """ returns true iff filename matches given filename_pattern """
    logger = logging.getLogger(__name__)
    filename = os.path.basename(filename)  # trim path if given

    # these strings need to be built up so strptime can read them
    strptime_pattern  = pattern.replace("{","").replace("}","")
    strptime_filename = filename  # values replaced by key string below

    # check for potential named args
    for key in valid_pattern_vars:
        if (key in pattern):
            # validate value in filename
            for valid_pattern in valid_pattern_vars[key]:
                if "*" in valid_pattern_vars[key]:
                    # cut out
                    regex = (
                        valid_pattern_vars[key][0]
                        + "[^"+valid_pattern_vars[key][0]+"]+".format(key)
                        + valid_pattern_vars[key][2]
                    )
                    strptime_filename = re.sub(
                        regex,
                        valid_pattern_vars[key][0]
                            + key
                            + valid_pattern_vars[key][2],
                        strptime_filename
                    )

                elif valid_pattern in filename:
                    strptime_filename.replace(valid_pattern, key)
                else:
                    logger.info(
                        "value read from filename for " + key
                        + " is not in list of valid keys: "
                        + str(data.valid_pattern_vars[key])
                    )
                    return False
    # check for valid date in filename that matches pattern
    try:
        datetime.strptime(strptime_filename, strptime_pattern)
    except ValueError as v_err:
        logger.info(v_err)
        return False

    return True

import logging
import shutil
import sys

def extract_file(src_path, target_path, **kwargs):
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    logger.debug(["cp", src_path, target_path])
    shutil.copy(src_path, target_path)
    return target_path

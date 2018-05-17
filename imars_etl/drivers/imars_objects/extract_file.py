import logging
import shutil
import sys
import os

def extract_file(src_path, tmp_dir="/srv/imars-objects/airflow_tmp", **kwargs):
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )

    logger.debug("s_pth: {}".format(src_path))

    filename = os.path.basename(src_path)
    target_path = os.path.join(tmp_dir, filename)

    logger.debug(["cp", src_path, target_path])
    shutil.copy(src_path, target_path)

    return target_path

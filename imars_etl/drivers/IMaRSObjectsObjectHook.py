"""
Uses /srv/imars-objects NFS shares.
Assumes the user has appropriate access to those shares.
"""
import errno
import logging
import os
import shutil
import sys

from imars_etl.drivers.BaseObjectHook import BaseObjectHook
from imars_etl.filepath.format_filepath import format_filepath


class IMaRSObjectsObjectHook(BaseObjectHook):

    def __init__(self, *args, **kwargs):
        super(IMaRSObjectsObjectHook, self).__init__(
            *args, source=None, **kwargs
        )

    def load(self, **kwargs):
        logger = logging.getLogger("{}.{}".format(
            __name__,
            sys._getframe().f_code.co_name)
        )
        # logger.debug('_load(args)| args=\n\t{}'.format(args))
        ul_target = format_filepath(**kwargs)
        logger.debug(["cp", kwargs['filepath'], ul_target])

        if not kwargs.get('dry_run', False):  # don't load if test mode
            try:
                shutil.copy(kwargs['filepath'], ul_target)
            except IOError as i_err:  # possible dir DNE
                # ENOENT(2): file does not exist or missing dest parent dir
                if i_err.errno != errno.ENOENT:
                    raise i_err
                else:
                    os.makedirs(os.path.dirname(ul_target))
                    shutil.copy(kwargs['filepath'], ul_target)

        return ul_target

    def extract(self, src_path, target_path, **kwargs):
        logger = logging.getLogger("{}.{}".format(
            __name__,
            sys._getframe().f_code.co_name)
        )
        logger.debug(["cp", src_path, target_path])
        shutil.copy(src_path, target_path)
        return target_path

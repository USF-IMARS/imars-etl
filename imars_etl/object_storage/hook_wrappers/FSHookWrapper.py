"""
Provides wrapper for airflow.contrib.hooks.FSHook-like object storage
hooks.
"""
import errno
import logging
import os
import shutil

from imars_etl.object_storage.hook_wrappers.BaseHookWrapper \
    import BaseHookWrapper

from imars_etl.filepath.formatter_hardcoded.get_product_filepath_template \
    import get_product_filepath_template \
    as hardcoded_get_product_filepath_template


class FSHookWrapper(BaseHookWrapper):
    REQUIRED_ATTRS = {
        'load': ['get_path'],
        'extract': ['get_path'],
        'format_filepath': ['get_path'],
    }

    def load(self, **kwargs):
        logger = logging.getLogger("imars_etl.{}".format(
            __name__,
            )
        )
        # logger.debug('_load(args)| args=\n\t{}'.format(args))
        ul_target = self.format_filepath(**kwargs)
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
        logger = logging.getLogger("imars_etl.{}".format(
            __name__,
            )
        )
        if not src_path.startswith("/"):
            src_path = self.hook.get_path() + src_path
        logger.debug(["cp", src_path, target_path])
        shutil.copy(src_path, target_path)
        return target_path

    def format_filepath(self, **kwargs):
        logger = logging.getLogger("imars_etl.{}".format(
            __name__,
            )
        )

        product_type_name = kwargs.get("product_type_name")
        product_id = kwargs.get("product_id")
        forced_basename = kwargs.get("forced_basename")

        fullpath = self._get_product_filepath_template(
            product_type_name=product_type_name,
            product_id=product_id,
            forced_basename=forced_basename
        )
        fullpath = self.hook.get_path() + fullpath
        logger.info("formatting FS path \n>>'{}'".format(fullpath))
        args_dict = dict(
            **kwargs
        )
        try:
            date_time = kwargs.get("date_time")
            return date_time.strftime(
                (fullpath).format(**args_dict)
            )
        except KeyError as k_err:
            logger.error(
                "cannot guess an argument required to make path. "
                " pass this argument manually using --json "
            )
            raise k_err

    @staticmethod
    def _get_product_filepath_template(
        product_type_name=None,
        product_id=None,
        forced_basename=None
    ):
        """returns filepath template string for given product type & id"""
        return hardcoded_get_product_filepath_template(
            product_type_name, product_id, forced_basename
        )

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
from imars_etl.filepath.get_ingest_format \
    import get_ingest_formats


class FSHookWrapper(BaseHookWrapper):
    REQUIRED_ATTRS = {
        'load': ['get_path'],
        'extract': ['get_path'],
        'format_filepath': ['get_path'],
    }

    def load(self, filepath, dry_run=False, **kwargs):
        logger = logging.getLogger("imars_etl.{}".format(
            __name__,
            )
        )
        # logger.debug('_load(args)| args=\n\t{}'.format(args))
        ul_target = self.format_filepath(
            filepath=filepath, dry_run=dry_run, **kwargs
        )
        logger.debug(["cp", filepath, ul_target])

        if not dry_run:  # don't load if test mode
            try:
                shutil.copy(filepath, ul_target)
            except IOError as i_err:  # possible dir DNE
                # ENOENT(2): file does not exist or missing dest parent dir
                if i_err.errno != errno.ENOENT:
                    raise i_err
                else:
                    os.makedirs(os.path.dirname(ul_target))
                    shutil.copy(filepath, ul_target)

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

    def format_filepath(
        self,
        metadata_db_handle,
        product_type_name=None, product_id=None, ingest_key=None,
        forced_basename=None, date_time=None,
        **kwargs
    ):
        args_dict = dict(
            product_type_name=product_type_name,
            product_id=product_id,
            ingest_key=ingest_key,
            date_time=date_time,
            metadata_db_handle=metadata_db_handle,
            forced_basename=forced_basename,
            **kwargs
        )
        logger = logging.getLogger("imars_etl.{}".format(
            __name__,
            )
        )

        # TODO:
        format_key, fullpath = get_ingest_formats(
            metadata_db_handle,
            short_name=product_type_name,
            product_id=product_id,
            ingest_name=ingest_key,
            include_test_formats=False,
            first=True
        ).popitem()
        if forced_basename is not None:  # for testing only
            fullpath = os.path.join(forced_basename, fullpath)
        else:
            fullpath = os.path.join(self.hook.get_path(), fullpath)
        # fullpath = get_product_filepath_template(
        #     product_type_name=kwargs.get("product_type_name"),
        #     product_id=kwargs.get("product_id"),
        #     forced_basename=kwargs.get("forced_basename")
        # )
        # fullpath = os.path.join(self.hook.get_path(), fullpath)

        logger.info("formatting FS path \n>>'{}'".format(fullpath))
        try:
            return date_time.strftime(
                (fullpath).format(**args_dict)
            )
        except KeyError as k_err:
            logger.error(
                "cannot guess an argument required to make path. "
                " pass this argument manually using --json "
            )
            raise k_err

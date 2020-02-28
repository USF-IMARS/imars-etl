"""
Provides wrapper for airflow.contrib.hooks.FSHook-like object storage
hooks.

The format_filepath method will use the filepath format with the highest
priority ranking in `imars_product_metadata.product_format.priority`.
"""
import errno
import logging
import os
import shutil

from filepanther.get_filepath_formats \
    import get_filepath_formats
from filepanther.parse_to_fmt_sanitize import parse_to_fmt_sanitize


class imars_objects(object):
    hook_path = "/srv/imars-objects"

    def load(self, filepath, dry_run=False, **kwargs):
        logger = logging.getLogger("imars_etl.{}".format(
            __name__,
        ))
        logger.trace("load()")
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
            src_path = self.hook_path + src_path
        logger.debug(["cp", src_path, target_path])
        shutil.copy(src_path, target_path)
        return target_path

    def format_filepath(
        self,
        metadata_db_handle,
        *args,
        date_time,
        product_type_name=None, product_id=None, ingest_key=None,
        forced_basename=None,
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
        logger.trace('fmt_fpath()')
        # get the format with the highest priority rank
        try:
            format_key, fullpath = get_filepath_formats(
                metadata_db_handle,
                short_name=product_type_name,
                product_id=product_id,
                ingest_name=ingest_key,
                include_test_formats=False,
                first=True
            ).popitem()
        except KeyError:
            raise KeyError(
                "Cannot create filepath for this product. "
                "Metadata DB `product_formats` table contains no entries "
                "for this product. (product_id:{}) ".format(product_id) +
                "Add a product_format row for this product_id and try again."
            )
        if forced_basename is not None:  # for testing only
            logger.trace('forcing basename="{}"'.format(forced_basename))
            fullpath = os.path.join(forced_basename, fullpath)
        else:
            logger.trace('hook path "{}"'.format(self.hook_path))
            fullpath = os.path.join(self.hook_path, fullpath)
        # fullpath = get_product_filepath_template(
        #     product_type_name=kwargs.get("product_type_name"),
        #     product_id=kwargs.get("product_id"),
        #     forced_basename=kwargs.get("forced_basename")
        # )
        # fullpath = os.path.join(self.hook_path, fullpath)

        logger.info("formatting FS path \n>>'{}'".format(fullpath))
        fullpath = parse_to_fmt_sanitize(fullpath)
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

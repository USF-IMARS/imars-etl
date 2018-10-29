"""
Provides wrapper for airflow.contrib.hooks.FSHook-like object storage
hooks.
"""
import errno
import logging
import os
import shutil
import sys

from imars_etl.object_storage.hook_wrappers.BaseHookWrapper \
    import BaseHookWrapper

from imars_etl.filepath.data import data


class FSHookWrapper(BaseHookWrapper):
    REQUIRED_ATTRS = {
        'load': ['get_path'],
        'extract': ['get_path'],
        'format_filepath': ['get_path'],
    }

    def load(self, **kwargs):
        logger = logging.getLogger("{}.{}".format(
            __name__,
            sys._getframe().f_code.co_name)
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
        logger = logging.getLogger("{}.{}".format(
            __name__,
            sys._getframe().f_code.co_name)
        )
        logger.debug(["cp", src_path, target_path])
        shutil.copy(src_path, target_path)
        return target_path

    def format_filepath(self, **kwargs):
        logger = logging.getLogger("{}.{}".format(
            __name__,
            sys._getframe().f_code.co_name)
        )

        product_type_name = kwargs.get("product_type_name")
        product_id = kwargs.get("product_id")
        forced_basename = kwargs.get("forced_basename")

        fullpath = self._format_filepath_template(
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
    def _format_filepath_template(
        product_type_name=None,
        product_id=None,
        forced_basename=None
    ):
        logger = logging.getLogger("{}.{}".format(
            __name__,
            sys._getframe().f_code.co_name)
        )
        logger.setLevel(logging.INFO)
        logger.info("placing {} (#{})...".format(
            product_type_name,
            product_id)
        )
        for prod_name, prod_meta in (
            FSHookWrapper.get_imars_object_paths().items()
        ):
            logger.debug(
                "is {} (#{})?".format(prod_name, prod_meta['product_id'])
            )
            if (
                    product_type_name == prod_name or
                    product_id == prod_meta['product_id']
            ):  # if file type or id is given and matches a known type
                logger.debug('y!')

                if forced_basename is not None:
                    _basename = forced_basename
                else:
                    _basename = prod_meta['basename']

                try:  # set product_type_name if not already set
                    # test = args['product_type_name']
                    # TODO: check for match w/ prod_name & raise if not?
                    pass
                except KeyError:
                    product_type_name = prod_name

                return prod_meta['path']+"/"+_basename
            else:
                logger.debug("no.")
        else:
            # logger.debug(args)
            raise ValueError("could not identify product type")

    @staticmethod
    def get_imars_object_paths():
        """
        Returns a dict of all imars_object paths keyed by product name.

        example:
        {
            "test_test_test":{
                "//": "this is a fake type used for testing only",
                "basename": "simple_file_with_no_args.txt",
                "path"    : "test_test_test",
                "product_id": -1
            },
            "zip_wv2_ftp_ingest":{
                "basename": "wv2_%Y_%m_{tag}.zip",
                "path"    : "{product_type_name}",
                "product_id": 6
            },
            "att_wv2_m1bs":{
                "basename": "WV2_%Y%m%d%H%M%S-M1-{idNumber}_P{passNumber}.att",
                "path": "extra_data/WV02/%Y.%m",
                "product_id": 7
            }
        }
        """
        res = {}
        for product_id in data:
            res[product_id] = data[product_id]["imars_object_format"]
        return res

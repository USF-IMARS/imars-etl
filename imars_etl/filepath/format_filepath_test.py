"""
"""

# std modules:
from unittest import TestCase
try:
    # py2
    from mock import MagicMock
except ImportError:
    # py3
    from unittest.mock import MagicMock
from datetime import datetime

# dependencies:
from imars_etl.filepath.format_filepath import _format_filepath_template, format_filepath

class Test_format_filepath(TestCase):
    def test_format_filepath_p_name(self):
        """" create filepath w/ minimal args (product_name)"""
        args = {
            "datetime": datetime(2015,5,25,15,55),
            "product_type_name": "myd01"
        }
        result = format_filepath(
            args
        )
        self.assertEqual(
            result,
            "/srv/imars-objects/modis_aqua_gom/myd01/A2015145.1555.hdf"
        )
    def test_format_filepath_pid(self):
        """" create filepath w/ minimal args (product_id)"""
        args = {
            "datetime": datetime(2015,5,25,15,55),
            "product_id": 5
        }
        result = format_filepath(
            args
        )
        self.assertEqual(
            result,
            "/srv/imars-objects/modis_aqua_gom/myd01/A2015145.1555.hdf"
        )

class Test_format_filepath_template(TestCase):
    def test_format_filename_template(self):
        """ create basic file format template with 100% knowledge"""
        result = _format_filepath_template(
            "myd01",
            5,
            forced_basename=None
        )
        self.assertEqual(
            result,
            "/srv/imars-objects/modis_aqua_gom/myd01/A%Y%j.%H%M.hdf"
        )

    def test_format_filename_w_product_id(self):
        """ can create file format template with only product_id"""
        result = _format_filepath_template(
            None,
            5,
            forced_basename=None
        )
        self.assertEqual(
            result,
            "/srv/imars-objects/modis_aqua_gom/myd01/A%Y%j.%H%M.hdf"
        )

    def test_format_filename_w_prod_name(self):
        """ can create file format template with only product_type_name"""
        result = _format_filepath_template(
            "myd01",
            None,
            forced_basename=None
        )
        self.assertEqual(
            result,
            "/srv/imars-objects/modis_aqua_gom/myd01/A%Y%j.%H%M.hdf"
        )

    def test_format_filename_w_nothing(self):
        """ file format template with no product info raises err """
        with self.assertRaises(ValueError):
            result = _format_filepath_template(
                None,
                None,
                forced_basename=None
            )

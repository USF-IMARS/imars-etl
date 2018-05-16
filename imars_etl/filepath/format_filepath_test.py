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
from imars_etl.filepath.format_filepath import _format_filepath_template

class Test_format_filepath_template(TestCase):
    def test_format_filename(self):
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

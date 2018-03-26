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

# dependencies:
from imars_etl.filepath import check_match
from imars_etl.filepath.data import get_ingest_format

class Test_check_match(TestCase):

    # tests:
    #########################
    def test_check_zip_wv2_filename(self):
        """ check_match returns true for valid wv2.zip filename """
        self.assertTrue(
            check_match.check_match(
                "wv2_2000_01_RB1.zip",
                get_ingest_format("zip_wv2_ftp_ingest","matts_wv2_ftp_ingest")
            )
        )
    def test_check_zip_wv2_filepath(self):
        """ check_match returns true for valid wv2.zip full path """
        self.assertTrue(
            check_match.check_match(
                "/this/is/a/fake/path/wv2_2000_01_RB1.zip",
                get_ingest_format("zip_wv2_ftp_ingest","matts_wv2_ftp_ingest")
            )
        )
    def test_malformed_zip_wv2_filename(self):
        """ check_match returns false for malformed date wv2.zip filename """
        self.assertFalse(
            check_match.check_match(
                "wv2_20a0_01_RB1.zip",
                get_ingest_format("zip_wv2_ftp_ingest","matts_wv2_ftp_ingest")
            )
        )

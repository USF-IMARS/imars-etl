# std modules:
from unittest import TestCase
try:
    # py2
    from mock import MagicMock
except ImportError:
    # py3
    from unittest.mock import MagicMock

# dependencies:
from satfilename.BaseSatFilepath import BaseSatFilepath

class Test_BaseSatFilepath(TestCase):

    # tests:
    #########################
    def test_match_filename_success(self):
        test_namer = BaseSatFilepath("test", "my_test%Y-%m-%dT%H:%M:%SZ.path")
        self.assertTrue(
            test_namer.match_filename("my_test2017-01-01T12:00:00Z.path")
        )
    def test_match_filename_fail(self):
        """
        same as test_match_filename_success, but with slightly malformed
        filename.
        """
        test_namer = BaseSatFilepath("test", "my_test%Y-%m-%dT%H:%M:%SZ.path")
        self.assertFalse(
            test_namer.match_filename("my_test2017-01-01TABC:00:00Z.path")
        )

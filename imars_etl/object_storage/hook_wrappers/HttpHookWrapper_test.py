"""
"""

# std modules:
from unittest import TestCase
from datetime import datetime
from io import StringIO
try:
    # py2
    from mock import MagicMock
except ImportError:
    # py3
    from unittest.mock import MagicMock

# dependencies:
from imars_etl.object_storage.hook_wrappers.HttpHookWrapper \
    import HttpHookWrapper


class Test_extract(TestCase):
    fake_http_hook = MagicMock(
        run=lambda **kwargs: MagicMock(
            content="fake file content"
        )
    )

    def test_extract_public_data(self):
        """
        http://imars-webserver-01.marine.usf.edu/gom/chlor_a_l3_pass/\
        chlor_a_l3_pass_A2002185191000.hdf
        """
        fake_file_handle = StringIO()
        HttpHookWrapper(self.fake_http_hook).extract(
            'gom/chlor_a_l3_pass/chlor_a_l3_pass_A2002185191000.hdf',
            fake_file_handle
        )
        fake_file_handle.seek(0)
        file_content = fake_file_handle.read()
        self.assertEqual(
            file_content,
            "fake file content"
        )

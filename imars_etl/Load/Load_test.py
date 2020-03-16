
from unittest import TestCase

from imars_etl.Load.Load import _replace_oldpath_w_newpath


class Test_replace_oldpath_w_newpath(TestCase):

    def test__replace_oldpath_w_newpath_basic(self):
        """new path replaces oldpath"""
        rows = _replace_oldpath_w_newpath(
            rows=[[1, "test_fpath", 3]],
            filepath="test_fpath",
            new_filepath="new_test_fpath"
        )
        self.assertTrue("new_test_fpath" in rows[0])
        self.assertFalse("test_fpath" in rows[0])

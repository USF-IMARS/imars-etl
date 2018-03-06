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
from satfilename.satfilename import l1a_lac_hdf_bz2

class Test_satfilename_main(TestCase):

    # tests:
    #########################
    def test_l1a_lac_hdf_bz2(self):
        """ basic test for l1a_lac_hdf_bz2 """

        res = l1a_lac_hdf_bz2(datetime(2000, 1, 2, 3, 45), "fake_test-region01")
        self.assertEqual(
            res,
            "/srv/imars-objects/modis_aqua_fake_test-region01/l1a_lac_hdf_bz2/A2000002034500.L1A_LAC.x.hdf.bz2"
        )

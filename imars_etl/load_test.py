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
from imars_etl.load import load

class Test_load(TestCase):

    # tests:
    #########################
    def test_rclone_cmd_basic(self):
        """
        test cmd:
            imars_etl.py load
                --dry_run
                -f /fake/filepath.png
                -a 1
                -t 4
                -d '2018-02-26T13:00'
                -j '{"status":1}'
        """
        test_args = MagicMock(
            verbose=0,
            dry_run=True,
            filepath="/fake/filepath.png",
            area=1,
            type=4,
            date="2018-02-26T13:00",
            json='{"status":1}'
        )
        self.assertEqual(
            load(test_args),
            'INSERT INTO file'
            + ' (status,date_time,area_id,product_type_id,filepath)'
            + ' VALUES (1,"2018-02-26T13:00",1,4,"/fake/filepath.png")'
        )

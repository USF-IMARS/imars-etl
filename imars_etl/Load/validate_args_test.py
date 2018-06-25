"""
"""

# std modules:
from datetime import datetime
from unittest import TestCase


class Test_validate_args(TestCase):

    def test_validate_returns_dict(self):
        from imars_etl.Load.Load import validate_args
        from imars_etl.cli import parse_args

        test_args = parse_args([
            '-vvv',
            'load',
            '--dry_run',
            '-f', "/fake/path/file_w_date_2018.txt",
            '-p', '-1',
            '-t', "2018-02-26T13:00",
            '-j', '{"status_id":1,"area_id":1}',
            '--nohash'
        ])
        result_arg_dict = validate_args(vars(test_args))
        self.assertDictContainsSubset(
            {
                "verbose": 3,
                "dry_run": True,
                "filepath": "/fake/path/file_w_date_2018.txt",
                "product_id": -1,
                "product_type_name": "test_test_test",
                "time": "2018-02-26T13:00",
                "datetime": datetime(2018, 2, 26, 13),
            },
            result_arg_dict
        )

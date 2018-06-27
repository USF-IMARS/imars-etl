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

    def test_validate_includes_hash(self):
        """
        File hash is auto-added. Assumes ipfs binary filepath exists and...
        has a hard-coded hash. This is likely to break later.

        TODO: mock something to make this work instead of these assumptions.
        """
        from imars_etl.Load.Load import validate_args
        from imars_etl.cli import parse_args

        REAL_FILEPATH = '/usr/local/bin/ipfs'  # hopefully...
        FILE_HASH = 'QmR4XynxbkEbrMZ1upyPaMhkcm9HX1kSYhr2QpvYbxCcSQ'
        test_args = parse_args([
            '-vvv',
            'load',
            '--dry_run',
            '-f', REAL_FILEPATH,
            '-p', '-1',
            '-t', "2018-02-26T13:00",
            '-j', '{"status_id":1,"area_id":1}',
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
                "multihash": FILE_HASH,
            },
            result_arg_dict
        )

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
        print("\n\n{}\n\n".format(vars(test_args)))
        result_arg_dict = validate_args(vars(test_args))
        self.assertDictContainsSubset(
            {
                "verbose": 3,
                "dry_run": True,
                "filepath": "/fake/path/file_w_date_2018.txt",
                "product_id": -1,
                "product_type_name": "test_test_test",
                "time": "2018-02-26T13:00:00.000000",
                "date_time": datetime(2018, 2, 26, 13),
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

        REAL_FILEPATH = '/usr/local/bin/ipfs'  # hopefully...
        FILE_HASH = 'QmR4XynxbkEbrMZ1upyPaMhkcm9HX1kSYhr2QpvYbxCcSQ'
        test_args = {
            'verbose': 3,
            'dry_run': True,
            'filepath':  REAL_FILEPATH,
            'product_id': -1,
            'time': "2018-02-26T13:00",
            'json': '{"status_id":1,"area_id":1}',
            'noparse': True,
        }
        result_arg_dict = validate_args(test_args)
        self.assertDictContainsSubset(
            {
                "verbose": 3,
                "dry_run": True,
                "filepath": "/usr/local/bin/ipfs",
                "product_id": -1,
                "product_type_name": "test_test_test",
                "time": "2018-02-26T13:00:00.000000",
                "date_time": datetime(2018, 2, 26, 13),
                "multihash": FILE_HASH,
            },
            result_arg_dict
        )

    def test_validate_gets_area_id_from_area_name(self):
        """
        area_id can be inferred from area_name
        """
        from imars_etl.Load.Load import validate_args
        from imars_etl.cli import parse_args

        test_args = parse_args([
            '-vvv',
            'load',
            '--dry_run',
            '-f',
            (
                "/srv/imars-objects/ftp-ingest/"
                "wv2_2018_10_08T115750_fl_se_058523212_10_0.zip"
            ),
            '--nohash'
        ])
        result_arg_dict = validate_args(vars(test_args))
        self.assertDictContainsSubset(
            {
                "verbose": 3,
                "dry_run": True,
                "area_short_name": "fl_se",
                "area_id": 7,
            },
            result_arg_dict
        )

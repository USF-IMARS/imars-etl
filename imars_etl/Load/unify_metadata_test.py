# std modules:
from unittest import TestCase


class Test_unify_metadata(TestCase):
    def test_unify_metadata_json_into_args(self):
        """
        unify_metadata sticks stuff from --json into args
        """
        from imars_etl.Load.Load import unify_metadata
        from imars_etl.cli import parse_args

        test_args = parse_args([
            '-vvv',
            'load',
            '--dry_run',
            '-f', "/fake/path/file_w_date_2018.txt",
            '-p', '-1',
            '-t', "2018-02-26T13:00",
            '-j', '{"status_id":1,"area_id":1}',
        ])
        result_arg_dict = unify_metadata(vars(test_args))
        result_arg_dict = unify_metadata(vars(test_args))
        self.assertDictContainsSubset(
            {
                "verbose": 3,
                "dry_run": True,
                "filepath": "/fake/path/file_w_date_2018.txt",
                "product_id": -1,
                "json": '{"status_id":1,"area_id":1}',
                "status_id": 1,
                "area_id": 1
            },
            result_arg_dict
        )

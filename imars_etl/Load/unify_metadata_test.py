# std modules:
from unittest import TestCase


class Test_unify_metadata(TestCase):
    def test_unify_metadata_json_into_args(self):
        """
        unify_metadata sticks stuff from --json into args
        """
        from imars_etl.Load.unify_metadata import unify_metadata
        from imars_etl.cli import parse_args

        test_args = parse_args([
            '-vvv',
            'load',
            '--dry_run',
            '-p', '-1',
            '-t', "2018-02-26T13:00",
            '-j', '{"status_id":1,"area_id":1}',
            "/fake/path/file_w_date_2018.txt",
        ])
        result_arg_dict = unify_metadata(**vars(test_args))
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

    def test_unify_metadata_similar_keys_of_different_types(self):
        """
        unify_metadata passes with matching metadata in kwargs & sql
        """
        from imars_etl.Load.unify_metadata import unify_metadata
        result_arg_dict = unify_metadata(
            verbose=3,
            dry_run=True,
            product_id=-1,
            sql="product_id=-1"
        )
        self.assertDictContainsSubset(
            {
                "verbose": 3,
                "dry_run": True,
                "product_id": -1,
            },
            result_arg_dict
        )

    def test_unify_identical_date_and_datestr(self):
        """
        unify_metadata can use date_time from sql string
        """
        import datetime
        from imars_etl.Load.unify_metadata import unify_metadata
        DT = datetime.datetime(2018, 8, 8, 19, 25)
        TIMESTR = '2018-08-08T19:25:00.000000'
        result_arg_dict = unify_metadata(
            verbose=3,
            dry_run=True,
            product_id=35,
            nohash=True,
            sql=(
                "product_id=35 AND area_id=1 "
                "AND date_time='2018-08-08 19:25:00'"
            ),
            time=TIMESTR,
            filepath='/processing_modis_aqua_pass_gom_20180808T192500_l2_file',
            product_type_name='myd0_otis_l2',
            load_format='{dag_id}_%Y%m%dT%H%M%S_{tag}',
            date_time=DT,
            json='{"status_id":3,"area_short_name":"gom"}'
        )
        self.assertDictContainsSubset(
            {
                "date_time": DT,
                "time": TIMESTR,
            },
            result_arg_dict
        )

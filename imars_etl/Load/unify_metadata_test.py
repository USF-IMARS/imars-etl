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
        expected_subset = {
            "verbose": 3,
            "dry_run": True,
            "filepath": "/fake/path/file_w_date_2018.txt",
            "product_id": -1,
            "json": '{"status_id":1,"area_id":1}',
            "status_id": 1,
            "area_id": 1
        }
        assert expected_subset.items() <= result_arg_dict.items()

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
        expected_subset = {
            "verbose": 3,
            "dry_run": True,
            "product_id": -1,
        }
        assert expected_subset.items() <= result_arg_dict.items()

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
        expected_subset = {
            "date_time": DT,
            "time": TIMESTR,
        }
        assert expected_subset.items() <= result_arg_dict.items()


class Test_sql_str_to_dict(TestCase):
    def test_parse_dt_w_airflow_tz(self):
        """
        sql_str_to_dict converts airflow+timezone dt string to datetime

        Adapted from failing s3_chloro_a task on 2019-01:
        ```python
        DATETIME = "2018-06-22T16:25:25+00:00"
        FNAME = "processing_s3_chloro_a__florida_20180622T162525000000_l2_file"
        args = {
            'filepath': '/srv/imars-objects/airflow_tmp/'+FNAME,
            'json': '{"area_short_name":"florida"}',
            'sql': "product_id=49 AND area_id=12 AND date_time='"+DATETIME+"'"
        }
        imars_etl.load(**args)
        >>> Traceback (most recent call last):
        >>>    ...
        >>>  File "/opt/imars_etl/imars_etl/Load/metadata_constraints.py", l12
        >>>    ('time', ['date_time'], lambda dt: dt.strftime(ISO_8601_FMT)),
        >>> AttributeError: 'str' object has no attribute 'strftime'
        ```
        """
        import datetime
        from imars_etl.Load.unify_metadata import sql_str_to_dict
        DATE_TIME = "2018-06-22T16:25:25+00:00"
        result_arg_dict = sql_str_to_dict(
            "date_time='"+DATE_TIME+"'"
        )

        assert isinstance(result_arg_dict['date_time'], datetime.datetime)

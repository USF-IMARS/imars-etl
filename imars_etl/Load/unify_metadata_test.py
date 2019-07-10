# std modules:
from unittest import TestCase

from imars_etl.Load.unify_metadata import sql_str_to_dict


class Test_unify_metadata(TestCase):
    # def test_unify_metadata_json_into_args(self):
    #     """
    #     unify_metadata sticks stuff from --json into args
    #     """
    #     from imars_etl.Load.unify_metadata import unify_metadata
    #     from imars_etl.cli import parse_args
    #
    #     test_args = parse_args([
    #         '-vvv',
    #         'load',
    #         '--dry_run',
    #         '-p', '-1',
    #         '-t', "2018-02-26T13:00",
    #         '-j', '{"status_id":1,"area_id":1}',
    #         "/fake/path/file_w_date_2018.txt",
    #     ])
    #     result_arg_dict = unify_metadata(**vars(test_args))
    #     expected_subset = {
    #         "verbose": 3,
    #         "dry_run": True,
    #         "filepath": "/fake/path/file_w_date_2018.txt",
    #         "product_id": -1,
    #         "json": '{"status_id":1,"area_id":1}',
    #         "status_id": 1,
    #         "area_id": 1
    #     }
    #     assert expected_subset.items() <= result_arg_dict.items()

    # def test_unify_metadata_similar_keys_of_different_types(self):
    #     """
    #     unify_metadata passes with matching metadata in kwargs & sql
    #     """
    #     from imars_etl.Load.unify_metadata import unify_metadata
    #     result_arg_dict = unify_metadata(
    #         verbose=3,
    #         dry_run=True,
    #         product_id=-1,
    #         sql="product_id=-1"
    #     )
    #     expected_subset = {
    #         "verbose": 3,
    #         "dry_run": True,
    #         "product_id": -1,
    #     }
    #     assert expected_subset.items() <= result_arg_dict.items()

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

    def test_unify_sql_str(self):
        """
        unify_metadata can typecast sql string to datetime
        """
        import datetime
        from imars_etl.Load.unify_metadata import unify_metadata
        result_arg_dict = unify_metadata(
            verbose=0,
            dry_run=True,
            nohash=True,
            sql=(
                'product_id=49 AND area_id=12 AND '
                'date_time="2018-06-22T16:03:16+00:00"'
            ),
            json='{"area_short_name":"florida"}',
            filepath=(
                'mapped.tif'
            ),
            product_id=49,
            load_format='{crap}.tif',
        )
        expected_subset = {
            "date_time": datetime.datetime(2018, 6, 22, 16, 3, 16),
        }
        assert expected_subset.items() <= result_arg_dict.items()


class Test_sql_str_to_dict(TestCase):
    def test_multi_sql_stmt_value_error(self):
        """
        test for unexpected error condition from airflow
        ingest_ftp_na.ingest_s3a_ol_1_efr
        first observed 2019-07-10

        example of error
        ----------------
        File "/opt/imars_etl/imars_etl/Load/unify_metadata.py", line 122,
        in sql_str_to_dict
            key, val = pair.split('=')
        ValueError: too many values to unpack (expected 2)
        """
        SQL = """                         status_id=3 AND
                        area_id=12 AND
                        provenance='af-ftp_v1'                     """
        self.assertEqual(
            {"status_id": 3, "area_id": 12, "provenance": "af-ftp_v1"},
            sql_str_to_dict(SQL)
        )

from unittest import TestCase
from datetime import datetime

from imars_etl.util.timestrings import iso8601strptime


class Test_iso8601strptime(TestCase):
    def test_parse_dt_w_airflow_tz(self):
        """
        can parse (UTC) airflow+timezone dt string to datetime

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
        DATE_TIME = "2018-06-22T16:25:25+00:00"
        assert iso8601strptime(DATE_TIME) == datetime(2018, 6, 22, 16, 25, 25)

    def test_parse_dt_w_airflow_tz_pos(self):
        """
        can parse positive airflow+timezone dt string to datetime
        """
        DATE_TIME = "2018-01-01T01:01:01+12:30"
        assert iso8601strptime(DATE_TIME) == datetime(2018, 1, 1, 13, 31, 1)

    def test_parse_dt_w_airflow_tz_neg(self):
        """
        can parse negative airflow+timezone dt string to datetime
        """
        DATE_TIME = "2018-01-01T13:13:45-01:30"
        assert iso8601strptime(DATE_TIME) == datetime(2018, 1, 1, 13, 12, 15)

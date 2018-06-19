"""
unittest.TestCase with added methods to assert with SQL statements
"""
from unittest import TestCase


def parse_keys_vals_from_sql_insert(sql_str):
    # 'INSERT INTO file (
    # product_id,filepath,date_time) VALUES (
    # 6,"/srv/imars-objects/zip_wv2_ftp_ingest/wv2_2000-06-07T1122_m...")'
    pre, keys, vals = sql_str.split('(')
    keys = keys.split(')')[0].split(',')
    vals = vals.split(')')[0].split(',')
    return keys, vals


class TestCasePlusSQL(TestCase):
    def assertSQLInsertKeyValuesMatch(self, sql_str, keys, vals):
        """
        asserts that keys and values in SQL INSERT statement match the given.
        expected form of the sql_str is:

        'INSERT INTO file (status_id,date_time) VALUES (1,"2018-02-26T13:00")'
        """
        if sql_str is None:
            raise ValueError("sql string is None?")
        elif len(sql_str) < 1:
            raise ValueError("recieved empty sql string?")

        exp_keys, exp_vals = parse_keys_vals_from_sql_insert(sql_str)
        try:  # py3
            self.assertCountEqual(keys, exp_keys)
            self.assertCountEqual(vals, exp_vals)
        except AttributeError:
            # py2
            self.assertItemsEqual(keys, exp_keys)
            self.assertItemsEqual(vals, exp_vals)

    def assertSQLsEquals(self, sql_str_arry, keys_arry, vals_arry):
        """
        asserts that keys and values in array of SQL INSERT statements match
        the given arrays of keys & vals.
        """
        assert(len(sql_str_arry) == len(keys_arry) == len(vals_arry))
        for i, sql in enumerate(sql_str_arry):
            self.assertSQLInsertKeyValuesMatch(
                sql,
                keys_arry[i],
                vals_arry[i]
            )

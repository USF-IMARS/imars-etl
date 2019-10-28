"""
"""

# std modules:
from unittest import TestCase
from unittest.mock import patch

from airflow.exceptions import AirflowException


class Test_get_hook(TestCase):
    # mock patch settings.Session().query() to throw AirflowException before
    #   conn is initialized.
    @patch(
        "airflow.settings.Session",
        side_effect=AirflowException("test db exception")
    )
    def test_get_supplemental_hook(self, mocked_settings_session):
        """
        Gets a supplemental (non-airflow-supported) hook with early db error.

        Reproduces the following error fixed in v0.14.1
        "UnboundLocalError: local variable 'conn' referenced before assignment"
        """
        from imars_etl.BaseHookHandler import _get_hook
        _get_hook(
            "imars_object_store"
        )

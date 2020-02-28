"""
"""
from unittest.mock import patch
from unittest.mock import MagicMock

from imars_etl.util.TestCasePlusSQL import TestCasePlusSQL


class Test_load_cli(TestCasePlusSQL):

    @patch(
        'imars_etl.Load.validate_args._get_handles',
        return_value=(
            MagicMock(),
            MagicMock(
                name='get_records',
                return_value={}
            ),
        )
    )
    def test_load_missing_date_unguessable(self, mock_get_handles):
        """
        CLI cmd missing date that cannot be guessed fails:
            imars_etl.py load
                --dry_run
                -p 6
                -j '{"area_id":1}'
                '/my/path/without/a/date/in.it'
        """
        from imars_etl.cli import main
        test_args = [
            '-vvv',
            'load',
            '--dry_run',
            '-j', '{"area_id":1}',
            '-p', '6',
            '--nohash',
            "/my/path/without/a/date/in.it",
        ]
        self.assertRaises(Exception, main, test_args)  # noqa H202

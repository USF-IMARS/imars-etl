#!/usr/bin/env python3

from argparse import ArgumentParser
import subprocess

import imars_etl

ISO_8601_SPACEY_FMT = "%Y-%m-%d %H:%M:%S.%f"


def main(args):
    for prod_date in imars_etl.select(sql=args.sql, cols='date_time'):
        prod_date = prod_date[0]
        print('-'*100)
        cmd = ([
            "/usr/bin/env", "airflow", "backfill", "--mark_success",
            "-s", prod_date.strftime(ISO_8601_SPACEY_FMT),
            "-e", prod_date.strftime(ISO_8601_SPACEY_FMT),
            args.dag_id
        ])
        print(' '.join(cmd))
        subprocess.call(cmd)


if __name__ == "__main__":
    # =========================================================================
    # === set up arguments
    # =========================================================================
    parser = ArgumentParser(
        description=(
            'Backfill a given airflow DAG with completed DAGRuns with '
            '`execution_date`s matching the `file.date_time` of each product.'
            '\n'
            'This is useful to avoid reprocessing products that already exist.'
        )
    )

    # === arguments for the main command
    parser.add_argument(
        "dag_id", help="ID of the DAG we want to Backfill."
    )
    parser.add_argument(
        "sql", help="SQL to pass through to `imars-etl select`"
    )
    main(parser.parse_args())
else:
    raise AssertionError("CLI must be called as __main__")

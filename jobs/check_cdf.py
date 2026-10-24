"""Proof for novel idea 2: the incrementally maintained feature tables equal a full recompute.

    spark-submit /opt/jobs/check_cdf.py

Every feature table in gold is compared, row by row with exceptAll in both directions and every
column except created, against the same table recomputed now from the full facts. Exit 1 on any
difference.
"""

import sys
import time

from common import lakehouse_session
from dp3_features import BUILDERS


def main() -> None:
    """Compare each stored feature table with a full recompute."""
    spark = lakehouse_session("check_cdf")
    differences = 0
    for table, build in BUILDERS.items():
        started = time.perf_counter()
        stored = spark.table(f"gold.{table}").drop("created")
        full = build(spark).drop("created").cache()
        only_stored, only_full = stored.exceptAll(full).count(), full.exceptAll(stored).count()
        differences += only_stored + only_full
        print(f"{'PASS' if only_stored + only_full == 0 else 'FAIL'}  gold.{table}: {stored.count():,} rows; "
              f"only in the stored table {only_stored}, only in the full recompute {only_full} "
              f"(recompute {time.perf_counter() - started:.1f} s)")
    print(f"{differences} differences")
    spark.stop()
    sys.exit(1 if differences else 0)


if __name__ == "__main__":
    main()

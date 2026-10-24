"""Check the SCD type 2 invariants of gold.dim_courier; exit 1 if any is violated.

    spark-submit /opt/jobs/check_dim_courier.py

Each query counts violations, so every result must be 0.
"""

import sys

from common import lakehouse_session

INVARIANTS = {
    "couriers without exactly one current version":
        "SELECT count(*) FROM (SELECT courier_id FROM gold.dim_courier GROUP BY courier_id HAVING count_if(is_current) <> 1)",
    "versions with valid_from_ts >= valid_to_ts":
        "SELECT count(*) FROM gold.dim_courier WHERE valid_from_ts >= valid_to_ts",
    "current versions not open-ended, or closed versions open-ended":
        "SELECT count_if(is_current AND valid_to_ts <> TIMESTAMP '9999-12-31') "
        "+ count_if(NOT is_current AND valid_to_ts = TIMESTAMP '9999-12-31') FROM gold.dim_courier",
    "gaps or overlaps between consecutive versions":
        "SELECT count(*) FROM (SELECT valid_to_ts, lead(valid_from_ts) OVER (PARTITION BY courier_id ORDER BY valid_from_ts) "
        "AS next_from FROM gold.dim_courier) WHERE next_from IS NOT NULL AND next_from <> valid_to_ts",
    "consecutive versions with identical attributes":
        "SELECT count(*) FROM (SELECT struct(hub_id, vehicle_type, rating, employment_type) AS attrs, "
        "lag(struct(hub_id, vehicle_type, rating, employment_type)) OVER (PARTITION BY courier_id ORDER BY valid_from_ts) "
        "AS prev FROM gold.dim_courier) WHERE prev = attrs",
    "duplicate courier_sk":
        "SELECT count(*) - count(DISTINCT courier_sk) FROM gold.dim_courier",
    "couriers in silver missing from the dimension":
        "SELECT count(DISTINCT courier_id) FROM silver.stg_courier_changes LEFT ANTI JOIN gold.dim_courier USING (courier_id)",
    "current version differs from the courier's last audit row":
        "SELECT count(*) FROM gold.dim_courier d JOIN (SELECT courier_id, max_by(struct(home_hub_id AS hub_id, vehicle_type, "
        "rating, employment_type), changed_ts) AS last FROM silver.stg_courier_changes GROUP BY courier_id) l USING (courier_id) "
        "WHERE d.is_current AND struct(d.hub_id, d.vehicle_type, d.rating, d.employment_type) <> l.last",
}


def main() -> None:
    """Run every invariant query and print its violation count."""
    spark = lakehouse_session("check_dim_courier")
    total = 0
    for name, sql in INVARIANTS.items():
        violations = spark.sql(sql).first()[0]
        total += violations
        print(f"{'PASS' if violations == 0 else 'FAIL'}  {name}: {violations}")
    versions = spark.table("gold.dim_courier").count()
    audit = spark.table("silver.stg_courier_changes").count()
    print(f"{versions:,} versions from {audit:,} audit rows ({audit - versions:,} changed nothing); {total} violations")
    spark.stop()
    sys.exit(1 if total else 0)


if __name__ == "__main__":
    main()

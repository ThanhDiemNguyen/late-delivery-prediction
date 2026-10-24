# Novel idea 1: H3 delivery zones with Apache Sedona

The platform cuts Los Angeles into H3 hexagons and learns how late each hexagon's deliveries run.
Apache Sedona, a spatial extension for Spark that the EDAI course does not teach, computes the
cells inside the Spark jobs. The same library also checks every store point geometrically.

## What H3 and Sedona are

- **H3** is Uber's hierarchical hexagon grid of the whole Earth. Every point falls in exactly one
  cell at each of 16 resolutions; a resolution 7 cell is about 5 km². A cell id is a 64-bit integer,
  so grouping by zone is a cheap integer group-by instead of a polygon join.
- **Apache Sedona** adds spatial types and about 200 `ST_*` SQL functions to Spark: points,
  polygons, `ST_Contains`, areas on the spheroid, and the H3 functions `ST_H3CellIDs` and
  `ST_H3ToGeom`. It runs on the executors like any Spark expression.

Hubs and area types are too coarse for a late-delivery model. One hub serves drops between 1.5
and 21 km away, across very different streets. A hexagon of about 5 km² is fine enough to separate
neighbourhoods and coarse enough to hold hundreds of deliveries a month.

## Where it is in the pipelines

| Step | Job | What Sedona does |
|---|---|---|
| DP2 gold dimension | `jobs/dp2_gold_dims.py` | `dim_zone`: one row per resolution 7 cell that holds a customer's home (the drop point): `h3_index`, centre, area (`ST_AreaSpheroid`), hexagon (`ST_H3ToGeom`, as WKT), main hub |
| DP2 gold fact | `jobs/dp2_gold_facts.py` | `fact_delivery.drop_zone_id = ST_H3CellIDs(ST_Point(drop_lon, drop_lat), 7, true)[0]` |
| DP3 features | `jobs/dp3_features.py` | `feat_zone_daily`: deliveries and late rate over 30 days per zone and day, with `event_timestamp` and `created` (point-in-time, like the other feature tables) |
| Warehouse | `jobs/dp2_warehouse.py` | `dwh.dim_zone`, with a foreign key from `fact_delivery.drop_zone_id` |
| Validation | `jobs/validate.py` | DP2: every delivery's zone is in `dim_zone`. DP3: `feat_zone_daily` keys, rates and time columns |

Sedona 1.9.0 is in the Spark image: `sedona-spark-shaded-3.5_2.12` and `geotools-wrapper` jars,
plus the `apache-sedona` Python package. `jobs/common.py` registers its functions with
`SedonaContext.create`.

## Proof that it worked (`jobs/check_h3.py`, 2026-10-23, dev slice)

```text
$ docker compose exec spark-master /opt/spark/bin/spark-submit /opt/jobs/check_h3.py
[h3] 20,000 drop points: Sedona's cell equals h3-py 4.5.0's for 20,000; 0 mismatches
[geo] bronze orders: 808,000; outside the LA polygon (Sedona) 68,210 = 8.442%; injected (0, 0) or flipped 68,210 = 8.442%; false alarms 0, missed 0
[zones] 1,497 H3 cells (resolution 7), mean area 5.753 km2, 50,000 customers
[zones] highest 30-day late rate on 2024-11-30: 8729a1880ffffff (hub 8) 24.8% of 363 deliveries
[zones] highest 30-day late rate on 2024-11-30: 8729a1d74ffffff (hub 1) 24.1% of 1283 deliveries
[zones] highest 30-day late rate on 2024-11-30: 8729a5615ffffff (hub 7) 23.8% of 370 deliveries
```

1. **The cells are right.** For 20,000 sampled drop points, the cell Sedona wrote into gold equals
   the cell from Uber's reference Python library `h3-py`, with 0 mismatches. A Downtown LA point
   gives 608718350972878847 in both.
2. **The spatial test is exact.** Sedona's `ST_Contains` with the LA service polygon flags 68,210
   bronze store points as outside LA. Those are exactly the rows the generator broke: (0, 0) or a
   sign-flipped latitude, the seed's 8.013% + 0.430% = 8.443%. There are no false alarms and none
   missed. This is an independent check of silver's simpler box-based repair, which replaced the
   same 68,210 points.
3. **The zones work end to end.**
   - `dim_zone` has 1,497 cells with a mean area of 5.75 km². The global mean at resolution 7 is
     5.16 km²; cells at LA's latitude are larger.
   - `feat_zone_daily` has 275,448 rows (1,497 zones × 184 days).
   - The DP2 suite finds every delivery's zone in `dim_zone`, and the DP3 suite passes 31 of 31.
   - The worst zones on 2024-11-30 run 24 to 25% late over 30 days, against 17.6% overall. That
     is the zone signal the model can use.

The job exits 1 if either proof fails, so it can run as a check after DP2.

## Limits

- The cells are computed for drop points only (customers' homes); pings are not mapped to cells.
  At full scale that is 4.9 billion more H3 calls for a feature no model uses yet.
- The 30-day late rate of a sparse zone is noisy. `feat_zone_daily` keeps `deliveries_30d` next to
  the rate, so the model can weight it.
- The LA service polygon is hand-drawn ([data_generator.md](data_generator.md)). The proof uses the
  same polygon the generator used, so it tests Sedona against the injected problems, not the
  polygon against the real coastline.

# Data generator

The generator makes all of LastLeg Logistics' source data on the Mac (host Python, not Docker).
This page grows with each generator step. So far it covers the seed, the delivery-time model, the
reference data and orders in the order system (`oms`), and the courier-app exports in SeaweedFS.

## The seed

The only seed is the Kaggle
[Amazon Delivery Dataset](https://www.kaggle.com/datasets/sujalsuthar/amazon-delivery-dataset)
(MIT license, 43,739 orders in India). It is committed gzipped as `seed/amazon_delivery.csv.gz`,
with its license and checksum in `seed/LICENSE`, so the tests run without a Kaggle account.

The seed is not copied into the platform. It sets only the shapes: how much traffic, weather,
vehicle, area type, distance and courier rating change the delivery time, and later the distance
distribution and the dirty-value rates. Every row in the platform is generated.

**Usable rows: 39,958 of 43,739.** The model drops a row when:
- its store point is (0, 0) or sign-flipped, so its distances are wrong;
- traffic or weather is null;
- the courier rating is null or outside 1 to 5.

Those dirty rows are not thrown away as patterns: their rates are what the generator injects
later.

## Delivery-time model

Re-weighting seed rows to match LA did not work: only about 3,112 effective rows remained, and
Jam traffic came out less late than High. So the generator separates the two questions:

1. **How much does each factor add?** A main-effects model is fitted on the whole seed by least
   squares (`generator/effect_model.py`):

   `minutes = intercept + traffic + weather + vehicle + area + 1.73 × km − 43.4 × rating + residual`

2. **How often does each factor occur?** That comes from the LA mix (below), not from the seed.

Each generated order gets its factor levels from the LA mix and its expected minutes from the
model. A residual is then drawn at random from the seed's own residuals, so the noise has the
seed's real shape, not a bell curve.

Fit on the seed: R² 0.331, residual standard deviation 42.5 min. Effects, in minutes over the
fastest level:

| Factor | Effects |
|---|---|
| Traffic | Low 0, Medium +20.4, High +34.0, Jam +35.9 |
| Weather | Sunny 0, Stormy +17.3, Windy +17.3, Fog +31.6, Cloudy +33.1 (Sandstorms +16.9, not used in LA) |
| Vehicle | van 0, e-bike +0.7, car +14.1 |
| Area | Other 0, Urban +3.9, Metropolitan +15.9, Semi-Urban +92.9 |
| Distance | +1.73 min per km |
| Courier rating | −43.4 min per rating point |

The seed's `motorcycle` and `scooter` are renamed `car` and `e-bike` for LA. The seed spells
Metropolitan as "Metropolitian"; the model uses the correct name. The misspelling comes back later
as a dirty value of the order system.

## LA config

`generator/la_config.py`:

| Factor | LA mix | Reason |
|---|---|---|
| Weather | Sunny 62%, Cloudy 14%, Fog 12%, Windy 7%, Stormy 5% | LA climate: marine-layer fog, Santa Ana winds, no sandstorms. The seed's weather is uniform (about 16.7% each) |
| Traffic | Low 25%, Medium 25%, High 15%, Jam 35% | LA congestion |
| Vehicle | van 45%, car 40%, e-bike 15% | delivery vans plus gig drivers in cars |
| Area type | Metropolitan 55%, Urban 35%, Semi-Urban 7%, Other 3% | dense central LA, the Valley and South Bay, the outer edges |

**SLA rule.** It is set when the customer orders:

`promised minutes = area base + 3 min per km from the hub to the drop point`

A delivery is late when it takes longer than that.

| Area | Base (LA) | Base (seed check only) |
|---|---:|---:|
| Metropolitan | 135 | 140 |
| Urban | 125 | 110 |
| Semi-Urban | 215 | 230 |
| Other | 120 | 110 |

The LA bases were fitted on 400,000 simulated LA orders. For each area, the base is the 82nd
percentile of `minutes − 3 × km` (so 18% are late), rounded up to 5 minutes. With the seed bases,
Urban parcels were late 25.7% of the time and Semi-Urban only 9.1%. With the LA bases, every area
is late about as often, so the label does not mostly measure the area type.

## Checks

`uv run pytest` runs `tests/test_effect_model.py` (and the reference-data checks below), and
`uv run python -m generator.effect_model` prints the numbers below. Late rates come from 400,000
simulated orders (4 million for the per-area row).

| Check | Result |
|---|---|
| Fit | 39,958 rows, R² 0.331, residual sd 42.5 min, effects as in the table above |
| The model reproduces the seed | under the seed mix and seed SLA: 22.1% late, against 22.9% in the seed itself |
| LA late rate | 16.9% overall |
| By area (4M orders) | Metropolitan 17.4%, Other 16.9%, Urban 16.4%, Semi-Urban 15.7% |
| By traffic | Low 6.7% < Medium 14.4% < High 22.3% < Jam 23.7% |
| By weather | Sunny 10.7%, Windy 18.9%, Stormy 19.8%, Fog 29.9%, Cloudy 31.2% |
| By vehicle | van 13.8%, e-bike 14.1%, car 21.5% |
| SLA bases | re-fitting them from simulated orders gives exactly 135 / 125 / 215 / 120 |

## Reference data in `oms`

```bash
uv run python -m generator.oms_reference --scale 0.01            # the 1% dev slice
uv run python -m generator.oms_reference --scale 1.0 --replace   # full scale, replaces what is there
```

The loader applies `generator/sql/oms_schema.sql` (`CREATE TABLE IF NOT EXISTS`), then loads
all four tables with `COPY` in one transaction. If the tables already hold rows, it refuses unless
`--replace` is given. Every timestamp is `timestamptz`, written in UTC. The same scale and seed
always give the same rows; the default seed is 2024.

| Table | Rows (dev slice) | Rows (full) | What it holds |
|---|---:|---:|---|
| `hubs` | 40 | 40 | `generator/la_hubs.py`: name, city, area type, coordinates, capacity tier |
| `couriers` | 200 | 20,000 | current state: home hub, vehicle, rating, employment type, age, `hired_ts`, `updated_ts` |
| `courier_changes` | 1,093 | 104,192 | audit: one full snapshot of the tracked attributes per change |
| `customers` | 50,000 | 5,000,000 | service hub, home point (the drop point), segment, `signup_ts` |

Building takes 0.1 s for the dev slice and 2.3 s at full scale. Loading the dev slice takes
0.4 s.

### Hubs and skew

The 40 hubs are LA neighbourhoods and cities. Each coordinate is the OpenStreetMap (Nominatim)
point for the hub's name, looked up once on 2026-10-23. The hand-written guesses they replaced
were 0.0 to 2.9 km away (median 0.5 km).

Each hub has a weight: its share of couriers, customers and later orders.

- **Skew:** Downtown LA has 25% and the top 10 hubs have 60% (the rubric's skew).
- **Area type:** an order's area type is its hub's area type. So the weights per area type add up
  exactly to the LA mix (Metropolitan 55%, Urban 35%, Semi-Urban 7%, Other 3%), the mix the SLA
  bases were fitted on. A test checks this.

Every hub is inside the validation box (latitude 33.70 to 34.82, longitude −118.95 to −117.65) and
inside the hand-drawn service polygon along the coast.

### Couriers and their change history

At hiring, each courier gets these attributes:

- **home hub:** by hub weight;
- **vehicle:** from the LA mix;
- **employment type:** by vehicle. 85% of van drivers are employees, but only 10% of car drivers
  and 40% of e-bike riders; the rest are contractors;
- **rating and age:** from the seed. Ages are 18 or over.

80% were hired between 2022-01-01 and the period start, and 20% during the period. During the
period these events change a courier:

| Event | How often | Change |
|---|---|---|
| Rating review | Poisson, mean 4.5 for a courier employed all period | rating −0.1 (35%), unchanged (30%), +0.1 (35%), kept within 1.0 to 5.0 |
| Hub transfer | 8% of couriers | a new home hub, by weight |
| Vehicle change | 5% | another vehicle, by the mix |
| Employment change | 3% | employee ↔ contractor |

Each event appends a row to `courier_changes` holding all four tracked attributes after the
change (home hub, vehicle, rating, employment type). It also overwrites the courier's row in
`couriers`.

**No-op rows.** A review that leaves the rating unchanged still writes a row: 294 of 1,093 rows in
the dev slice (26.5% at full scale). The SCD2 `dim_courier` must compare attributes with the
previous row, not create a version per audit row.

For example, courier 146 has 11 rows. It was hired on 2024-03-13 at Downtown LA (hub 1) with a van,
rating 4.6, as an employee. Then came ten rating reviews: 4.7, 4.7 (no-op), 4.6, 4.7, 4.8, 4.8
(no-op), 4.9, 5.0, 5.0 (no-op), 4.9.

### Customers

Each customer is placed by these steps:

1. **Service hub:** chosen by hub weight.
2. **Home point:** a distance from the seed (median 9.2 km), in a random direction from the hub.
3. **Redraw:** points that fall outside the service polygon, in the Pacific, are drawn again.

The home point is the customer's drop point. 15% sign up during the period, and the rest between
2019 and the period start. The segment is consumer (88%) or small business (12%).

## Orders in `oms`

```bash
uv run python -m generator.oms_orders --scale 0.01   # needs the reference data loaded first
```

Orders are generated one local day at a time, each day with its own random seed, and copied into
`oms.orders` straight away. So memory stays at one day's rows even at full scale. The generator
reads the couriers, the courier audit and the customers back from `oms`, so every order points to
rows that really exist.

**Volume.**

- **Normal days:** 400,000 orders a day at full scale.
- **Peak weeks:** 2× in the mid-July sale week (2024-07-15 to 07-21) and 3× in Black Friday week
  (2024-11-25 to 11-30).
- **Total:** 80.8M orders at full scale, 808,000 in the dev slice.

**How one order is made:**

| Field | Rule |
|---|---|
| order time | a local time of day from the seed (most orders 08:00 to 11:59 and 17:00 to 23:59), plus random seconds |
| customer | any customer who signed up before that day |
| hub | the customer's service hub, which gives the skew |
| courier | a courier based at that hub that day, with the vehicle and rating valid that day. A hub with no courier that day borrows one from any hub, which only happens in small slices |
| traffic, weather | the LA mix |
| area | the hub's area type |
| category | the seed's 16 categories |
| distance | haversine km from the hub (the store point) to the customer's home (the drop point) |
| pickup | 5, 10 or 15 minutes after the order, as in the seed |
| delivery | order time plus the effect-model minutes, at least 10 minutes and at least 5 after pickup |
| `promised_ts` | order time plus the SLA (area base + 3 min per km) |

### How the order system stores an order

`orders` copies the seed's legacy layout, so the silver layer has real cleaning to do:

- **Local time as text.**
  - `order_date` is the local LA date.
  - `order_time` and `pickup_time` are local wall-clock text, `HH:MM:SS`.
  - Only `promised_ts`, `delivered_ts` and `updated_ts` are `timestamptz`.
  - On 2024-11-03, when daylight saving time ends, the local hour 01:00 to 01:59 happens twice, so
    that text is ambiguous.
- **Pickup after midnight.** An order at 23:55 picked up 10 minutes later shows `pickup_time`
  00:05:xx with the same `order_date`. This rollover is not injected: it comes from the seed's
  time-of-day pattern.
- **Legacy spellings.** These are copied from the seed:
  - every traffic value has a trailing space (`"Jam "`);
  - every vehicle and area also has one, except `"van"` and `"Other"`;
  - Metropolitan is written `"Metropolitian "`.
- **Seed rates, measured from the seed file at run time:**

| Dirty value | Seed rate | Dev slice in `oms` |
|---|---:|---:|
| Null block: traffic `"NaN "`, weather `"NaN"` and order_time `"NaN "` in the same row (as in the seed) | 0.21% | 0.203% |
| Store coordinates (0, 0) | 8.01% | 8.01% |
| Store latitude sign-flipped (14% of those also flip the longitude) | 0.43% | 0.432% |
| Pickup rollover (`pickup_time < order_time`) | 1.89% | 1.83% |

The delivery time is computed from the clean values. The dirty values only change how the row is
stored, so the label does not depend on them.

### Checks on the dev slice

`tests/test_oms_orders.py` (10 checks on two weeks at 5% scale) and these SQL results on `oms`
after loading the dev slice:

| Check | Result |
|---|---|
| Rows | 808,000 orders, 2024-06-01 to 2024-11-30, loaded in 20 s |
| Late share | 17.56% (the calibration gives about 17%. This slice's 200 couriers include 42.5% car drivers against the 40% mix, and cars add 14 minutes) |
| Skew | Downtown LA 24.84% of orders, the top 10 hubs 59.89% |
| Area mix | Metropolitan 55.08%, Urban 35.23%, Semi-Urban 6.67%, Other 3.02% |
| Black Friday week | 12,000 orders a day, 3× normal |
| Size | `oms` is 238 MB (about 280 B per order with its indexes), so about 23 GB at full scale |

## Courier-app exports in SeaweedFS

```bash
uv run python -m generator.app_exports --scale 0.01 --workers 6   # after the oms load, same scale and seed
```

The courier-app team's daily exports are a second source system: Parquet files in the
`app-exports` bucket, in one `export_date=YYYY-MM-DD` folder per local day. A `_SUCCESS` marker
closes each folder.

**No local files.** Each file is built in memory and uploaded with one PUT, so the Mac's disk never
holds a copy. A single PUT also makes the object's ETag its MD5.

**Matching the database.** The orders are rebuilt in memory from the same seed as the `oms` load,
so every scan and trip matches a row in `oms.orders`. The exporter stops with an error if the
rebuilt order count differs from the one in `oms`.

### GPS pings (`courier_pings/`, schema v1)

| Column | Type | Meaning |
|---|---|---|
| `courier_id` | int32 | the courier |
| `device_seq` | int64 | the phone's ping counter; with `courier_id`, identifies a ping |
| `hub_id` | int16 | the courier's home hub that day (carries the skew: Downtown LA has 25% of couriers) |
| `trip_id` | int64, nullable | the order the courier is delivering; NULL between trips |
| `ping_ts` | timestamp (UTC) | when the phone took the fix |
| `lat`, `lon` | float64 | position, 6 decimals |
| `speed_kmh` | int16 | speed |
| `app_version` | string | `3.14.2` |
| `uploaded_ts` | timestamp (UTC) | when the phone uploaded it; phones upload every 5 minutes |

The rules:

- **Shifts.** Every courier hired before the day works a 10-hour local shift and sends a fix every
  25 s, which is 1,440 pings. Couriers with an even id work 07:00 to 17:00, odd ids 14:00 to 24:00.
- **Trips.** A shift alternates trips (30 minutes on average) and idle gaps, sized so that 30% of
  pings are idle and carry a NULL `trip_id`. Each trip takes the courier's next order of the day,
  in pickup order. That is what the skew benchmark joins on:
  `pings LEFT JOIN orders ON trip_id = order_id`, where the NULL keys pile up.
- **Movement.** While on a trip, speed is the vehicle's mean (van 24, car 28, e-bike 16 km/h) times
  random noise; idle couriers move 0 to 2 km/h. The heading drifts, and a pull toward the hub
  keeps couriers a median 5.8 km away (95% within 11.4 km). GPS noise is about 8 m.
- **Files.** One file per 278 couriers per day (278 × 1,440 = 400,320 rows). Each file has its own
  random seed from (seed, day, file), so files can be built in parallel and in any order.

### Parcel scans (`parcel_scans/`)

Three scans per order:

1. **`at_hub`:** by hub staff, so `courier_id` is NULL. It comes between the order and the pickup.
2. **`picked_up`:** at the pickup time.
3. **`delivered`:** at `delivered_ts`.

Columns: `scan_id`, `order_id`, `courier_id`, `hub_id`, `scan_type`, `scan_ts`, `uploaded_ts`.
Handhelds upload 1 to 120 s after the scan. A scan is exported on the local date of its `scan_ts`.
So a parcel delivered after midnight has its `delivered` scan in the next day's folder. Each file
holds one order day's scans for one export date (`orders-YYYY-MM-DD.parquet`).

### Checks

`tests/test_app_exports.py` checks the ping schema and spacing, the shift hours, the idle share, that
every `trip_id` is one of the courier's own orders, that couriers stay near their hub, the scan
lifecycle order, and byte-identical rebuilds. The dev slice in SeaweedFS:

| Check | Result |
|---|---|
| Pings | 183 files, 48,382,560 rows, 0.84 GB (17.3 B per row) |
| Scans | 366 files, 2,424,000 rows (3 per order) |
| Time | 14 s for the whole period with 6 workers |
| Rerun | all 916 objects have the same ETag (MD5) after a second run |
| Pings against `oms` (2024-08-15) | 2,528 trip ids, all found in `oms.orders`, each with the same courier |
| Scans against `oms` (2024-08-15) | 3,107 `delivered` scans, all with the order's courier and `delivered_ts` |

At 17.3 B per row, full scale (about 4.8 billion pings) comes to about 84 GB of pings. Adding the
scans (about 4 GB) and `oms` (about 23 GB) gives about 111 GB of source data. The one-day dry run
fixes the full-scale numbers.

## Limits

- **Main effects only.** Interactions in the seed (for example weather × traffic) are not kept.
  The seed itself looks generated, so this loses little. The later `check_data` step compares
  the generated late rate per factor with the table above.
- **Rounding moves each area differently.** The bases are rounded up to 5 minutes after fitting,
  which lowers each area's late rate by a different amount (Semi-Urban 15.7%, Metropolitan
  17.4%). The test accepts 15% to 18% per area.
- **Courier history starts at the period.** A courier hired before 2024-06-01 has one `hired` row
  holding their state at the period start; earlier changes are not generated.
- **The service polygon is hand-drawn.** It follows the coast closely enough to keep drops out of
  the ocean, but it includes mountains and parks where nobody lives.
- **Apple Accelerate warnings.** On macOS, numpy's Accelerate BLAS can raise false
  floating-point warnings in the fit's matrix product. The fit ignores those flags and checks
  that every residual is finite instead. Its result matches a plain loop to within 1e-13.

# Data generator

The generator makes all of LastLeg Logistics' source data on the Mac (host Python, not Docker).
This page grows with each generator step. So far it covers the seed and the delivery-time model.

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

`uv run pytest` runs `tests/test_effect_model.py`, and
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

## Limits

- **Main effects only.** Interactions in the seed (for example weather × traffic) are not kept.
  The seed itself looks generated, so this loses little. The later `check_data` step compares
  the generated late rate per factor with the table above.
- **Rounding moves each area differently.** The bases are rounded up to 5 minutes after fitting,
  which lowers each area's late rate by a different amount (Semi-Urban 15.7%, Metropolitan
  17.4%). The test accepts 15% to 18% per area.
- **Apple Accelerate warnings.** On macOS, numpy's Accelerate BLAS can raise false
  floating-point warnings in the fit's matrix product. The fit ignores those flags and checks
  that every residual is finite instead. Its result matches a plain loop to within 1e-13.

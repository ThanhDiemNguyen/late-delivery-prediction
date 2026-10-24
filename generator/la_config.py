"""LA configuration for LastLeg Logistics: how often each factor occurs, and the SLA rule.

The Kaggle seed decides how much each factor changes delivery time (see effect_model.py). This
file decides how often each factor occurs in Los Angeles, and how much time a parcel is promised.
The numbers and their reasons are in docs/data_generator.md.
"""

import pandas as pd

# The generated period: 183 days of local LA time. Timestamps are stored in UTC.
TIME_ZONE = "America/Los_Angeles"
PERIOD_START = pd.Timestamp("2024-06-01", tz=TIME_ZONE).tz_convert("UTC")
PERIOD_END = pd.Timestamp("2024-12-01", tz=TIME_ZONE).tz_convert("UTC")  # exclusive

# Full-scale counts. The 1% development slice uses --scale 0.01.
FULL_COURIERS = 20_000
FULL_CUSTOMERS = 5_000_000
FULL_ORDERS_PER_DAY = 400_000
# Local-date weeks with more orders: a mid-July sale and Black Friday week. (first, last, factor)
PEAK_WEEKS = [("2024-07-15", "2024-07-21", 2.0), ("2024-11-25", "2024-11-30", 3.0)]

TRAFFIC_MIX = {"Low": 0.25, "Medium": 0.25, "High": 0.15, "Jam": 0.35}
WEATHER_MIX = {"Sunny": 0.62, "Cloudy": 0.14, "Fog": 0.12, "Windy": 0.07, "Stormy": 0.05}
VEHICLE_MIX = {"van": 0.45, "car": 0.40, "e-bike": 0.15}
AREA_MIX = {"Metropolitan": 0.55, "Urban": 0.35, "Semi-Urban": 0.07, "Other": 0.03}
LA_MIX = {"traffic": TRAFFIC_MIX, "weather": WEATHER_MIX, "vehicle": VEHICLE_MIX, "area": AREA_MIX}

# SLA rule, set when the customer orders: promised minutes = area base + 3 minutes per km from the
# hub to the drop point. A delivery is late when it takes longer than that.
SLA_MIN_PER_KM = 3.0
# Bases re-fitted on the LA mix so that each area type is late about 17% of the time.
SLA_BASE_MIN = {"Metropolitan": 135, "Urban": 125, "Semi-Urban": 215, "Other": 120}
# The bases calibrated on the seed's own mix. Used only to check the model against the seed.
SEED_SLA_BASE_MIN = {"Metropolitan": 140, "Urban": 110, "Semi-Urban": 230, "Other": 110}
# Share of late orders per area that the LA bases were fitted for (before rounding up to 5 min).
SLA_TARGET_LATE_SHARE = 0.18

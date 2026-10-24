"""LA configuration for LastLeg Logistics: how often each factor occurs, and the SLA rule.

The Kaggle seed decides how much each factor changes delivery time (see effect_model.py). This
file decides how often each factor occurs in Los Angeles, and how much time a parcel is promised.
The numbers and their reasons are in docs/data_generator.md.
"""

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

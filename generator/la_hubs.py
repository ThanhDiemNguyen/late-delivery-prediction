"""LastLeg Logistics' 40 Los Angeles hubs, the service area polygon and the coordinate validation box.

Hub coordinates are the OpenStreetMap (Nominatim) point for each neighbourhood or city name,
looked up on 2026-10-23. The hand-written guesses they replaced were within 2.9 km.

The weights are each hub's share of orders, couriers and customers. They carry the skew the
rubric asks for: Downtown LA has 25% and the top 10 hubs have 60%. An order's area type is its
hub's area type, so the weights per area type add up exactly to la_config.AREA_MIX.
"""

import numpy as np
import pandas as pd

# (hub_id, name, city, area_type, lat, lon, weight, capacity_tier)
HUB_ROWS = [
    (1, "Downtown LA", "Los Angeles", "Metropolitan", 34.0432, -118.2328, 0.25, "mega"),
    (2, "Hollywood", "Los Angeles", "Metropolitan", 34.098, -118.3295, 0.04, "large"),
    (3, "Koreatown", "Los Angeles", "Metropolitan", 34.0618, -118.3054, 0.04, "large"),
    (4, "Santa Monica", "Santa Monica", "Metropolitan", 34.0195, -118.4912, 0.04, "large"),
    (5, "Pasadena", "Pasadena", "Metropolitan", 34.1477, -118.1442, 0.04, "large"),
    (6, "Glendale", "Glendale", "Metropolitan", 34.1469, -118.2478, 0.04, "large"),
    (7, "Long Beach", "Long Beach", "Urban", 33.769, -118.1916, 0.04, "large"),
    (8, "Van Nuys", "Los Angeles", "Urban", 34.1866, -118.4487, 0.04, "large"),
    (9, "Torrance", "Torrance", "Urban", 33.8371, -118.3414, 0.035, "large"),
    (10, "Inglewood", "Inglewood", "Urban", 33.9562, -118.3531, 0.035, "large"),
    (11, "West LA", "Los Angeles", "Metropolitan", 34.0469, -118.4303, 0.012, "standard"),
    (12, "Burbank", "Burbank", "Metropolitan", 34.1812, -118.3072, 0.012, "standard"),
    (13, "Culver City", "Culver City", "Metropolitan", 34.0211, -118.3965, 0.012, "standard"),
    (14, "Silver Lake", "Los Angeles", "Metropolitan", 34.0897, -118.2694, 0.012, "standard"),
    (15, "Beverly Hills", "Beverly Hills", "Metropolitan", 34.0697, -118.3963, 0.012, "standard"),
    (16, "Highland Park", "Los Angeles", "Metropolitan", 34.1099, -118.197, 0.01, "standard"),
    (17, "West Hollywood", "West Hollywood", "Metropolitan", 34.0923, -118.3693, 0.01, "standard"),
    (18, "Boyle Heights", "Los Angeles", "Metropolitan", 34.0437, -118.2098, 0.01, "standard"),
    (19, "Mid-City", "Los Angeles", "Metropolitan", 34.0415, -118.3604, 0.01, "standard"),
    (20, "North Hollywood", "Los Angeles", "Urban", 34.165, -118.3748, 0.016, "standard"),
    (21, "Sherman Oaks", "Los Angeles", "Urban", 34.1509, -118.449, 0.016, "standard"),
    (22, "Hawthorne", "Hawthorne", "Urban", 33.9128, -118.3426, 0.016, "standard"),
    (23, "El Monte", "El Monte", "Urban", 34.0752, -118.0368, 0.016, "standard"),
    (24, "Downey", "Downey", "Urban", 33.9404, -118.1293, 0.016, "standard"),
    (25, "Northridge", "Los Angeles", "Urban", 34.2346, -118.5369, 0.014, "standard"),
    (26, "Redondo Beach", "Redondo Beach", "Urban", 33.8398, -118.3846, 0.014, "standard"),
    (27, "Alhambra", "Alhambra", "Urban", 34.093, -118.1271, 0.014, "standard"),
    (28, "West Covina", "West Covina", "Urban", 34.0686, -117.939, 0.014, "standard"),
    (29, "Compton", "Compton", "Urban", 33.8949, -118.2266, 0.014, "standard"),
    (30, "Canoga Park", "Los Angeles", "Urban", 34.2011, -118.5978, 0.0125, "standard"),
    (31, "Reseda", "Los Angeles", "Urban", 34.2008, -118.5357, 0.0125, "standard"),
    (32, "Pomona", "Pomona", "Urban", 34.0554, -117.7517, 0.0125, "standard"),
    (33, "Whittier", "Whittier", "Urban", 33.9749, -118.0339, 0.0125, "standard"),
    (34, "Santa Clarita", "Santa Clarita", "Semi-Urban", 34.4132, -118.5598, 0.025, "standard"),
    (35, "Lancaster", "Lancaster", "Semi-Urban", 34.6981, -118.1366, 0.02, "standard"),
    (36, "Palmdale", "Palmdale", "Semi-Urban", 34.5793, -118.1171, 0.015, "standard"),
    (37, "Diamond Bar", "Diamond Bar", "Semi-Urban", 34.0286, -117.8103, 0.01, "standard"),
    (38, "Malibu", "Malibu", "Other", 34.0356, -118.6894, 0.01, "standard"),
    (39, "Castaic", "Castaic", "Other", 34.4889, -118.6229, 0.01, "standard"),
    (40, "Acton", "Acton", "Other", 34.4807, -118.1868, 0.01, "standard"),
]
HUB_COLUMNS = ["hub_id", "name", "city", "area_type", "lat", "lon", "weight", "capacity_tier"]

# Approximate LA service area, (lat, lon), drawn by hand along the coast from Leo Carrillo to Seal
# Beach. Customers whose drop point falls outside it (in the Pacific) are placed again.
SERVICE_POLYGON = [
    (34.82, -118.95), (34.82, -117.65), (33.78, -117.65), (33.75, -118.10), (33.762, -118.19),
    (33.71, -118.29), (33.74, -118.41), (33.80, -118.40), (33.842, -118.394), (33.885, -118.41),
    (33.91, -118.425), (33.955, -118.445), (33.985, -118.473), (34.00, -118.495), (34.03, -118.53),
    (34.033, -118.58), (34.03, -118.677), (33.995, -118.806), (34.04, -118.95),
]
# Box that every valid LA coordinate falls in. Sign-flipped longitudes land in Asia and (0, 0) is
# far outside, so the silver layer can detect both.
LAT_RANGE = (33.70, 34.82)
LON_RANGE = (-118.95, -117.65)


def hubs_frame() -> pd.DataFrame:
    """Return the hubs as a DataFrame with the HUB_COLUMNS columns."""
    return pd.DataFrame(HUB_ROWS, columns=HUB_COLUMNS)


def inside_service_area(lat, lon) -> np.ndarray:
    """Return True for each point inside SERVICE_POLYGON (even-odd ray casting)."""
    lat, lon = np.asarray(lat, float), np.asarray(lon, float)
    inside = np.zeros(lat.shape, dtype=bool)
    vertices = SERVICE_POLYGON + SERVICE_POLYGON[:1]
    for (lat1, lon1), (lat2, lon2) in zip(vertices, vertices[1:]):
        crosses = (lat1 > lat) != (lat2 > lat)
        with np.errstate(divide="ignore", invalid="ignore"):
            lon_at_lat = lon1 + (lat - lat1) * (lon2 - lon1) / (lat2 - lat1)
        inside ^= crosses & (lon < lon_at_lat)
    return inside


def inside_validation_box(lat, lon) -> np.ndarray:
    """Return True for each point inside LAT_RANGE and LON_RANGE."""
    lat, lon = np.asarray(lat, float), np.asarray(lon, float)
    return (lat >= LAT_RANGE[0]) & (lat <= LAT_RANGE[1]) & (lon >= LON_RANGE[0]) & (lon <= LON_RANGE[1])

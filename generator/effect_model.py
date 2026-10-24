"""Main-effects model of delivery time, fitted on the Kaggle seed, plus the LA late-rate checks.

    minutes = intercept + traffic + weather + vehicle + area effect
              + minutes per km * distance + minutes per rating point * courier rating
              + a residual drawn from the seed's own residuals

The seed decides how much each factor adds; la_config.py decides how often each factor occurs.
Run `python -m generator.effect_model` to print the fit and the late rates in docs/data_generator.md.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from generator.la_config import (
    LA_MIX,
    SEED_SLA_BASE_MIN,
    SLA_BASE_MIN,
    SLA_MIN_PER_KM,
    SLA_TARGET_LATE_SHARE,
)

SEED_PATH = Path(__file__).resolve().parent.parent / "seed" / "amazon_delivery.csv.gz"
FACTORS = ("traffic", "weather", "vehicle", "area")
# The fastest level of each factor; every effect is measured in minutes over it.
BASELINE = {"traffic": "Low", "weather": "Sunny", "vehicle": "van", "area": "Other"}
# Seed values renamed for LA. "Metropolitian" is the seed's spelling.
SEED_RENAMES = {
    "vehicle": {"motorcycle": "car", "scooter": "e-bike"},
    "area": {"Metropolitian": "Metropolitan"},
}
EARTH_RADIUS_KM = 6371.0


def haversine_km(lat1, lon1, lat2, lon2) -> np.ndarray:
    """Return the great-circle distance in km between two points (arrays of degrees)."""
    lat1, lon1, lat2, lon2 = (np.radians(np.asarray(v, dtype=float)) for v in (lat1, lon1, lat2, lon2))
    a = np.sin((lat2 - lat1) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    return 2 * EARTH_RADIUS_KM * np.arcsin(np.sqrt(a))


def load_seed(path: Path = SEED_PATH) -> pd.DataFrame:
    """Read the seed and keep the rows the model can learn from.

    A row is dropped when its store point is (0, 0) or sign-flipped (outside India), when traffic
    or weather is null, or when the courier rating is null or outside 1 to 5.

    Returns:
        One row per usable order, with columns traffic, weather, vehicle, area (LA names),
        km (store to drop), rating, age (the courier's) and minutes (the seed's Delivery_Time).
    """
    raw = pd.read_csv(path)
    for col in ("Traffic", "Weather", "Vehicle", "Area"):
        raw[col] = raw[col].astype(str).str.strip()
    usable = (
        (raw.Store_Latitude > 5)
        & (raw.Store_Longitude > 60)
        & (raw.Traffic != "NaN")
        & (raw.Weather != "nan")
        & raw.Agent_Rating.between(1, 5)
    )
    raw = raw[usable]
    seed = pd.DataFrame(
        {
            "traffic": raw.Traffic,
            "weather": raw.Weather,
            "vehicle": raw.Vehicle.replace(SEED_RENAMES["vehicle"]),
            "area": raw.Area.replace(SEED_RENAMES["area"]),
            "km": haversine_km(raw.Store_Latitude, raw.Store_Longitude, raw.Drop_Latitude, raw.Drop_Longitude),
            "rating": raw.Agent_Rating.astype(float),
            "age": raw.Agent_Age.astype(int),
            "minutes": raw.Delivery_Time.astype(float),
        }
    )
    return seed.reset_index(drop=True)


def _lookup(values, table: dict, name: str) -> np.ndarray:
    """Map each value through table, and raise ValueError naming any value the table lacks."""
    mapped = pd.Series(np.asarray(values)).map(table)
    if mapped.isna().any():
        unknown = sorted(set(pd.Series(np.asarray(values))[mapped.isna()]))
        raise ValueError(f"unknown {name} value(s): {unknown}")
    return mapped.to_numpy(dtype=float)


@dataclass(frozen=True)
class EffectModel:
    """Fitted main effects of delivery time, in minutes.

    Attributes:
        intercept: minutes for the baseline levels at 0 km and rating 0.
        effects: per factor, minutes each level adds over the baseline level.
        min_per_km: minutes added per km from hub to drop point.
        min_per_rating: minutes added per courier rating point (negative: better couriers are faster).
        residuals: the seed's residuals, drawn from at random to add realistic noise.
        r2: share of the seed's variance the model explains.
    """

    intercept: float
    effects: dict[str, dict[str, float]]
    min_per_km: float
    min_per_rating: float
    residuals: np.ndarray
    r2: float

    def predict(self, traffic, weather, vehicle, area, km, rating) -> np.ndarray:
        """Return the expected delivery minutes for each order, without noise."""
        minutes = self.intercept + self.min_per_km * np.asarray(km, float) + self.min_per_rating * np.asarray(rating, float)
        for factor, values in zip(FACTORS, (traffic, weather, vehicle, area)):
            minutes = minutes + _lookup(values, self.effects[factor], factor)
        return minutes

    def sample_minutes(self, rng: np.random.Generator, traffic, weather, vehicle, area, km, rating) -> np.ndarray:
        """Return delivery minutes for each order: the prediction plus a residual drawn from the seed."""
        expected = self.predict(traffic, weather, vehicle, area, km, rating)
        return expected + rng.choice(self.residuals, size=len(expected))


def fit(seed: pd.DataFrame) -> EffectModel:
    """Fit the main-effects model to the seed by ordinary least squares."""
    levels = pd.get_dummies(seed[list(FACTORS)], prefix_sep="=").astype(float)
    design = levels.drop(columns=[f"{factor}={level}" for factor, level in BASELINE.items()])
    design["km"], design["rating"], design["intercept"] = seed.km, seed.rating, 1.0
    beta, *_ = np.linalg.lstsq(design.to_numpy(), seed.minutes.to_numpy(), rcond=None)
    coef = pd.Series(beta, index=design.columns)
    # Apple's Accelerate BLAS raises spurious floating-point flags in this matmul (the result
    # equals a plain loop to 1e-13), so the flags are ignored and the result is checked instead.
    with np.errstate(all="ignore"):
        residuals = seed.minutes.to_numpy() - design.to_numpy() @ beta
    if not np.isfinite(residuals).all():
        raise ArithmeticError("the seed fit produced non-finite residuals")
    effects = {
        factor: {level: (0.0 if level == BASELINE[factor] else float(coef[f"{factor}={level}"]))
                 for level in sorted(seed[factor].unique())}
        for factor in FACTORS
    }
    return EffectModel(
        intercept=float(coef["intercept"]),
        effects=effects,
        min_per_km=float(coef["km"]),
        min_per_rating=float(coef["rating"]),
        residuals=residuals,
        r2=float(1 - residuals.var() / seed.minutes.var(ddof=0)),
    )


def sla_minutes(area, km, bases: dict[str, float] = SLA_BASE_MIN) -> np.ndarray:
    """Return the promised delivery minutes: the area's base plus SLA_MIN_PER_KM per km."""
    return _lookup(area, bases, "area") + SLA_MIN_PER_KM * np.asarray(km, float)


def simulate_orders(model: EffectModel, seed: pd.DataFrame, mix: dict, n: int, rng: np.random.Generator) -> pd.DataFrame:
    """Draw n orders: factor levels from mix, (km, rating) pairs from seed rows, minutes from the model."""
    orders = pd.DataFrame(
        {factor: rng.choice(list(mix[factor]), size=n, p=list(mix[factor].values())) for factor in FACTORS}
    )
    rows = rng.integers(0, len(seed), n)
    orders["km"] = seed.km.to_numpy()[rows]
    orders["rating"] = seed.rating.to_numpy()[rows]
    orders["minutes"] = model.sample_minutes(rng, *(orders[f] for f in FACTORS), orders.km, orders.rating)
    return orders


def late_shares(orders: pd.DataFrame, bases: dict[str, float]) -> dict[str, float | dict[str, float]]:
    """Return the share of late orders overall and per level of each factor."""
    late = pd.Series(orders.minutes.to_numpy() > sla_minutes(orders.area, orders.km, bases), index=orders.index)
    shares: dict[str, float | dict[str, float]] = {"overall": float(late.mean())}
    for factor in FACTORS:
        shares[factor] = late.groupby(orders[factor]).mean().to_dict()
    return shares


def fit_sla_bases(orders: pd.DataFrame, target: float = SLA_TARGET_LATE_SHARE, step: int = 5) -> dict[str, int]:
    """Return, per area, the base minutes at which `target` of its orders are late, rounded up to `step`."""
    slack = orders.minutes - SLA_MIN_PER_KM * orders.km
    return {
        area: int(np.ceil(np.quantile(slack[orders.area == area], 1 - target) / step) * step)
        for area in sorted(orders.area.unique())
    }


def seed_mix(seed: pd.DataFrame) -> dict[str, dict[str, float]]:
    """Return how often each factor level occurs in the seed."""
    return {factor: seed[factor].value_counts(normalize=True).to_dict() for factor in FACTORS}


def _percent(shares: dict) -> dict:
    """Round a late_shares result to percentages with one decimal, for printing."""
    return {k: round(v * 100, 1) if isinstance(v, float) else _percent(v) for k, v in shares.items()}


def main() -> None:
    """Print the fit, the seed check and the LA late rates (the numbers in docs/data_generator.md)."""
    seed = load_seed()
    model = fit(seed)
    rng = np.random.default_rng(42)
    print(f"seed rows used: {len(seed)}; R^2 {model.r2:.3f}; residual sd {model.residuals.std():.1f} min")
    for factor in FACTORS:
        print(f"  {factor}: " + ", ".join(f"{k} {v:+.1f}" for k, v in sorted(model.effects[factor].items(), key=lambda kv: kv[1])))
    print(f"  per km {model.min_per_km:+.2f} min; per rating point {model.min_per_rating:+.1f} min")
    actual = (seed.minutes > sla_minutes(seed.area, seed.km, SEED_SLA_BASE_MIN)).mean()
    simulated = late_shares(simulate_orders(model, seed, seed_mix(seed), 400_000, rng), SEED_SLA_BASE_MIN)
    print(f"seed mix, seed SLA: model {simulated['overall'] * 100:.1f}% late, seed itself {actual * 100:.1f}%")
    la_orders = simulate_orders(model, seed, LA_MIX, 400_000, rng)
    print(f"LA mix, LA SLA: {_percent(late_shares(la_orders, SLA_BASE_MIN))}")
    print(f"LA SLA bases re-fitted from these orders: {fit_sla_bases(la_orders)}; configured: {SLA_BASE_MIN}")


if __name__ == "__main__":
    main()

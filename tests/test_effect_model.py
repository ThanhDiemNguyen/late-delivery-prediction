"""Checks that the seed effect model and the LA config give the documented delivery-time behaviour."""

import numpy as np
import pandas as pd
import pytest

from generator.effect_model import (
    fit,
    fit_sla_bases,
    late_shares,
    load_seed,
    seed_mix,
    simulate_orders,
    sla_minutes,
)
from generator.la_config import LA_MIX, SEED_SLA_BASE_MIN, SLA_BASE_MIN

N_ORDERS = 400_000


@pytest.fixture(scope="module")
def seed():
    """The usable seed rows."""
    return load_seed()


@pytest.fixture(scope="module")
def model(seed):
    """The model fitted on the seed."""
    return fit(seed)


@pytest.fixture(scope="module")
def la_orders(model, seed):
    """Orders simulated under the LA mix, with a fixed random seed."""
    return simulate_orders(model, seed, LA_MIX, N_ORDERS, np.random.default_rng(1))


def test_seed_rows_and_fit_quality(seed, model):
    """The model learns from 39,958 seed rows and explains a third of the variance."""
    assert len(seed) == 39_958
    assert model.r2 == pytest.approx(0.331, abs=0.001)
    assert model.residuals.std() == pytest.approx(42.5, abs=0.1)


def test_effects_match_the_documented_table(model):
    """Each factor adds the minutes listed in docs/data_generator.md."""
    expected = {
        "traffic": {"Low": 0, "Medium": 20.4, "High": 34.0, "Jam": 35.9},
        "weather": {"Sunny": 0, "Stormy": 17.3, "Windy": 17.3, "Fog": 31.6, "Cloudy": 33.1, "Sandstorms": 16.9},
        "vehicle": {"van": 0, "e-bike": 0.7, "car": 14.1},
        "area": {"Other": 0, "Urban": 3.9, "Metropolitan": 15.9, "Semi-Urban": 92.9},
    }
    for factor, levels in expected.items():
        for level, minutes in levels.items():
            assert model.effects[factor][level] == pytest.approx(minutes, abs=0.05), (factor, level)
    assert model.min_per_km == pytest.approx(1.73, abs=0.005)
    assert model.min_per_rating == pytest.approx(-43.4, abs=0.05)


def test_model_reproduces_the_seed_late_share(model, seed):
    """Under the seed's own mix and SLA, the model is late about as often as the seed (22.9%)."""
    actual = (seed.minutes > sla_minutes(seed.area, seed.km, SEED_SLA_BASE_MIN)).mean()
    orders = simulate_orders(model, seed, seed_mix(seed), N_ORDERS, np.random.default_rng(2))
    simulated = late_shares(orders, SEED_SLA_BASE_MIN)["overall"]
    assert actual == pytest.approx(0.229, abs=0.001)
    assert simulated == pytest.approx(0.222, abs=0.005)


def test_la_mix_is_late_about_17_percent_in_every_area(la_orders):
    """Under the LA mix and LA SLA, about 17% of orders are late, and every area is within 15-18%.

    The bases were fitted for 18% late and then rounded up to 5 minutes, which lowers each area by
    a different amount: on 4 million orders, Semi-Urban is 15.7% and Metropolitan 17.4%.
    """
    shares = late_shares(la_orders, SLA_BASE_MIN)
    assert shares["overall"] == pytest.approx(0.170, abs=0.005)
    for area, share in shares["area"].items():
        assert 0.15 <= share <= 0.18, area


def test_traffic_orders_the_late_share(la_orders):
    """More traffic means more late orders: Low < Medium < High < Jam."""
    by_traffic = late_shares(la_orders, SLA_BASE_MIN)["traffic"]
    assert by_traffic["Low"] < by_traffic["Medium"] < by_traffic["High"] < by_traffic["Jam"]


def test_configured_sla_bases_are_the_refitted_ones(la_orders):
    """Re-fitting the bases on simulated LA orders gives the configured bases."""
    assert fit_sla_bases(la_orders) == SLA_BASE_MIN


def test_unknown_level_is_rejected(model):
    """A level the model has never seen raises instead of silently adding nothing."""
    with pytest.raises(ValueError, match="unknown traffic"):
        model.predict(pd.Series(["Gridlock"]), ["Sunny"], ["van"], ["Urban"], [5.0], [4.5])

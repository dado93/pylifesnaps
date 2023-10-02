import datetime

import numpy as np
import pandas as pd
import pytest

import pylifesnaps.constants
import pylifesnaps.loader


@pytest.fixture(scope="session")
def lifesnaps_loader():
    return pylifesnaps.loader.LifeSnapsLoader()


def test_load_daily_spo2(lifesnaps_loader: pylifesnaps.loader.LifeSnapsLoader):
    user_id = "621e2efa67b776a2409dd1c3"
    start_date = datetime.datetime(2021, 5, 26)
    end_date = datetime.datetime(2021, 6, 11)
    daily_spo2 = lifesnaps_loader.load_daily_spo2(user_id, start_date, end_date)
    assert isinstance(daily_spo2, pd.DataFrame)
    np.testing.assert_array_equal(
        [
            "timestamp",
            "average_value",
            "lower_bound",
            "upper_bound",
        ],
        daily_spo2.columns,
    )


def test_load_computed_temperature(
    lifesnaps_loader: pylifesnaps.loader.LifeSnapsLoader,
):
    user_id = "621e2e8e67b776a24055b564"
    start_date = datetime.datetime(2021, 5, 26)
    end_date = datetime.datetime(2021, 6, 11)
    computed_temperature = lifesnaps_loader.load_computed_temperature(
        user_id, start_date, end_date
    )
    assert isinstance(computed_temperature, pd.DataFrame)
    assert "type" in computed_temperature.columns
    assert "nightly_temperature" in computed_temperature.columns


def test_load_daily_hrv_summary(
    lifesnaps_loader: pylifesnaps.loader.LifeSnapsLoader,
):
    user_id = "621e2e8e67b776a24055b564"
    start_date = datetime.datetime(2021, 5, 26)
    end_date = datetime.datetime(2021, 6, 11)
    daily_hrv_summary = lifesnaps_loader.load_daily_hrv_summary(
        user_id, start_date, end_date
    )
    assert isinstance(daily_hrv_summary, pd.DataFrame)
    assert (
        pylifesnaps.constants._DB_FITBIT_COLLECTION_DAILY_HRV_SUMMARY_TIMESTAMP_KEY
        in daily_hrv_summary.columns
    )
    assert (
        pylifesnaps.constants._DB_FITBIT_COLLECTION_DAILY_HRV_SUMMARY_RMSSD_KEY
        in daily_hrv_summary.columns
    )

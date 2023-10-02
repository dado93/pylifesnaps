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


def test_load_hrv_details(lifesnaps_loader: pylifesnaps.loader.LifeSnapsLoader):
    user_id = "621e2e8e67b776a24055b564"
    start_date = datetime.datetime(2021, 5, 24, 0, 59)
    end_date = datetime.datetime(2021, 5, 24, 2, 1)
    hrv_details = lifesnaps_loader.load_hrv_details(
        user_id=user_id, start_date=start_date, end_date=end_date
    )
    assert isinstance(hrv_details, pd.DataFrame)
    assert pylifesnaps.constants._UNIXTIMESTAMP_IN_MS_COL in hrv_details.columns
    assert pylifesnaps.constants._TIMEZONEOFFSET_IN_MS_COL in hrv_details.columns
    assert pylifesnaps.constants._ISODATE_COL in hrv_details.columns


def test_load_profile(lifesnaps_loader: pylifesnaps.loader.LifeSnapsLoader):
    user_id = "621e2e8e67b776a24055b564"
    profile = lifesnaps_loader.load_profile(user_id=user_id)
    assert isinstance(profile, pd.DataFrame)
    assert (
        profile.iloc[0][pylifesnaps.constants._DB_FITBIT_COLLECTION_PROFILE_GENDER_COL]
        == "MALE"
    )
    assert (
        profile.iloc[0][pylifesnaps.constants._DB_FITBIT_COLLECTION_PROFILE_AGE_COL]
        == "<30"
    )


def test_load_respiratory_rate_summary(
    lifesnaps_loader: pylifesnaps.loader.LifeSnapsLoader,
):
    user_id = "621e2e8e67b776a24055b564"
    start_date = datetime.datetime(2021, 5, 24)
    end_date = datetime.datetime(2021, 5, 26)
    resp_rate_summary = lifesnaps_loader.load_respiratory_rate_summary(
        user_id=user_id, start_date=start_date, end_date=end_date
    )
    assert isinstance(resp_rate_summary, pd.DataFrame)
    assert pylifesnaps.constants._UNIXTIMESTAMP_IN_MS_COL in resp_rate_summary.columns
    assert pylifesnaps.constants._TIMEZONEOFFSET_IN_MS_COL in resp_rate_summary.columns
    assert pylifesnaps.constants._ISODATE_COL in resp_rate_summary.columns
    assert (
        pylifesnaps.constants._DB_FITBIT_COLLECTION_RESP_RATE_SUMMARY_FULL_SLEEP_BREATHING_RATE_COL
        in resp_rate_summary.columns
    )

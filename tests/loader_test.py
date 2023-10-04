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


def test_load_stress_score(
    lifesnaps_loader: pylifesnaps.loader.LifeSnapsLoader,
):
    user_id = "621e2e8e67b776a24055b564"
    start_date = datetime.datetime(2021, 5, 24)
    end_date = datetime.datetime(2021, 5, 26)
    stress_score = lifesnaps_loader.load_stress_score(
        user_id=user_id, start_date=start_date, end_date=end_date
    )
    assert isinstance(stress_score, pd.DataFrame)
    assert pylifesnaps.constants._UNIXTIMESTAMP_IN_MS_COL in stress_score.columns
    assert pylifesnaps.constants._TIMEZONEOFFSET_IN_MS_COL in stress_score.columns
    assert pylifesnaps.constants._ISODATE_COL in stress_score.columns


def test_load_wrist_temperature(
    lifesnaps_loader: pylifesnaps.loader.LifeSnapsLoader,
):
    user_id = "621e2e8e67b776a24055b564"
    start_date = datetime.datetime(2021, 5, 24)
    end_date = datetime.datetime(2021, 5, 26)
    wrist_temperature = lifesnaps_loader.load_wrist_temperature(
        user_id=user_id, start_date=start_date, end_date=end_date
    )
    assert isinstance(wrist_temperature, pd.DataFrame)
    assert pylifesnaps.constants._UNIXTIMESTAMP_IN_MS_COL in wrist_temperature.columns
    assert pylifesnaps.constants._TIMEZONEOFFSET_IN_MS_COL in wrist_temperature.columns
    assert pylifesnaps.constants._ISODATE_COL in wrist_temperature.columns
    assert (
        pylifesnaps.constants._DB_FITBIT_COLLECTION_WRIST_TEMP_TEMP_COL
        in wrist_temperature.columns
    )


@pytest.mark.parametrize(
    "start_date",
    [datetime.datetime(2021, 5, 24), datetime.date(2021, 5, 24), None, "2021/05/24"],
)
@pytest.mark.parametrize(
    "end_date",
    [datetime.datetime(2021, 5, 26), datetime.date(2021, 5, 26), None, "2021/05/26"],
)
def test_load_altitude(
    lifesnaps_loader: pylifesnaps.loader.LifeSnapsLoader, start_date, end_date
):
    user_id = "621e2e8e67b776a24055b564"
    altitude = lifesnaps_loader.load_altitude(
        user_id=user_id, start_date=start_date, end_date=end_date
    )
    assert isinstance(altitude, pd.DataFrame)
    assert pylifesnaps.constants._UNIXTIMESTAMP_IN_MS_COL in altitude.columns
    assert pylifesnaps.constants._TIMEZONEOFFSET_IN_MS_COL in altitude.columns
    assert pylifesnaps.constants._ISODATE_COL in altitude.columns
    assert (
        pylifesnaps.constants._DB_FITBIT_COLLECTION_ALTITUDE_ALTITUDE_COL
        in altitude.columns
    )


@pytest.mark.parametrize(
    "start_date",
    [datetime.datetime(2021, 5, 24), datetime.date(2021, 5, 24), None, "2021/05/24"],
)
@pytest.mark.parametrize(
    "end_date",
    [datetime.datetime(2021, 11, 30), datetime.date(2021, 11, 30), None, "2021/11/30"],
)
def test_load_badge(
    lifesnaps_loader: pylifesnaps.loader.LifeSnapsLoader, start_date, end_date
):
    user_id = "621e2e8e67b776a24055b564"
    badge = lifesnaps_loader.load_badge(
        user_id=user_id, start_date=start_date, end_date=end_date
    )
    assert isinstance(badge, pd.DataFrame)
    assert pylifesnaps.constants._UNIXTIMESTAMP_IN_MS_COL in badge.columns
    assert pylifesnaps.constants._TIMEZONEOFFSET_IN_MS_COL in badge.columns
    assert pylifesnaps.constants._ISODATE_COL in badge.columns
    assert pylifesnaps.constants._DB_FITBIT_COLLECTION_BADGE_TYPE_COL in badge.columns


@pytest.mark.parametrize(
    "start_date",
    [datetime.datetime(2021, 5, 24), datetime.date(2021, 5, 24), None, "2021/05/24"],
)
@pytest.mark.parametrize(
    "end_date",
    [datetime.datetime(2021, 5, 30), datetime.date(2021, 5, 30), None, "2021/05/30"],
)
def test_load_calories(
    lifesnaps_loader: pylifesnaps.loader.LifeSnapsLoader, start_date, end_date
):
    user_id = "621e2e8e67b776a24055b564"
    calories = lifesnaps_loader.load_calories(
        user_id=user_id, start_date=start_date, end_date=end_date
    )
    assert isinstance(calories, pd.DataFrame)
    assert pylifesnaps.constants._UNIXTIMESTAMP_IN_MS_COL in calories.columns
    assert pylifesnaps.constants._TIMEZONEOFFSET_IN_MS_COL in calories.columns
    assert pylifesnaps.constants._ISODATE_COL in calories.columns
    assert (
        pylifesnaps.constants._DB_FITBIT_COLLECTION_CALORIES_VALUE_COL
        in calories.columns
    )


@pytest.mark.parametrize(
    "start_date",
    [datetime.datetime(2021, 5, 24), datetime.date(2021, 5, 24), None, "2021/05/24"],
)
@pytest.mark.parametrize(
    "end_date",
    [datetime.datetime(2021, 5, 30), datetime.date(2021, 5, 30), None, "2021/05/30"],
)
def test_load_distance(
    lifesnaps_loader: pylifesnaps.loader.LifeSnapsLoader, start_date, end_date
):
    user_id = "621e2e8e67b776a24055b564"
    distance = lifesnaps_loader.load_distance(
        user_id=user_id, start_date=start_date, end_date=end_date
    )
    assert isinstance(distance, pd.DataFrame)
    assert pylifesnaps.constants._UNIXTIMESTAMP_IN_MS_COL in distance.columns
    assert pylifesnaps.constants._TIMEZONEOFFSET_IN_MS_COL in distance.columns
    assert pylifesnaps.constants._ISODATE_COL in distance.columns
    assert (
        pylifesnaps.constants._DB_FITBIT_COLLECTION_CALORIES_VALUE_COL
        in distance.columns
    )

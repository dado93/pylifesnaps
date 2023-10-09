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
    assert {
        pylifesnaps.constants._UNIXTIMESTAMP_IN_MS_COL,
        "average_value",
        "lower_bound",
        "upper_bound",
    }.issubset(daily_spo2.columns)


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
    assert pylifesnaps.constants._UNIXTIMESTAMP_IN_MS_COL in daily_hrv_summary.columns
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


@pytest.mark.parametrize(
    "start_date",
    [datetime.datetime(2021, 5, 24), datetime.date(2021, 5, 24), None, "2021/05/24"],
)
@pytest.mark.parametrize(
    "end_date",
    [datetime.datetime(2021, 5, 30), datetime.date(2021, 5, 30), None, "2021/05/30"],
)
def test_load_estimated_oxygen_variation(
    lifesnaps_loader: pylifesnaps.loader.LifeSnapsLoader, start_date, end_date
):
    user_id = "621e2e8e67b776a24055b564"
    distance = lifesnaps_loader.load_estimated_oxygen_variation(
        user_id=user_id, start_date=start_date, end_date=end_date
    )
    assert isinstance(distance, pd.DataFrame)
    assert pylifesnaps.constants._UNIXTIMESTAMP_IN_MS_COL in distance.columns
    assert pylifesnaps.constants._TIMEZONEOFFSET_IN_MS_COL in distance.columns
    assert pylifesnaps.constants._ISODATE_COL in distance.columns
    assert (
        pylifesnaps.constants._DB_FITBIT_COLLECTION_EST_OXY_VAR_VALUE_COL
        in distance.columns
    )


def test_load_heart_rate(
    lifesnaps_loader: pylifesnaps.loader.LifeSnapsLoader, start_date, end_date
):
    user_id = "621e2e8e67b776a24055b564"
    start_date = datetime.datetime(2021, 5, 24)
    end_date = datetime.datetime(2021, 5, 25)
    heart_rate = lifesnaps_loader.load_heart_rate(
        user_id=user_id, start_date=start_date, end_date=end_date
    )
    assert isinstance(heart_rate, pd.DataFrame)
    assert pylifesnaps.constants._UNIXTIMESTAMP_IN_MS_COL in heart_rate.columns
    assert pylifesnaps.constants._TIMEZONEOFFSET_IN_MS_COL in heart_rate.columns
    assert pylifesnaps.constants._ISODATE_COL in heart_rate.columns
    assert (
        pylifesnaps.constants._DB_FITBIT_COLLECTION_HEART_RATE_VALUE_BPM_COL
        in heart_rate.columns
    )


def test_load_journal_entries(
    lifesnaps_loader: pylifesnaps.loader.LifeSnapsLoader,
):
    user_id = "621e2eaf67b776a2406b14ac"
    start_date = datetime.datetime(2021, 11, 24)
    end_date = datetime.datetime(2021, 11, 26)
    journal_entries = lifesnaps_loader.load_journal_entries(
        user_id=user_id, start_date=start_date, end_date=end_date
    )
    assert isinstance(journal_entries, pd.DataFrame)
    assert pylifesnaps.constants._UNIXTIMESTAMP_IN_MS_COL in journal_entries.columns
    assert pylifesnaps.constants._TIMEZONEOFFSET_IN_MS_COL in journal_entries.columns
    assert pylifesnaps.constants._ISODATE_COL in journal_entries.columns
    assert (
        pylifesnaps.constants._DB_FITBIT_COLLECTION_JOURNAL_ENTRIES_LOG_TYPE_COL
        in journal_entries.columns
    )


def test_load_lightly_active_minutes(
    lifesnaps_loader: pylifesnaps.loader.LifeSnapsLoader,
):
    user_id = "621e2eaf67b776a2406b14ac"
    start_date = datetime.datetime(2021, 11, 1)
    end_date = datetime.datetime(2021, 12, 1)
    lightly_active_min = lifesnaps_loader.load_lightly_active_minutes(
        user_id=user_id, start_date=start_date, end_date=end_date
    )
    assert isinstance(lightly_active_min, pd.DataFrame)
    assert pylifesnaps.constants._UNIXTIMESTAMP_IN_MS_COL in lightly_active_min.columns
    assert pylifesnaps.constants._TIMEZONEOFFSET_IN_MS_COL in lightly_active_min.columns
    assert pylifesnaps.constants._ISODATE_COL in lightly_active_min.columns
    assert (
        pylifesnaps.constants._DB_FITBIT_COLLECTION_LIGHTLY_ACTIVE_MIN_VALUE_COL
        in lightly_active_min.columns
    )


def test_load_moderately_active_minutes(
    lifesnaps_loader: pylifesnaps.loader.LifeSnapsLoader,
):
    user_id = "621e2eaf67b776a2406b14ac"
    start_date = datetime.datetime(2021, 11, 1)
    end_date = datetime.datetime(2021, 12, 1)
    moderately_active_min = lifesnaps_loader.load_moderately_active_minutes(
        user_id=user_id, start_date=start_date, end_date=end_date
    )
    assert isinstance(moderately_active_min, pd.DataFrame)
    assert (
        pylifesnaps.constants._UNIXTIMESTAMP_IN_MS_COL in moderately_active_min.columns
    )
    assert (
        pylifesnaps.constants._TIMEZONEOFFSET_IN_MS_COL in moderately_active_min.columns
    )
    assert pylifesnaps.constants._ISODATE_COL in moderately_active_min.columns
    assert (
        pylifesnaps.constants._DB_FITBIT_COLLECTION_MODERATELY_ACTIVE_MIN_VALUE_COL
        in moderately_active_min.columns
    )


def test_load_very_active_minutes(
    lifesnaps_loader: pylifesnaps.loader.LifeSnapsLoader,
):
    user_id = "621e2eaf67b776a2406b14ac"
    start_date = datetime.datetime(2021, 11, 1)
    end_date = datetime.datetime(2021, 12, 1)
    very_active_min = lifesnaps_loader.load_very_active_minutes(
        user_id=user_id, start_date=start_date, end_date=end_date
    )
    assert isinstance(very_active_min, pd.DataFrame)
    assert pylifesnaps.constants._UNIXTIMESTAMP_IN_MS_COL in very_active_min.columns
    assert pylifesnaps.constants._TIMEZONEOFFSET_IN_MS_COL in very_active_min.columns
    assert pylifesnaps.constants._ISODATE_COL in very_active_min.columns
    assert (
        pylifesnaps.constants._DB_FITBIT_COLLECTION_VERY_ACTIVE_MIN_VALUE_COL
        in very_active_min.columns
    )


def test_load_sedentary_minutes(
    lifesnaps_loader: pylifesnaps.loader.LifeSnapsLoader,
):
    user_id = "621e2eaf67b776a2406b14ac"
    start_date = datetime.datetime(2021, 11, 1)
    end_date = datetime.datetime(2021, 12, 1)
    sedentary_min = lifesnaps_loader.load_sedentary_minutes(
        user_id=user_id, start_date=start_date, end_date=end_date
    )
    assert isinstance(sedentary_min, pd.DataFrame)
    assert pylifesnaps.constants._UNIXTIMESTAMP_IN_MS_COL in sedentary_min.columns
    assert pylifesnaps.constants._TIMEZONEOFFSET_IN_MS_COL in sedentary_min.columns
    assert pylifesnaps.constants._ISODATE_COL in sedentary_min.columns
    assert (
        pylifesnaps.constants._DB_FITBIT_COLLECTION_VERY_ACTIVE_MIN_VALUE_COL
        in sedentary_min.columns
    )


def test_steps(
    lifesnaps_loader: pylifesnaps.loader.LifeSnapsLoader,
):
    user_id = "621e2eaf67b776a2406b14ac"
    start_date = datetime.datetime(2021, 11, 1)
    end_date = datetime.datetime(2021, 11, 10)
    steps = lifesnaps_loader.load_steps(
        user_id=user_id, start_date=start_date, end_date=end_date
    )
    assert isinstance(steps, pd.DataFrame)
    assert pylifesnaps.constants._UNIXTIMESTAMP_IN_MS_COL in steps.columns
    assert pylifesnaps.constants._TIMEZONEOFFSET_IN_MS_COL in steps.columns
    assert pylifesnaps.constants._ISODATE_COL in steps.columns
    assert pylifesnaps.constants._DB_FITBIT_COLLECTION_STEPS_VALUE_COL in steps.columns

_METRIC_SPO2 = "daily-spo2"
_METRIC_COMP_TEMP = "computed-temprature"
_METRIC_DEVICE_TEMP = "device-temperature"
_METRIC_DAILY_HRV_SUMMARY = "daily-hrv-summary"
_METRIC_HRV_DETAILS = "hrv-details"
_METRIC_PROFILE = "profile"
_METRIC_RESPIRATORY_RATE_SUMMARY = "respiratory-rate-summary"


_UNIXTIMESTAMP_IN_MS_COL = "unixTimestampInMs"
_ISODATE_COL = "isoDate"
_TIMEZONEOFFSET_IN_MS_COL = "timezoneOffsetInMs"

##################################
#           Database             #
##################################
_DB_NAME = "rais_anonymized"

##################################
#       FitBit Collection        #
##################################
_DB_FITBIT_COLLECTION_NAME = "fitbit"
_DB_FITBIT_COLLECTION_TYPE_KEY = "type"
_DB_FITBIT_COLLECTION_ID_KEY = "id"
_DB_FITBIT_COLLECTION_DATA_KEY = "data"

# --------------------------------#
#          Document types         #
# --------------------------------#
_DB_FITBIT_COLLECTION_DATA_TYPE_SLEEP_VALUE = "sleep"
_DB_FITBIT_COLLECTION_DATA_TYPE_COMP_TEMP_VALUE = "Computed Temperature"
_DB_FITBIT_COLLECTION_DATA_TYPE_DAILY_HRV_SUMMARY_VALUE = (
    "Daily Heart Rate Variability Summary"
)
_DB_FITBIT_COLLECTION_DATA_TYPE_DAILY_SPO2_VALUE = "Daily SpO2"
_DB_FITBIT_COLLECTION_DATA_TYPE_DEVICE_TEMP_VALUE = "Device Temperature"
_DB_FITBIT_COLLECTION_DATA_TYPE_AFIB_ECG_READINGS_VALUE = "Afib ECG Readings"
_DB_FITBIT_COLLECTION_DATA_TYPE_DAILY_HRV_SUMMARY_VALUE = (
    "Daily Heart Rate Variability Summary"
)
_DB_FITBIT_COLLECTION_DATA_TYPE_HRV_DETAILS_VALUE = "Heart Rate Variability Details"
_DB_FITBIT_COLLECTION_DATA_TYPE_HRV_HISTOGRAM_VALUE = "Heart Rate Variability Histogram"
_DB_FITBIT_COLLECTION_DATA_TYPE_PROFILE_VALUE = "Profile"
_DB_FITBIT_COLLECTION_DATA_TYPE_RESPIRATORY_RATE_SUMMARY_VALUE = (
    "Respiratory Rate Summary"
)
_DB_FITBIT_COLLECTION_DATA_TYPE_STRESS_SCORE_VALUE = "Stress Score"
_DB_FITBIT_COLLECTION_DATA_TYPE_WRIST_TEMPERATURE_VALUE = "Wrist Temperature"

"""
'altitude', 'badge', 'calories', 'demographic_vo2_max', 'distance', 'estimated_oxygen_variation', 
'exercise', 'heart_rate', 'journal_entries', 'lightly_active_minutes', 'mindfulness_eda_data_sessions', 'mindfulness_goals', 
'mindfulness_sessions', 'moderately_active_minutes', 'resting_heart_rate', 'sedentary_minutes', 'sleep', 'steps',
'time_in_heart_rate_zones', 'very_active_minutes', 'water_logs']
"""


# --------------------------------#
#         Sleep Documents         #
# --------------------------------#
_DB_FITBIT_COLLECTION_SLEEP_DATA_LOG_ID_KEY = "logId"
_DB_FITBIT_COLLECTION_SLEEP_DATA_DATE_OF_SLEEP_KEY = "dateOfSleep"
_DB_FITBIT_COLLECTION_SLEEP_DATA_START_TIME_KEY = "startTime"
_DB_FITBIT_COLLECTION_SLEEP_DATA_END_TIME_KEY = "endTime"
_DB_FITBIT_COLLECTION_SLEEP_DATA_DURATION_KEY = "duration"
_DB_FITBIT_COLLECTION_SLEEP_DATA_MIN_TO_FALL_ASLEEP_KEY = "minutesToFallAsleep"
_DB_FITBIT_COLLECTION_SLEEP_DATA_MIN_ASLEEP_KEY = "minutesAsleep"
_DB_FITBIT_COLLECTION_SLEEP_DATA_MIN_AWAKE_KEY = "minutesAwake"
_DB_FITBIT_COLLECTION_SLEEP_DATA_MIN_AFTER_WAKEUP_KEY = "minutesAfterWakeup"
_DB_FITBIT_COLLECTION_SLEEP_DATA_TIME_IN_BED_KEY = "timeInBed"
_DB_FITBIT_COLLECTION_SLEEP_DATA_EFFICIENCY_KEY = "efficiency"
_DB_FITBIT_COLLECTION_SLEEP_DATA_MAIN_SLEEP_KEY = "mainSleep"
_DB_FITBIT_COLLECTION_SLEEP_DATA_LEVELS_KEY = "levels"
_DB_FITBIT_COLLECTION_SLEEP_DATA_LEVELS_SUMMARY_KEY = "summary"
_DB_FITBIT_COLLECTION_SLEEP_DATA_LEVELS_SUMMARY_DEEP_KEY = "deep"
_DB_FITBIT_COLLECTION_SLEEP_DATA_LEVELS_SUMMARY_LIGHT_KEY = "light"
_DB_FITBIT_COLLECTION_SLEEP_DATA_LEVELS_SUMMARY_REM_KEY = "rem"
_DB_FITBIT_COLLECTION_SLEEP_DATA_LEVELS_SUMMARY_WAKE_KEY = "wake"
_DB_FITBIT_COLLECTION_SLEEP_DATA_LEVELS_DATA_KEY = "data"
_DB_FITBIT_COLLECTION_SLEEP_DATA_LEVELS_DATA_DATETIME_KEY = "dateTime"
_DB_FITBIT_COLLECTION_SLEEP_DATA_LEVELS_DATA_SECONDS_KEY = "seconds"
_DB_FITBIT_COLLECTION_SLEEP_DATA_LEVELS_DATA_LEVEL_KEY = "level"
_DB_FITBIT_COLLECTION_SLEEP_DATA_LEVELS_SHORT_DATA_KEY = "shortData"
_DB_FITBIT_COLLECTION_SLEEP_DATA_TYPE_KEY = "type"
_DB_FITBIT_COLLECTION_SLEEP_DATA_INFO_CODE_KEY = "infoCode"
_DB_FITBIT_COLLECTION_SLEEP_DATA_DEEP_COUNT_KEY = "deepCount"
_DB_FITBIT_COLLECTION_SLEEP_DATA_DEEP_MIN_KEY = "deepMinutes"
_DB_FITBIT_COLLECTION_SLEEP_DATA_DEEP_30_DAYS_AVG_MIN_KEY = "deepThirtydayavgminutes"
_DB_FITBIT_COLLECTION_SLEEP_DATA_WAKE_COUNT_KEY = "wakeCount"
_DB_FITBIT_COLLECTION_SLEEP_DATA_WAKE_MIN_KEY = "wakeMinutes"
_DB_FITBIT_COLLECTION_SLEEP_DATA_WAKE_30_DAYS_AVG_MIN_KEY = "wakeThirtydayavgminutes"
_DB_FITBIT_COLLECTION_SLEEP_DATA_LIGHT_COUNT_KEY = "lightCount"
_DB_FITBIT_COLLECTION_SLEEP_DATA_LIGHT_MIN_KEY = "lightMinutes"
_DB_FITBIT_COLLECTION_SLEEP_DATA_LIGHT_30_DAYS_AVG_MIN_KEY = "lightThirtydayavgminutes"
_DB_FITBIT_COLLECTION_SLEEP_DATA_REM_COUNT_KEY = "remCount"
_DB_FITBIT_COLLECTION_SLEEP_DATA_REM_MIN_KEY = "remMinutes"
_DB_FITBIT_COLLECTION_SLEEP_DATA_REM_30_DAYS_AVG_MIN_KEY = "remThirtydayavgminutes"
_DB_FITBIT_COLLECTION_SLEEP_DATA_RESTLESS_COUNT_KEY = "restlessCount"
_DB_FITBIT_COLLECTION_SLEEP_DATA_RESTLESS_MIN_KEY = "restlessMinutes"
_DB_FITBIT_COLLECTION_SLEEP_DATA_AWAKE_COUNT_KEY = "awakeCount"
_DB_FITBIT_COLLECTION_SLEEP_DATA_AWAKE_MIN_KEY = "awakeMinutes"
_DB_FITBIT_COLLECTION_SLEEP_DATA_ASLEEP_COUNT_KEY = "asleepCount"
_DB_FITBIT_COLLECTION_SLEEP_DATA_ASLEEP_MIN_KEY = "asleepMinutes"

# ---------------------------------------------#
#      Converted Temperature Documents         #
# ---------------------------------------------#
_DB_FITBIT_COLLECTION_COMP_TEMP_SLEEP_START_KEY = "sleep_start"
_DB_FITBIT_COLLECTION_COMP_TEMP_SLEEP_END_KEY = "sleep_end"

# --------------------------------#
#          SPo2 Documents         #
# --------------------------------#
_DB_FITBIT_COLLECTION_SPO2_TIMESTAMP_KEY = "timestamp"

# --------------------------------#
#  Device Temperature Documents   #
# --------------------------------#
_DB_FITBIT_COLLECTION_DEVICE_TEMP_RECORDED_TIME_KEY = "recorded_time"

# --------------------------------#
#   Daily HRV Summary Documents   #
# --------------------------------#
_DB_FITBIT_COLLECTION_DAILY_HRV_SUMMARY_TIMESTAMP_KEY = "timestamp"
_DB_FITBIT_COLLECTION_DAILY_HRV_SUMMARY_RMSSD_KEY = "rmssd"
_DB_FITBIT_COLLECTION_DAILY_HRV_SUMMARY_NREMHR_KEY = "nremhr"
_DB_FITBIT_COLLECTION_DAILY_HRV_SUMMARY_ENTROPY_KEY = "entropy"

# --------------------------------#
#           HRV Details           #
# --------------------------------#
_DB_FITBIT_COLLECTION_HRV_DETAILS_TIMESTAMP_KEY = "timestamp"
_DB_FITBIT_COLLECTION_HRV_DETAILS_COVERAGE_KEY = "coverage"
_DB_FITBIT_COLLECTION_HRV_DETAILS_LOW_FREQUENCY_KEY = "low_frequency"
_DB_FITBIT_COLLECTION_HRV_DETAILS_HIGH_FREQUENCY_KEY = "high_frequency"
_DB_FITBIT_COLLECTION_HRV_DETAILS_RMSSD_KEY = "rmssd"

# --------------------------------#
#             Profile             #
# --------------------------------#
_DB_FITBIT_COLLECTION_PROFILE_GENDER_COL = "gender"
_DB_FITBIT_COLLECTION_PROFILE_BMI_COL = "bmi"
_DB_FITBIT_COLLECTION_PROFILE_AGE_COL = "age"

# --------------------------------#
#     Respiratory Rate Summary    #
# --------------------------------#
_DB_FITBIT_COLLECTION_RESP_RATE_SUMMARY_FULL_SLEEP_BREATHING_RATE_COL = (
    "full_sleep_breathing_rate"
)
_DB_FITBIT_COLLECTION_RESP_RATE_SUMMARY_TIMESTAMP_COL = "timestamp"

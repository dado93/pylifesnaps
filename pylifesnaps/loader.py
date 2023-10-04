import datetime
from typing import Union

import pandas as pd
import pymongo
from bson.objectid import ObjectId

import pylifesnaps.constants
import pylifesnaps.utils

_METRIC_DICT = {
    pylifesnaps.constants._METRIC_COMP_TEMP: {
        "metric_key": pylifesnaps.constants._DB_FITBIT_COLLECTION_DATA_TYPE_COMP_TEMP_VALUE,
        "start_date_key": pylifesnaps.constants._DB_FITBIT_COLLECTION_COMP_TEMP_SLEEP_START_KEY,
        "end_date_key": pylifesnaps.constants._DB_FITBIT_COLLECTION_COMP_TEMP_SLEEP_END_KEY,
    },
    pylifesnaps.constants._METRIC_SPO2: {
        "metric_key": pylifesnaps.constants._DB_FITBIT_COLLECTION_DATA_TYPE_DAILY_SPO2_VALUE,
        "start_date_key": pylifesnaps.constants._DB_FITBIT_COLLECTION_SPO2_TIMESTAMP_KEY,
        "end_date_key": None,
    },
    pylifesnaps.constants._METRIC_DEVICE_TEMP: {
        "metric_key": pylifesnaps.constants._DB_FITBIT_COLLECTION_DATA_TYPE_DEVICE_TEMP_VALUE,
        "start_date_key": pylifesnaps.constants._DB_FITBIT_COLLECTION_DEVICE_TEMP_RECORDED_TIME_KEY,
        "end_date_key": None,
    },
    pylifesnaps.constants._METRIC_DAILY_HRV_SUMMARY: {
        "metric_key": pylifesnaps.constants._DB_FITBIT_COLLECTION_DATA_TYPE_DAILY_HRV_SUMMARY_VALUE,
        "start_date_key": pylifesnaps.constants._DB_FITBIT_COLLECTION_DAILY_HRV_SUMMARY_TIMESTAMP_KEY,
        "end_date_key": None,
    },
    pylifesnaps.constants._METRIC_HRV_DETAILS: {
        "metric_key": pylifesnaps.constants._DB_FITBIT_COLLECTION_DATA_TYPE_HRV_DETAILS_VALUE,
        "start_date_key": pylifesnaps.constants._DB_FITBIT_COLLECTION_HRV_DETAILS_TIMESTAMP_KEY,
        "end_date_key": None,
    },
    pylifesnaps.constants._METRIC_PROFILE: {
        "metric_key": pylifesnaps.constants._DB_FITBIT_COLLECTION_DATA_TYPE_PROFILE_VALUE,
        "start_date_key": None,
        "end_date_key": None,
    },
    pylifesnaps.constants._METRIC_RESPIRATORY_RATE_SUMMARY: {
        "metric_key": pylifesnaps.constants._DB_FITBIT_COLLECTION_DATA_TYPE_RESPIRATORY_RATE_SUMMARY_VALUE,
        "start_date_key": pylifesnaps.constants._DB_FITBIT_COLLECTION_RESP_RATE_SUMMARY_TIMESTAMP_COL,
        "end_date_key": None,
    },
    pylifesnaps.constants._METRIC_STRESS: {
        "metric_key": pylifesnaps.constants._DB_FITBIT_COLLECTION_DATA_TYPE_STRESS_SCORE_VALUE,
        "start_date_key": pylifesnaps.constants._DB_FITBIT_COLLECTION_STRESS_SCORE_DATE_COL,
        "end_date_key": None,
    },
    pylifesnaps.constants._METRIC_WRIST_TEMPERATURE: {
        "metric_key": pylifesnaps.constants._DB_FITBIT_COLLECTION_DATA_TYPE_WRIST_TEMPERATURE_VALUE,
        "start_date_key": pylifesnaps.constants._DB_FITBIT_COLLECTION_WRIST_TEMP_RECORDED_TIME_COL,
        "end_date_key": None,
    },
    pylifesnaps.constants._METRIC_ALTITUDE: {
        "metric_key": pylifesnaps.constants._DB_FITBIT_COLLECTION_DATA_TYPE_ALTITUDE_VALUE,
        "start_date_key": pylifesnaps.constants._DB_FITBIT_COLLECTION_ALTITUDE_DATETIME_COL,
        "end_date_key": None,
    },
    pylifesnaps.constants._METRIC_BADGE: {
        "metric_key": pylifesnaps.constants._DB_FITBIT_COLLECTION_DATA_TYPE_BADGE_VALUE,
        "start_date_key": pylifesnaps.constants._DB_FITBIT_COLLECTION_BADGE_DATETIME_COL,
        "end_date_key": None,
    },
    pylifesnaps.constants._METRIC_CALORIES: {
        "metric_key": pylifesnaps.constants._DB_FITBIT_COLLECTION_DATA_TYPE_CALORIES_VALUE,
        "start_date_key": pylifesnaps.constants._DB_FITBIT_COLLECTION_CALORIES_DATETIME_COL,
        "end_date_key": None,
    },
    pylifesnaps.constants._METRIC_DISTANCE: {
        "metric_key": pylifesnaps.constants._DB_FITBIT_COLLECTION_DATA_TYPE_DISTANCE_VALUE,
        "start_date_key": pylifesnaps.constants._DB_FITBIT_COLLECTION_DISTANCE_DATETIME_COL,
        "end_date_key": None,
    },
}


class LifeSnapsLoader:
    def __init__(self, host: str = "localhost", port: int = 27017):
        self.host = host
        self.port = port
        self.client = pymongo.MongoClient(self.host, self.port)
        self.db = self.client[pylifesnaps.constants._DB_NAME]
        self.fitbit_collection = self.db[
            pylifesnaps.constants._DB_FITBIT_COLLECTION_NAME
        ]

    def get_user_ids(self) -> list:
        """Get available user ids.

        This function gets available user ids in the DB.

        Returns
        -------
        list
            List of strings of unique user ids.
        """
        return [str(x) for x in self.fitbit_collection.distinct("id")]

    def load_sleep_summary(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> pd.DataFrame:
        """Load sleep summary data.

        This function load sleep summary data from the Mongo DB dataset.
        The returned data comes in the form of a :class:`pd.DataFrame`,
        with each row representing an unique entry of sleep data for
        the given `user_id`. If no sleep data are available for the
        provided `user_id`, then an empty :class:`pd.DataFrame` is
        returned.

        Parameters
        ----------
        user_id : ObjectId or str
            Unique identifier for the user.
        start_date : datetime.datetime or datetime.date or str or None, optional
            Start date for data retrieval, by default None
        end_date : datetime.datetime or datetime.date or str or None, optional
            End date for data retrieval, by default None

        Returns
        -------
        pd.DataFrame
            DataFrame with sleep summary data.

        Raises
        ------
        ValueError
            If `user_id` is not valid.
        ValueError
            If dates are not consistent.
        """
        if str(user_id) not in self.get_user_ids():
            raise ValueError(f"f{user_id} does not exist in DB.")
        user_id = pylifesnaps.utils.check_user_id(user_id)
        start_date = pylifesnaps.utils.convert_to_datetime(start_date)
        end_date = pylifesnaps.utils.convert_to_datetime(end_date)
        date_of_sleep_key = f"{pylifesnaps.constants._DB_FITBIT_COLLECTION_DATA_KEY}"
        date_of_sleep_key += f".{pylifesnaps.constants._DB_FITBIT_COLLECTION_SLEEP_DATA_DATE_OF_SLEEP_KEY}"
        start_sleep_key = f"{pylifesnaps.constants._DB_FITBIT_COLLECTION_DATA_KEY}"
        start_sleep_key += (
            f".{pylifesnaps.constants._DB_FITBIT_COLLECTION_SLEEP_DATA_START_TIME_KEY}"
        )
        date_filter = self._get_start_and_end_date_time_filter_dict(
            start_date_key=date_of_sleep_key,
            start_date=start_date,
            end_date=end_date,
            end_date_key=None,
        )
        filtered_coll = self.fitbit_collection.aggregate(
            [
                {
                    "$match": {
                        pylifesnaps.constants._DB_FITBIT_COLLECTION_TYPE_KEY: pylifesnaps.constants._DB_FITBIT_COLLECTION_DATA_TYPE_SLEEP_VALUE,
                        pylifesnaps.constants._DB_FITBIT_COLLECTION_ID_KEY: user_id,
                    }
                },
                {
                    "$addFields": {
                        date_of_sleep_key: {
                            "$convert": {
                                "input": f"${date_of_sleep_key}",
                                "to": "date",
                            }
                        },
                        start_sleep_key: {
                            "$convert": {
                                "input": f"${start_sleep_key}",
                                "to": "date",
                            }
                        },
                    }
                },
                date_filter,
            ]
        )
        # Convert to dataframe
        sleep_summary_df = pd.DataFrame()
        for sleep_summary in filtered_coll:
            # For each row, save all fields except sleep levels
            filtered_dict = {
                k: sleep_summary[pylifesnaps.constants._DB_FITBIT_COLLECTION_DATA_KEY][
                    k
                ]
                for k in set(
                    list(
                        sleep_summary[
                            pylifesnaps.constants._DB_FITBIT_COLLECTION_DATA_KEY
                        ].keys()
                    )
                )
                - set(["levels"])
            }
            # Create a pd.DataFrame
            temp_df = pd.DataFrame(
                filtered_dict,
                index=[
                    sleep_summary[pylifesnaps.constants._DB_FITBIT_COLLECTION_DATA_KEY][
                        pylifesnaps.constants._DB_FITBIT_COLLECTION_SLEEP_DATA_LOG_ID_KEY
                    ]
                ],
            )
            # Get sleep stage duration from levels
            stages_info = sleep_summary[
                pylifesnaps.constants._DB_FITBIT_COLLECTION_DATA_KEY
            ][pylifesnaps.constants._DB_FITBIT_COLLECTION_SLEEP_DATA_LEVELS_KEY][
                pylifesnaps.constants._DB_FITBIT_COLLECTION_SLEEP_DATA_LEVELS_SUMMARY_KEY
            ]
            for stage_name, stage_values in stages_info.items():
                for stage_values_key, stage_values_value in stage_values.items():
                    temp_df[
                        f"{stage_name}{stage_values_key.title()}"
                    ] = stage_values_value

            sleep_summary_df = pd.concat((sleep_summary_df, temp_df), ignore_index=True)
        if len(sleep_summary_df) > 0:
            sleep_summary_df[pylifesnaps.constants._TIMEZONEOFFSET_IN_MS_COL] = 0
            sleep_summary_df = sleep_summary_df.rename(
                columns={
                    pylifesnaps.constants._DB_FITBIT_COLLECTION_SLEEP_DATA_START_TIME_KEY: pylifesnaps.constants._ISODATE_COL
                }
            )
            sleep_summary_df[
                pylifesnaps.constants._UNIXTIMESTAMP_IN_MS_COL
            ] = sleep_summary_df[pylifesnaps.constants._ISODATE_COL].apply(
                lambda x: int(x.timestamp() * 1000)
            )
            sleep_summary_df = sleep_summary_df.sort_values(
                by=pylifesnaps.constants._DB_FITBIT_COLLECTION_SLEEP_DATA_DATE_OF_SLEEP_KEY
            ).reset_index(drop=True)
            for idx, col in enumerate(
                [
                    pylifesnaps.constants._DB_FITBIT_COLLECTION_SLEEP_DATA_LOG_ID_KEY,
                    pylifesnaps.constants._DB_FITBIT_COLLECTION_SLEEP_DATA_DATE_OF_SLEEP_KEY,
                    pylifesnaps.constants._DB_FITBIT_COLLECTION_SLEEP_DATA_END_TIME_KEY,
                    pylifesnaps.constants._DB_FITBIT_COLLECTION_SLEEP_DATA_DURATION_KEY,
                ]
            ):
                sleep_summary_df.insert(
                    idx,
                    col,
                    sleep_summary_df.pop(col),
                )
        return sleep_summary_df

    def _merge_sleep_data_and_sleep_short_data(
        self, sleep_data: pd.DataFrame, short_sleep_data: list
    ):
        pass

    def _get_raw_sleep_data(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> list:
        pass

    def _get_raw_sleep_short_data(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> list:
        pass

    def _get_raw_short_sleep_data(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> list:
        pass

    def load_sleep_stage(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
        include_short_data: bool = True,
    ) -> pd.DataFrame:
        # We need to load sleep data -> then levels.data and levels.shortData
        # After getting levels.shortData, we merge everything together
        if str(user_id) not in self.get_user_ids():
            raise ValueError(f"f{user_id} does not exist in DB.")
        user_id = pylifesnaps.utils.check_user_id(user_id)
        start_date = pylifesnaps.utils.convert_to_datetime(start_date)
        end_date = pylifesnaps.utils.convert_to_datetime(end_date)
        pylifesnaps.utils.compare_dates(start_date, end_date)
        # Get sleep start key
        sleep_start_key = f"{pylifesnaps.constants._DB_FITBIT_COLLECTION_DATA_KEY}"
        sleep_start_key += (
            f".{pylifesnaps.constants._DB_FITBIT_COLLECTION_SLEEP_DATA_START_TIME_KEY}"
        )
        date_filter = self._get_start_and_end_date_time_filter_dict(
            start_date_key=sleep_start_key,
            end_date_key=None,
            start_date=start_date,
            end_date=end_date,
        )
        filtered_coll = self.fitbit_collection.aggregate(
            [
                {
                    "$match": {
                        pylifesnaps.constants._DB_FITBIT_COLLECTION_TYPE_KEY: pylifesnaps.constants._DB_FITBIT_COLLECTION_DATA_TYPE_SLEEP_VALUE,
                        pylifesnaps.constants._DB_FITBIT_COLLECTION_ID_KEY: user_id,
                    }
                },
                {
                    "$addFields": {
                        sleep_start_key: {
                            "$convert": {
                                "input": f"${sleep_start_key}",
                                "to": "date",
                            }
                        }
                    }
                },
                date_filter,
            ]
        )
        # Convert to dataframe
        sleep_stage_df = pd.DataFrame()
        for sleep_entry in filtered_coll:
            # Get data
            sleep_data_dict = sleep_entry[
                pylifesnaps.constants._DB_FITBIT_COLLECTION_DATA_KEY
            ][pylifesnaps.constants._DB_FITBIT_COLLECTION_SLEEP_DATA_LEVELS_KEY][
                pylifesnaps.constants._DB_FITBIT_COLLECTION_SLEEP_DATA_LEVELS_DATA_KEY
            ]
            # Create a pd.DataFrame with sleep data
            sleep_data_df = pd.DataFrame(sleep_data_dict)
            # Add log id to pd.DataFrame
            sleep_data_df[
                pylifesnaps.constants._DB_FITBIT_COLLECTION_SLEEP_DATA_LOG_ID_KEY
            ] = sleep_entry[pylifesnaps.constants._DB_FITBIT_COLLECTION_DATA_KEY][
                pylifesnaps.constants._DB_FITBIT_COLLECTION_SLEEP_DATA_LOG_ID_KEY
            ]

            # Get shortData if they are there
            if (
                pylifesnaps.constants._DB_FITBIT_COLLECTION_SLEEP_DATA_LEVELS_SHORT_DATA_KEY
                in sleep_entry[pylifesnaps.constants._DB_FITBIT_COLLECTION_DATA_KEY][
                    pylifesnaps.constants._DB_FITBIT_COLLECTION_SLEEP_DATA_LEVELS_KEY
                ].keys()
            ) and include_short_data:
                sleep_short_data_list = sleep_entry[
                    pylifesnaps.constants._DB_FITBIT_COLLECTION_DATA_KEY
                ][pylifesnaps.constants._DB_FITBIT_COLLECTION_SLEEP_DATA_LEVELS_KEY][
                    pylifesnaps.constants._DB_FITBIT_COLLECTION_SLEEP_DATA_LEVELS_SHORT_DATA_KEY
                ]
                datetime_col = (
                    pylifesnaps.constants._DB_FITBIT_COLLECTION_SLEEP_DATA_LEVELS_DATA_DATETIME_KEY
                )
                seconds_col = (
                    pylifesnaps.constants._DB_FITBIT_COLLECTION_SLEEP_DATA_LEVELS_DATA_SECONDS_KEY
                )
                level_col = (
                    pylifesnaps.constants._DB_FITBIT_COLLECTION_SLEEP_DATA_LEVELS_DATA_LEVEL_KEY
                )
                # Create a pd.DataFrame with sleep data

                # We need to inject sleep short data in data
                # 1. Get start and end of sleep from sleep data
                sleep_data_df[datetime_col] = pd.to_datetime(
                    sleep_data_df[datetime_col]
                )
                sleep_start_dt = sleep_data_df.iloc[0][datetime_col]
                sleep_end_dt = sleep_data_df.iloc[-1][
                    datetime_col
                ] + datetime.timedelta(seconds=int(sleep_data_df.iloc[-1][seconds_col]))

                sleep_data_df = sleep_data_df.set_index(datetime_col)

                # 2. Create new list with short data using 30 seconds windows
                sleep_short_data_list_copy = []
                for sleep_short_data_entry in sleep_short_data_list:
                    start_dt = datetime.datetime.strptime(
                        sleep_short_data_entry[datetime_col], "%Y-%m-%dT%H:%M:%S.%f"
                    )
                    if sleep_short_data_entry[seconds_col] > 30:
                        n_to_add = int(sleep_short_data_entry[seconds_col] / 30)
                        for i in range(n_to_add):
                            sleep_short_data_list_copy.append(
                                {
                                    datetime_col: start_dt
                                    + i * datetime.timedelta(seconds=30),
                                    level_col: "wake",
                                    seconds_col: 30,
                                }
                            )
                    else:
                        sleep_short_data_list_copy.append(
                            {
                                datetime_col: start_dt,
                                level_col: "wake",
                                seconds_col: sleep_short_data_entry[seconds_col],
                            }
                        )
                # 3. Create DataFrame with sleep short data and get start and end sleep data
                sleep_short_data_df = pd.DataFrame(sleep_short_data_list_copy)
                sleep_short_data_df[datetime_col] = pd.to_datetime(
                    sleep_short_data_df[datetime_col]
                )
                sleep_short_data_start_dt = sleep_short_data_df.iloc[0][datetime_col]
                sleep_short_data_end_dt = sleep_short_data_df.iloc[-1][
                    datetime_col
                ] + datetime.timedelta(
                    seconds=int(sleep_short_data_df.iloc[-1][seconds_col])
                )
                sleep_short_data_df = sleep_short_data_df.set_index(datetime_col)

                # 4. Let's create a new dataframe that goes from min sleep time to max sleep time
                min_sleep_dt = (
                    sleep_start_dt
                    if sleep_start_dt < sleep_short_data_start_dt
                    else sleep_short_data_start_dt
                )
                max_sleep_dt = (
                    sleep_end_dt
                    if sleep_end_dt > sleep_short_data_end_dt
                    else sleep_short_data_end_dt
                )

                new_sleep_data_df_index = pd.Index(
                    [
                        min_sleep_dt + i * datetime.timedelta(seconds=30)
                        for i in range(
                            int((max_sleep_dt - min_sleep_dt).total_seconds() / 30)
                        )
                    ]
                )
                new_sleep_data_df = pd.DataFrame(index=new_sleep_data_df_index)
                new_sleep_data_df.loc[
                    new_sleep_data_df.index, level_col
                ] = sleep_data_df[level_col]
                new_sleep_data_df[level_col] = new_sleep_data_df.ffill()
                new_sleep_data_df[seconds_col] = 30

                # 5. Inject short data into new dataframe
                new_sleep_data_df.loc[
                    sleep_short_data_df.index, level_col
                ] = sleep_short_data_df[level_col]

                # 6. Create new column with levels and another one for their changes
                new_sleep_data_df["levelMap"] = pd.factorize(
                    new_sleep_data_df[level_col]
                )[0]
                new_sleep_data_df["levelMapDiff"] = new_sleep_data_df["levelMap"].diff()

                # 7. Create new column with id for each sleep stage
                new_sleep_data_df["levelGroup"] = (
                    new_sleep_data_df["levelMap"]
                    != new_sleep_data_df["levelMap"].shift()
                ).cumsum()
                new_sleep_data_df = new_sleep_data_df.reset_index(names=datetime_col)
                # 8. Get rows when change is detected
                final_sleep_df = (
                    new_sleep_data_df[new_sleep_data_df["levelMapDiff"] != 0]
                    .copy()
                    .reset_index(drop=True)
                )

                # 9. Get total seconds with isoDate information
                final_sleep_df[seconds_col] = (
                    (
                        new_sleep_data_df.groupby("levelGroup")[datetime_col].last()
                        + datetime.timedelta(seconds=30)
                        - new_sleep_data_df.groupby("levelGroup")[datetime_col].first()
                    )
                    .dt.total_seconds()
                    .reset_index(drop=True)
                )
                sleep_data_df = final_sleep_df.copy()

                sleep_data_df = sleep_data_df.drop(
                    ["levelMap", "levelMapDiff", "levelGroup"], axis=1
                )
                sleep_data_df[
                    pylifesnaps.constants._DB_FITBIT_COLLECTION_SLEEP_DATA_LOG_ID_KEY
                ] = sleep_entry[pylifesnaps.constants._DB_FITBIT_COLLECTION_DATA_KEY][
                    pylifesnaps.constants._DB_FITBIT_COLLECTION_SLEEP_DATA_LOG_ID_KEY
                ]

            sleep_stage_df = pd.concat(
                (sleep_stage_df, sleep_data_df), ignore_index=True
            )
        if len(sleep_stage_df) > 0:
            sleep_stage_df[pylifesnaps.constants._ISODATE_COL] = pd.to_datetime(
                sleep_stage_df[
                    pylifesnaps.constants._DB_FITBIT_COLLECTION_SLEEP_DATA_LEVELS_DATA_DATETIME_KEY
                ],
                utc=True,
            ).dt.tz_localize(None)
            sleep_stage_df = sleep_stage_df.drop(
                [
                    pylifesnaps.constants._DB_FITBIT_COLLECTION_SLEEP_DATA_LEVELS_DATA_DATETIME_KEY
                ],
                axis=1,
            )
            sleep_stage_df[
                pylifesnaps.constants._UNIXTIMESTAMP_IN_MS_COL
            ] = sleep_stage_df[pylifesnaps.constants._ISODATE_COL].apply(
                lambda x: int(x.timestamp() * 1000)
            )
            sleep_stage_df = sleep_stage_df.sort_values(
                by=pylifesnaps.constants._UNIXTIMESTAMP_IN_MS_COL
            ).reset_index(drop=True)
            sleep_stage_df[pylifesnaps.constants._TIMEZONEOFFSET_IN_MS_COL] = 0
        return sleep_stage_df

    def load_metric(
        self,
        metric: str,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> pd.DataFrame:
        if str(user_id) not in self.get_user_ids():
            raise ValueError(f"f{user_id} does not exist in DB.")
        user_id = pylifesnaps.utils.check_user_id(user_id)
        start_date = pylifesnaps.utils.convert_to_datetime(start_date)
        end_date = pylifesnaps.utils.convert_to_datetime(end_date)

        metric_start_key = _METRIC_DICT[metric]["start_date_key"]
        metric_end_key = _METRIC_DICT[metric]["end_date_key"]
        if metric_start_key is None:
            metric_start_date_key_db = None
        else:
            metric_start_date_key_db = (
                pylifesnaps.constants._DB_FITBIT_COLLECTION_DATA_KEY
                + "."
                + metric_start_key
            )
        if metric_end_key is None:
            metric_end_date_key_db = None
        else:
            metric_end_date_key_db = (
                pylifesnaps.constants._DB_FITBIT_COLLECTION_DATA_KEY
                + "."
                + metric_end_key
            )

        date_filter_dict = self._get_start_and_end_date_time_filter_dict(
            start_date_key=metric_start_date_key_db,
            end_date_key=metric_end_date_key_db,
            start_date=start_date,
            end_date=end_date,
        )
        date_conversion_dict = self._get_date_conversion_dict(
            start_date_key=metric_start_date_key_db, end_date_key=metric_end_date_key_db
        )
        filtered_coll = self.fitbit_collection.aggregate(
            [
                {
                    "$match": {
                        pylifesnaps.constants._DB_FITBIT_COLLECTION_TYPE_KEY: _METRIC_DICT[
                            metric
                        ][
                            "metric_key"
                        ],
                        pylifesnaps.constants._DB_FITBIT_COLLECTION_ID_KEY: user_id,
                    }
                },
                date_conversion_dict,
                date_filter_dict,
            ]
        )
        metric_df = pd.DataFrame()
        list_of_metric_dict = [
            entry[pylifesnaps.constants._DB_FITBIT_COLLECTION_DATA_KEY]
            for entry in filtered_coll
        ]
        metric_df = pd.DataFrame(list_of_metric_dict)
        if len(metric_df) > 0 and (metric_start_key is not None):
            metric_df = metric_df.sort_values(by=metric_start_key).reset_index(
                drop=True
            )
        metric_df = self._setup_datetime_columns(df=metric_df, metric=metric)
        return metric_df

    def load_computed_temperature(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> pd.DataFrame:
        """Load computed temperature from DB.

        This function loads the computed temperature data from
        the database. The returned :class:`pd.DataFrame`
        contains temperature data with baseline values for the
        given `user_id` over the time interval from
        ``start_date`` to ``end_date``.

        Parameters
        ----------
        user_id : ObjectId or str
            Unique identifier for the user.
        start_date : datetime.datetime or datetime.date or str or None, optional
            Start date for data retrieval, by default None
        end_date : datetime.datetime or datetime.date or str or None, optional
            End date for data retrieval, by default None

        Returns
        -------
        pd.DataFrame
            _description_

        Raises
        ------
        ValueError
            If incorrect values for parameters are used.
        """
        return self.load_metric(
            pylifesnaps.constants._METRIC_COMP_TEMP,
            user_id=user_id,
            start_date=start_date,
            end_date=end_date,
        )

    def load_daily_spo2(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> pd.DataFrame:
        """Load daily SpO2 values.

        This function loads SpO2 values from the DB. The returned
        :class:`pd.DataFrame` contains the following colums:

        - timestamp: date
        - average_value: average measured SpO2
        - lower_bound: lower bound of measured SpO2
        - upper_bound: upper bound of measured SpO2

        Parameters
        ----------
        user_id : ObjectId or str
            Unique identifier for the user.
        start_date : datetime.datetime or datetime.date or str or None, optional
            Start date for data retrieval, by default None
        end_date : datetime.datetime or datetime.date or str or None, optional
            End date for data retrieval, by default None

        Returns
        -------
        pd.DataFrame
            Daily SpO2 values.
        """
        return self.load_metric(
            metric=pylifesnaps.constants._METRIC_SPO2,
            user_id=user_id,
            start_date=start_date,
            end_date=end_date,
        )

    def load_device_temperature(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> pd.DataFrame:
        return self.load_metric(
            metric=pylifesnaps.constants._METRIC_DEVICE_TEMP,
            user_id=user_id,
            start_date=start_date,
            end_date=end_date,
        )

    def load_hrv_details(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> pd.DataFrame:
        hrv_details = self.load_metric(
            metric=pylifesnaps.constants._METRIC_HRV_DETAILS,
            user_id=user_id,
            start_date=start_date,
            end_date=end_date,
        )
        return hrv_details

    def load_daily_hrv_summary(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> pd.DataFrame:
        return self.load_metric(
            metric=pylifesnaps.constants._METRIC_DAILY_HRV_SUMMARY,
            user_id=user_id,
            start_date=start_date,
            end_date=end_date,
        )

    def load_hrv_histogram(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> pd.DataFrame:
        pass

    def load_profile(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> pd.DataFrame:
        return self.load_metric(
            metric=pylifesnaps.constants._METRIC_PROFILE,
            user_id=user_id,
            start_date=start_date,
            end_date=end_date,
        )

    def load_respiratory_rate_summary(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> pd.DataFrame:
        resp_rate_summary = self.load_metric(
            metric=pylifesnaps.constants._METRIC_RESPIRATORY_RATE_SUMMARY,
            user_id=user_id,
            start_date=start_date,
            end_date=end_date,
        )
        return resp_rate_summary

    def load_stress_score(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> pd.DataFrame:
        stress_score = self.load_metric(
            metric=pylifesnaps.constants._METRIC_STRESS,
            user_id=user_id,
            start_date=start_date,
            end_date=end_date,
        )
        return stress_score

    def load_wrist_temperature(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> pd.DataFrame:
        wrist_temperature = self.load_metric(
            metric=pylifesnaps.constants._METRIC_WRIST_TEMPERATURE,
            user_id=user_id,
            start_date=start_date,
            end_date=end_date,
        )
        return wrist_temperature

    def load_altitude(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> pd.DataFrame:
        altitude = self.load_metric(
            metric=pylifesnaps.constants._METRIC_ALTITUDE,
            user_id=user_id,
            start_date=start_date,
            end_date=end_date,
        )
        return altitude

    def load_badge(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> pd.DataFrame:
        badge = self.load_metric(
            metric=pylifesnaps.constants._METRIC_BADGE,
            user_id=user_id,
            start_date=start_date,
            end_date=end_date,
        )
        return badge

    def load_calories(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> pd.DataFrame:
        calories = self.load_metric(
            metric=pylifesnaps.constants._METRIC_CALORIES,
            user_id=user_id,
            start_date=start_date,
            end_date=end_date,
        )
        return calories

    def load_demographic_vo2_max(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> pd.DataFrame:
        pass

    def load_distance(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> pd.DataFrame:
        distance = self.load_metric(
            metric=pylifesnaps.constants._METRIC_DISTANCE,
            user_id=user_id,
            start_date=start_date,
            end_date=end_date,
        )
        return distance

    def load_estimated_oxygen_variation(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> pd.DataFrame:
        pass

    def load_exercise(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> pd.DataFrame:
        pass

    def load_heart_rate(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> pd.DataFrame:
        pass

    def load_journal_entries(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> pd.DataFrame:
        pass

    def load_lighly_active_minutes(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> pd.DataFrame:
        pass

    def load_mindfulness_eda_data_sessions(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> pd.DataFrame:
        pass

    def load_mindfulness_goals(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> pd.DataFrame:
        pass

    def load_mindfulness_sessions(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> pd.DataFrame:
        pass

    def load_moderately_active_minutes(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> pd.DataFrame:
        pass

    def load_resting_heart_rate(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> pd.DataFrame:
        pass

    def load_sedentary_minutes(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> pd.DataFrame:
        pass

    def load_steps(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> pd.DataFrame:
        pass

    def load_time_in_heart_rate_zones(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> pd.DataFrame:
        pass

    def load_very_active_minutes(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> pd.DataFrame:
        pass

    def load_water_logs(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> pd.DataFrame:
        pass

    def load_step_goal_survey(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> pd.DataFrame:
        pass

    def load_context_and_mood_survey(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> pd.DataFrame:
        pass

    def get_sema_statistics():
        pass

    def get_sleep_statistics():
        pass

    def _get_start_and_end_date_time_filter_dict(
        self, start_date_key, end_date_key=None, start_date=None, end_date=None
    ) -> dict:
        if end_date_key is None:
            end_date_key = start_date_key
        if (not (start_date is None)) and (not (end_date is None)):
            if end_date < start_date:
                raise ValueError(f"{end_date} must be greater than {start_date}")
            date_filter = {
                "$match": {
                    "$and": [
                        {start_date_key: {"$gte": start_date}},
                        {end_date_key: {"$lte": end_date}},
                    ]
                }
            }
        elif (start_date is None) and (not (end_date is None)):
            date_filter = {"$match": {end_date_key: {"$lte": end_date}}}
        elif (not (start_date is None)) and (end_date is None):
            date_filter = {"$match": {start_date_key: {"$gte": start_date}}}
        else:
            date_filter = {"$match": {}}

        return date_filter

    def _get_date_conversion_dict(self, start_date_key, end_date_key=None) -> dict:
        if start_date_key is None:
            date_conversion_dict = {"$addFields": {}}
        elif (not start_date_key is None) and (not end_date_key is None):
            date_conversion_dict = {
                "$addFields": {
                    start_date_key: {
                        "$convert": {
                            "input": f"${start_date_key}",
                            "to": "date",
                        }
                    },
                    end_date_key: {
                        "$convert": {
                            "input": f"${end_date_key}",
                            "to": "date",
                        }
                    },
                }
            }
        elif (not start_date_key is None) and (end_date_key is None):
            date_conversion_dict = {
                "$addFields": {
                    start_date_key: {
                        "$convert": {
                            "input": f"${start_date_key}",
                            "to": "date",
                        }
                    },
                }
            }
        return date_conversion_dict

    def _setup_datetime_columns(self, df: pd.DataFrame, metric: str):
        if len(df) > 0:
            df = df.rename(
                columns={
                    _METRIC_DICT[metric][
                        "start_date_key"
                    ]: pylifesnaps.constants._ISODATE_COL
                }
            )
            df[pylifesnaps.constants._UNIXTIMESTAMP_IN_MS_COL] = df[
                pylifesnaps.constants._ISODATE_COL
            ].apply(lambda x: int(x.timestamp() * 1000))
            df[pylifesnaps.constants._TIMEZONEOFFSET_IN_MS_COL] = 0
        return df

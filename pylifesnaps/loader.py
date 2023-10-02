import datetime
from typing import Union

import pandas as pd
import pymongo
from bson.objectid import ObjectId

import pylifesnaps.constants
import pylifesnaps.utils

"""
['Afib ECG Readings', 'Computed Temperature', 'Daily Heart Rate Variability Summary', 'Daily SpO2', 'Device Temperature', 
'Heart Rate Variability Details', 'Heart Rate Variability Histogram', 'Profile', 'Respiratory Rate Summary', 'Stress Score', 
'Wrist Temperature', 'altitude', 'badge', 'calories', 'demographic_vo2_max', 'distance', 'estimated_oxygen_variation', 
'exercise', 'heart_rate', 'journal_entries', 'lightly_active_minutes', 'mindfulness_eda_data_sessions', 'mindfulness_goals', 
'mindfulness_sessions', 'moderately_active_minutes', 'resting_heart_rate', 'sedentary_minutes', 'sleep', 'steps',
'time_in_heart_rate_zones', 'very_active_minutes', 'water_logs']
"""


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
        date_filter = self._get_filter_start_and_end_date_time(
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
                        }
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
            sleep_summary_df = sleep_summary_df.sort_values(
                by=pylifesnaps.constants._DB_FITBIT_COLLECTION_SLEEP_DATA_DATE_OF_SLEEP_KEY
            ).reset_index(drop=True)
            for idx, col in enumerate(
                [
                    pylifesnaps.constants._DB_FITBIT_COLLECTION_SLEEP_DATA_LOG_ID_KEY,
                    pylifesnaps.constants._DB_FITBIT_COLLECTION_SLEEP_DATA_DATE_OF_SLEEP_KEY,
                    pylifesnaps.constants._DB_FITBIT_COLLECTION_SLEEP_DATA_START_TIME_KEY,
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

    def load_sleep_stages(
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
        date_filter = self._get_filter_start_and_end_date_time(
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
        return sleep_stage_df

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
        user_id : Union[ObjectId, str]
            _description_
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
        if str(user_id) not in self.get_user_ids():
            raise ValueError(f"f{user_id} does not exist in DB.")
        user_id = pylifesnaps.utils.check_user_id(user_id)
        start_date = pylifesnaps.utils.convert_to_datetime(start_date)
        end_date = pylifesnaps.utils.convert_to_datetime(end_date)
        # Get sleep start key
        sleep_start_key = pylifesnaps.constants._DB_FITBIT_COLLECTION_DATA_KEY
        sleep_start_key += (
            f".{pylifesnaps.constants._DB_FITBIT_COLLECTION_COMP_TEMP_SLEEP_START_KEY}"
        )
        # Get sleep end key
        sleep_end_key = pylifesnaps.constants._DB_FITBIT_COLLECTION_DATA_KEY
        sleep_end_key += (
            f".{pylifesnaps.constants._DB_FITBIT_COLLECTION_COMP_TEMP_SLEEP_END_KEY}"
        )
        date_filter = self._get_filter_start_and_end_date_time(
            start_date_key=sleep_start_key,
            end_date_key=sleep_end_key,
            start_date=start_date,
            end_date=end_date,
        )
        filtered_coll = self.fitbit_collection.aggregate(
            [
                {
                    "$match": {
                        pylifesnaps.constants._DB_FITBIT_COLLECTION_TYPE_KEY: pylifesnaps.constants._DB_FITBIT_COLLECTION_DATA_TYPE_COMP_TEMP_VALUE,
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
                        },
                        sleep_end_key: {
                            "$convert": {
                                "input": f"${sleep_end_key}",
                                "to": "date",
                            }
                        },
                    }
                },
                date_filter,
            ]
        )
        comp_temp_df = pd.DataFrame()
        for entry in filtered_coll:
            temp_df = pd.DataFrame(
                entry[pylifesnaps.constants._DB_FITBIT_COLLECTION_DATA_KEY], index=[0]
            )
            comp_temp_df = pd.concat((comp_temp_df, temp_df), ignore_index=True)
        comp_temp_df = comp_temp_df.sort_values(
            by=pylifesnaps.constants._DB_FITBIT_COLLECTION_COMP_TEMP_SLEEP_START_KEY
        ).reset_index(drop=True)
        return comp_temp_df

    def load_daily_spo2(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> pd.DataFrame:
        pass

    def load_device_temperature(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> pd.DataFrame:
        pass

    def load_hrv_details(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> pd.DataFrame:
        pass

    def load_daily_hrv_summary(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> pd.DataFrame:
        pass

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
        pass

    def load_respiratory_rate_summary(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> pd.DataFrame:
        pass

    def load_stress_score(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> pd.DataFrame:
        pass

    def load_wrist_temperature(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> pd.DataFrame:
        pass

    def load_altitude(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> pd.DataFrame:
        pass

    def load_badges(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> pd.DataFrame:
        pass

    def load_calories(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> pd.DataFrame:
        pass

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
        pass

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

    def _get_filter_start_and_end_date_time(
        self, start_date_key, end_date_key=None, start_date=None, end_date=None
    ):
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

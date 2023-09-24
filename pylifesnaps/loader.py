import datetime
from typing import Union

import pandas as pd
import pymongo
from bson.objectid import ObjectId

import pylifesnaps.constants
import pylifesnaps.utils


class LifeSnapsLoader:
    def __init__(self, host: str = "localhost", port: int = 27017):
        self.host = host
        self.port = port
        self.client = pymongo.MongoClient(self.host, self.port)
        self.db = self.client[pylifesnaps.constants._DB_NAME]
        self.fitbit_collection = self.db[
            pylifesnaps.constants._DB_FITBIT_COLLECTION_NAME
        ]
        print(self.db["surveys"].distinct(key="type"))

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
        """LOad sleep summary data.

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
        if not isinstance(user_id, ObjectId):
            user_id = ObjectId(user_id)
        start_date = pylifesnaps.utils.convert_to_datetime(start_date)
        end_date = pylifesnaps.utils.convert_to_datetime(end_date)
        _DATE_OF_SLEEP_KEY = f"{pylifesnaps.constants._DB_FITBIT_COLLECTION_DATA_KEY}"
        _DATE_OF_SLEEP_KEY += f".{pylifesnaps.constants._DB_FITBIT_COLLECTION_SLEEP_DATA_DATE_OF_SLEEP_KEY}"
        if (not (start_date is None)) and (not (end_date is None)):
            if end_date < start_date:
                raise ValueError(f"{end_date} must be greater than {start_date}")
            date_filter = {
                "$match": {
                    "$and": [
                        {_DATE_OF_SLEEP_KEY: {"$gte": start_date}},
                        {_DATE_OF_SLEEP_KEY: {"$lte": end_date}},
                    ]
                }
            }
        elif (start_date is None) and (not (end_date is None)):
            date_filter = {"$match": {_DATE_OF_SLEEP_KEY: {"$lte": end_date}}}
        elif (not (start_date is None)) and (end_date is None):
            date_filter = {"$match": {_DATE_OF_SLEEP_KEY: {"$gte": start_date}}}
        else:
            date_filter = {"$match": {}}
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
                        _DATE_OF_SLEEP_KEY: {
                            "$convert": {
                                "input": f"${_DATE_OF_SLEEP_KEY}",
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

    def load_sleep_stages(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> pd.DataFrame:
        pass

    def load_computed_temperature(
        self,
        user_id: Union[ObjectId, str],
        start_date: Union[datetime.datetime, datetime.date, str, None] = None,
        end_date: Union[datetime.datetime, datetime.date, str, None] = None,
    ) -> pd.DataFrame:
        pass

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

    def _filter_start_and_date_time(self, collection, date_key, start_date, end_date):
        pass

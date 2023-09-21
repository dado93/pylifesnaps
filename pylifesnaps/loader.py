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
        if not isinstance(user_id, ObjectId):
            user_id = ObjectId(user_id)
        start_date = pylifesnaps.utils.convert_to_datetime(start_date)
        end_date = pylifesnaps.utils.convert_to_datetime(end_date)
        if (not (start_date is None)) and (not (end_date is None)):
            date_filter = {
                "$match": {
                    "$and": [
                        {
                            f"{pylifesnaps.constants._DB_FITBIT_COLLECTION_DATA_KEY}.{pylifesnaps.constants._DB_FITBIT_COLLECTION_SLEEP_DATE_OF_SLEEP_KEY}": {
                                "$gte": start_date
                            }
                        },
                        {
                            f"{pylifesnaps.constants._DB_FITBIT_COLLECTION_DATA_KEY}.{pylifesnaps.constants._DB_FITBIT_COLLECTION_SLEEP_DATE_OF_SLEEP_KEY}": {
                                "$lte": end_date
                            }
                        },
                    ]
                }
            }
        elif (start_date is None) and (not (end_date is None)):
            date_filter = {
                "$match": {
                    f"{pylifesnaps.constants._DB_FITBIT_COLLECTION_DATA_KEY}.{pylifesnaps.constants._DB_FITBIT_COLLECTION_SLEEP_DATE_OF_SLEEP_KEY}": {
                        "$lte": end_date
                    }
                }
            }
        elif (not (start_date is None)) and (end_date is None):
            date_filter = {
                "$match": {
                    f"{pylifesnaps.constants._DB_FITBIT_COLLECTION_DATA_KEY}.{pylifesnaps.constants._DB_FITBIT_COLLECTION_SLEEP_DATE_OF_SLEEP_KEY}": {
                        "$gte": start_date
                    }
                }
            }
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
                        f"{pylifesnaps.constants._DB_FITBIT_COLLECTION_DATA_KEY}.{pylifesnaps.constants._DB_FITBIT_COLLECTION_SLEEP_DATE_OF_SLEEP_KEY}": {
                            "$convert": {
                                "input": f"${pylifesnaps.constants._DB_FITBIT_COLLECTION_DATA_KEY}.{pylifesnaps.constants._DB_FITBIT_COLLECTION_SLEEP_DATE_OF_SLEEP_KEY}",
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
        sleep_summary_df = sleep_summary_df.sort_values(
            by=pylifesnaps.constants._DB_FITBIT_COLLECTION_SLEEP_DATE_OF_SLEEP_KEY
        ).reset_index(drop=True)
        sleep_summary_df = sleep_summary_df.loc[:, []]
        return sleep_summary_df

    def load_sleep_stages(self, user_id, start_date, end_date):
        pass

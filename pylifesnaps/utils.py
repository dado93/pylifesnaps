import datetime
from typing import Union

import dateutil.parser
from bson import ObjectId


def convert_to_datetime(
    date: Union[datetime.datetime, datetime.date, str, None]
) -> datetime.datetime:
    """Convert input to :class:`datetime.datetime`.

    Parameters
    ----------
    date : datetime.datetime or datetime.date or str or None
        The date/time to be converted.

    Returns
    -------
    datetime.datetime
        The converted date/time.

    Raises
    ------
    ValueError
        Input parameter is not of valid data type.
    """
    if type(date) == datetime.date:
        return datetime.datetime.combine(date, datetime.time())
    elif type(date) == str:
        parsed_date = dateutil.parser.parse(date)
        return parsed_date
    elif isinstance(date, datetime.datetime):
        return date
    elif date is None:
        return None
    else:
        raise ValueError


def compare_dates(start_date: datetime.datetime, end_date: datetime.datetime) -> bool:
    if not ((start_date is None) and (end_date is None)):
        if end_date < start_date:
            raise ValueError(f"{start_date} is greater than {end_date}")
    return True


def check_user_id(user_id):
    if not isinstance(user_id, ObjectId):
        user_id = ObjectId(user_id)
    return user_id

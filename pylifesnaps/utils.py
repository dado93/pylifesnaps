import datetime
from typing import Union

import dateutil.parser


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
    if isinstance(date, datetime.date):
        return datetime.datetime.combine(date, datetime.time())
    elif type(date) == str:
        parsed_date = dateutil.parser.parse(date)
        return parsed_date
    elif isinstance(date, datetime.datetime):
        return datetime.datetime(date)
    elif date is None:
        return None
    else:
        raise ValueError

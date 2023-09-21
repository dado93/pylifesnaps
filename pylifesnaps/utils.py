import datetime

import dateutil.parser


def convert_to_datetime(date) -> datetime.datetime:
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

import datetime


def encode_date(year, month, day):
    """
    Encode a date as a floating-point number representing the number of days since 1-jan-2017. Used to decode satellite measurement data.

    Args:
        year (int): year of the measurement
        month (int): month of the measurement
        day (int): day of the measurement

    Returns:
        timedelta: measurement day in days since January 2017
    """
    date = datetime.datetime(year, month, day)
    base_date = datetime.datetime(2017, 1, 1)
    delta = date - base_date
    return delta.total_seconds() / 86400.0  # Convert seconds to days

import pandas as pd

# https://gist.github.com/jrgcubano/c4dbaa879a1cfc9899f961d6eafa737c
# https://www.mssqltips.com/sqlservertip/4054/creating-a-date-dimension-or-calendar-table-in-sql-server/
# https://stackoverflow.com/questions/47150709/how-to-create-a-calendar-table-date-dimension-in-pandas

def dim_date(start='2022-01-01', end='2024-12-31'):

    """Creates calendar table for star schema.
    
    Arguments:
        # start date (YYYY-MM-DD)
        # end date (YYYY-MM-DD)
    
    Returns dataframe with:
        # date_id (INT - unique - YYYYMMDD format)
        # year eg 2024
        # month eg 01
        # day eg 24
        # day_of_week eg 02
        # day_name eg Tuesday
        # month_name eg February
        # quarter eg 04
    """

    dates = pd.date_range(start=start, end=end)
    df = pd.DataFrame({
        "date_id": dates.strftime('%Y%m%d').astype(int),
        "year": dates.year,
        "month": dates.month,
        "day": dates.day,
        "day_of_week": dates.dayofweek,
        "day_name": dates.day_name(),
        "month_name": dates.month_name(),
        "quarter": dates.quarter
    })

    return df
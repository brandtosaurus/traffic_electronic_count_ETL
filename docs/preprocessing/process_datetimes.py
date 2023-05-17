import pandas as pd
import traceback
import gc
import os
import csv
from datetime import timedelta, date
import config


# !most likely to go wrong
def process_datetimes(df: pd.DataFrame, type: str) -> pd.DataFrame:
    """
    It takes a dataframe, checks if the first column is equal to 10, if it is, it does one thing, if
    it isn't, it does another

    :param df: the dataframe
    :type df: pd.DataFrame
    :return: A dataframe with the columns renamed and the datetime columns added.
    """

    if type == "summary":
        duration_min_col = 4
        date_col = 2
        time_col = 3
        add_day = 1
        date_col_name = "end_date"
        time_col_name = "end_time"
        max_time_str_len = int(df[time_col].str.len().max())
    elif type == "indv":
        date_col = 4
        time_col = 5
        add_day = 0
        date_col_name = "departure_date"
        time_col_name = "departure_time"
        max_time_str_len = int(df[time_col].str.len().max())
    else:
        raise Exception("type must be either 'summary' or 'indv'")

    if max_time_str_len > 6:
        time_fmt = "%H%M%S%f"  # 00000000
        time_format = "%H:%M:%S.%f"  # 00:00:00.00
    elif max_time_str_len == 6:
        time_fmt = "%H%M%S"  # 00000000
        time_format = "%H:%M:%S"  # 00:00:00.00
    elif max_time_str_len == 4:
        time_fmt = "%H%M"  # 00000000
        time_format = "%H:%M"  # 00:00:00.00

    date_fmt = "%Y%m%d"  # YYYYMMDD
    date_format = "%Y-%m-%d"  # YYYY-MM-DD

    df[time_col] = df[time_col].str.pad(
        width=max_time_str_len, side="right", fillchar="0"
    )
    df[time_col].loc[df[time_col].str[:2] == "24"] = ("0").zfill(max_time_str_len)

    df[date_col] = df[date_col].apply(
        lambda x: str(date.today())[:2] + x
        if int(x[:2]) <= 50 and len(x) == 6
        else (
            str(int(str(date.today())[:2]) - 1) + x
            if int(x[:2]) > 50 and len(x) == 6
            else x
        )
    )

    if type == "indv":
        df[date_col] = pd.to_datetime(df[date_col], format=date_fmt).dt.strftime(
            date_format
        )
    else:
        df[date_col] = df[date_col].apply(
            lambda x: pd.to_datetime(x, format=date_fmt).date()
            + timedelta(days=add_day)
            if x[time_col]
            in ["0".zfill(max_time_str_len), "24".ljust(max_time_str_len, "0")]
            else pd.to_datetime(x, format=date_fmt).date()
        )

    df["end_datetime"] = pd.to_datetime(
        (df[date_col].astype(str) + df[time_col].astype(str)),
        format=date_format + time_fmt,
    )

    if df.loc[df[0] == "10"].empty:
        df["start_datetime"] = df.apply(
            lambda x: pd.to_datetime(
                str(x[date_col]) + str(x[time_col]), format=date_format + time_fmt
            )
            - timedelta(minutes=int(x[duration_min_col])),
            axis=1,
        )
    else:
        df["start_datetime"] = pd.to_datetime(
            df[date_col] + df[time_col], format=date_format + time_fmt
        )

    df[time_col] = pd.to_datetime(df[time_col], format=time_fmt).dt.strftime(
        time_format
    )

    df.rename(columns={date_col: date_col_name, time_col: time_col_name}, inplace=True)
    return df

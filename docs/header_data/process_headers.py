import pandas as pd
import numpy as np
from datetime import timedelta, date
import traceback


def header(df: pd.DataFrame, file: str, header_id, site_id) -> pd.DataFrame:
    """
    It takes a dataframe, finds the header information, and returns a dataframe with the header
    information

    :param df: pd.DataFrame
    :type df: pd.DataFrame
    :return: A dataframe
    """
    df = df.reset_index(drop=True)
    st_nd = df.loc[df[0].isin(["D1", "D3"]), 0:4].reset_index(drop=True).copy()

    # standard column identifiers
    st_dt_col = 1
    end_dt_col = 3
    st_tme_col = 2
    end_tme_col = 4

    # checks for all non numeric characters in the date and removes them
    st_nd[st_dt_col] = st_nd[st_dt_col].str.extract("(\d+)", expand=False)
    st_nd[end_dt_col] = st_nd[end_dt_col].str.extract("(\d+)", expand=False)
    st_nd[st_tme_col] = st_nd[st_tme_col].str.extract("(\d+)", expand=False)
    st_nd[end_tme_col] = st_nd[end_tme_col].str.extract("(\d+)", expand=False)
    st_nd.loc[st_nd[st_dt_col].str.len() < 6, st_dt_col] = None
    st_nd.loc[st_nd[end_dt_col].str.len() < 6, end_dt_col] = None

    # backfills the missing dates with the previous date
    st_nd[st_dt_col].bfill(inplace=True)
    st_nd[end_dt_col].bfill(inplace=True)
    st_nd[st_tme_col].bfill(inplace=True)
    st_nd[end_tme_col].bfill(inplace=True)

    # adds century to year if it is not there
    st_nd[st_dt_col] = st_nd[st_dt_col].apply(
        lambda x: str(date.today())[:2] + x
        if len(x) == 6 and int(x[:2]) <= 50
        else str(int(str(date.today())[:2]) - 1) + x
        if len(x) == 6 and int(x[:2]) > 50
        else x
    )
    st_nd[end_dt_col] = st_nd[end_dt_col].apply(
        lambda x: str(date.today())[:2] + x
        if len(x) == 6 and int(x[:2]) <= 50
        else str(int(str(date.today())[:2]) - 1) + x
        if len(x) == 6 and int(x[:2]) > 50
        else x
    )

    # index 2 and 4 are time, this makes the time uniform length
    st_nd[st_tme_col] = st_nd[st_tme_col].str.pad(width=7, side="right", fillchar="0")
    st_nd[end_tme_col] = st_nd[end_tme_col].str.pad(width=7, side="right", fillchar="0")

    # this filters for time = 24H00m and makes it zero hour
    st_nd[end_tme_col].loc[st_nd[end_tme_col].str[:2] == "24"] = ("0").zfill(7)

    try:  # Looks for invalid date then changes to date format
        st_nd[st_dt_col] = st_nd[st_dt_col].apply(
            lambda x: pd.to_datetime(str(int(x) + 1), format="%Y%m%d")
            if x[-2:] == "00"
            else pd.to_datetime(x, format="%Y%m%d").date()
        )
    except ValueError:
        traceback.print_exc()

    try:  # adds a day if the hour is zero hour and changes string to datetime.date
        st_nd[end_dt_col] = st_nd[end_dt_col].apply(
            lambda x: pd.to_datetime(x[end_dt_col], format="%Y%m%d").date()
            + timedelta(days=1)
            if x[4] == "0".zfill(7)
            else pd.to_datetime(x[end_dt_col], format="%Y%m%d").date()
        )
    except (ValueError, TypeError):
        print(file)
        st_nd[end_dt_col] = pd.to_datetime(st_nd[end_dt_col], format="%Y%m%d").dt.date

    # changes time string into datetime.time
    try:
        st_nd[st_tme_col] = pd.to_datetime(st_nd[st_tme_col], format="%H%M%S%f").dt.time
    except ValueError:
        st_nd[st_tme_col] = pd.to_datetime("0".zfill(7), format="%H%M%S%f").strftime(
            "%H:%M:%S"
        )
    try:
        st_nd[end_tme_col] = pd.to_datetime(
            st_nd[end_tme_col], format="%H%M%S%f"
        ).dt.time
    except ValueError:
        st_nd[end_tme_col] = pd.to_datetime("0".zfill(7), format="%H%M%S%f").strftime(
            "%H:%M:%S"
        )
    except:
        raise Exception(
            f"""header func: time not working 
        - st_nd[st_tme_col] = st_nd[st_tme_col].apply(lambda x: pd.to_datetime(x, format='%H%M%S%f').time())
            look at file {file}"""
        )

    # creates start_datetime and end_datetime
    st_nd["start_datetime"] = pd.to_datetime(
        (st_nd[st_dt_col].astype(str)), format="%Y-%m-%d"
    )
    st_nd["end_datetime"] = pd.to_datetime(
        (st_nd[end_dt_col].astype(str)), format="%Y-%m-%d"
    )

    st_nd = st_nd.iloc[:, 1:].drop_duplicates()

    headers = pd.DataFrame()
    headers["start_datetime"] = st_nd.groupby(st_nd["start_datetime"].dt.year).min()[
        "start_datetime"
    ]
    headers["end_datetime"] = st_nd.groupby(st_nd["end_datetime"].dt.year).max()[
        "end_datetime"
    ]

    headers["year"] = headers["start_datetime"].dt.year.round().astype(int)
    headers["number_of_lanes"] = int(
        df.loc[df[0] == "L0", 2].drop_duplicates().reset_index(drop=True).at[0]
    )

    t21 = df.loc[df[0] == "21"].dropna(axis=1).drop_duplicates().reset_index().copy()
    t21 = t21.iloc[:, (t21.shape[1]) - 9 :].astype(int)
    t21.columns = range(t21.columns.size)
    t21.rename(
        columns={
            0: "speedbin1",
            1: "speedbin2",
            2: "speedbin3",
            3: "speedbin4",
            4: "speedbin5",
            5: "speedbin6",
            6: "speedbin7",
            7: "speedbin8",
            8: "speedbin9",
        },
        inplace=True,
    )

    headers = (
        pd.concat([headers, t21], ignore_index=True, axis=0)
        .bfill()
        .ffill()
        .drop_duplicates()
    )

    try:
        headers["summary_interval_minutes"] = int(
            df.loc[df[0] == "21", 1].drop_duplicates().reset_index(drop=True)[0]
        )
        headers["summary_interval_minutes"] = int(
            df.loc[df[0] == "21", 1].drop_duplicates().reset_index(drop=True)[0]
        )
        headers["summary_interval_minutes"] = int(
            df.loc[df[0] == "21", 1].drop_duplicates().reset_index(drop=True)[0]
        )
    except KeyError:
        pass
    try:
        headers["summary_interval_minutes"] = int(
            df.loc[df[0] == "30", 1].drop_duplicates().reset_index(drop=True)[0]
        )
        headers["summary_interval_minutes"] = int(
            df.loc[df[0] == "30", 1].drop_duplicates().reset_index(drop=True)[0]
        )
        headers["summary_interval_minutes"] = int(
            df.loc[df[0] == "30", 1].drop_duplicates().reset_index(drop=True)[0]
        )
        headers["vehicle_classification_scheme"] = int(
            df.loc[df[0] == "30", 3].drop_duplicates().reset_index(drop=True)[0]
        )
    except KeyError:
        pass
    try:
        headers["summary_interval_minutes"] = int(
            df.loc[df[0] == "70", 1].drop_duplicates().reset_index(drop=True)[0]
        )
        headers["summary_interval_minutes"] = int(
            df.loc[df[0] == "70", 1].drop_duplicates().reset_index(drop=True)[0]
        )
        headers["summary_interval_minutes"] = int(
            df.loc[df[0] == "70", 1].drop_duplicates().reset_index(drop=True)[0]
        )
    except KeyError:
        pass

    try:
        headers["dir1_id"] = int(
            df[df[0] == "L1"]
            .dropna(axis=1)
            .drop_duplicates()
            .reset_index(drop=True)[2]
            .min()
        )
        headers["dir2_id"] = int(
            df[df[0] == "L1"]
            .dropna(axis=1)
            .drop_duplicates()
            .reset_index(drop=True)[2]
            .max()
        )
    except (KeyError, ValueError):
        headers["dir1_id"] = 0
        headers["dir2_id"] = 4

    try:
        headers["vehicle_classification_scheme"] = int(
            df.loc[df[0] == "70", 2].drop_duplicates().reset_index(drop=True)[0]
        )
        headers["maximum_gap_milliseconds"] = int(
            df.loc[df[0] == "70", 3].drop_duplicates().reset_index(drop=True)[0]
        )
        headers["maximum_differential_speed"] = int(
            df.loc[df[0] == "70", 4].drop_duplicates().reset_index(drop=True)[0]
        )
        headers["error_bin_code"] = int(
            df.loc[df[0] == "70", 5].drop_duplicates().reset_index(drop=True)[0]
        )
    except KeyError:
        pass

    headers = headers.reset_index(drop=True).fillna(0)

    headers[headers.select_dtypes(include=[np.number]).columns] = (
        headers[headers.select_dtypes(include=[np.number]).columns]
        .astype(float)
        .round()
        .astype(int)
    )

    headers["document_url"] = file
    headers["header_id"] = header_id
    headers["site_id"] = site_id
    station_name = (
        df.loc[df[0].isin(["S0"]), 3:]
        .reset_index(drop=True)
        .drop_duplicates()
        .dropna(axis=1)
    )
    headers["station_name"] = station_name[station_name.columns].apply(
        lambda row: " ".join(row.values.astype(str)), axis=1
    )

    return headers

import pandas as pd
from pandasql import sqldf
import numpy as np
import config
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError, SQLError
import zipfile
from typing import List
from datetime import timedelta, datetime
import uuid
import csv
import os

#### DATA TOOLS ####
def gui():
    root = Tk().withdraw()
    f = filedialog.askdirectory()
    # root.destroy()
    return str(f)

def is_zip(path: str) -> bool:
    for filename in path:
        return zipfile.is_zipfile(filename)

def getfiles(path: str) -> List[str]:
    print("COLLECTING FILES......")
    src = []
    for root, dirs, files in os.walk(path):
        for name in files:
            if (
                name.endswith(".RSA")
                or name.endswith(".rsa")
                or name.endswith(".rsv")
                or name.endswith(".RSV")
            ):
                p = os.path.join(root, name)
                src.append(p)
    src = list(set(src))
    return src

def to_df(file: str) -> pd.DataFrame:
    df = pd.read_csv(file, header=None, sep='\s+')
    df = df[0].str.split("\s+|,\s+|,", expand=True)
    df = pd.DataFrame(df)
    return df

def get_direction(lane_number, df: pd.DataFrame) -> pd.DataFrame:
    filt = df[1] == lane_number
    df = df.where(filt)
    df = df[2].dropna()
    df = int(df)
    return df

def get_lane_position(lane_number, df: pd.DataFrame) -> pd.DataFrame:
    filt = df[1] == lane_number
    df = df.where(filt)
    df = df[3].dropna()
    df = str(df)
    return df

def join_header_id(d2, header):
    if d2 is None:
        pass
    else:
        data = data_join(d2, header)
        data.drop("station_name", axis=1, inplace=True)
        data["start_datetime"] = data["start_datetime"].astype("datetime64[ns]")
        d2["start_datetime"] = d2["start_datetime"].astype("datetime64[ns]")
        data = data.merge(
            d2, how="outer", on=["site_id", "start_datetime", "lane_number"]
        )
    return data

def data_join(data: pd.DataFrame, header: pd.DataFrame) -> pd.DataFrame:
    if data is None:
        pass
    elif data.empty:
        pass
    else:
        data = pd.DataFrame(data)
        data = join(header, data)
    return data

def join(header: pd.DataFrame, data: pd.DataFrame) -> pd.DataFrame:
    if data.empty:
        df = pd.DataFrame()
    else:
        q = """
        SELECT header.header_id, header.station_name, data.*
        FROM header
        LEFT JOIN data ON data.start_datetime WHERE data.start_datetime >= header.start_datetime AND data.end_datetime <= header.end_datetime;
        """
        q2 = """UPDATE data set header_id = (SELECT header_id from header WHERE data.start_datetime >= header.start_datetime AND data.counttime_end <= header.enddate)"""
        pysqldf = lambda q: sqldf(q, globals())
        df = sqldf(q, locals())
        df = pd.DataFrame(df)
    return df

def postgres_upsert(table, conn, keys, data_iter):
    data = [dict(zip(keys, row)) for row in data_iter]

    insert_statement = insert(table.table).values(data)
    upsert_statement = insert_statement.on_conflict_do_update(
        constraint=f"{table.table.name}_pkey",
        set_={c.key: c for c in insert_statement.excluded},
    )
    conn.execute(upsert_statement)

def push_to_db(df, table, subset) -> None:
    try:
        df.to_sql(
            table,
            con=config.ENGINE,
            schema="trafc",
            if_exists="append",
            index=False,
            method=psql_insert_copy,
        )
    except Exception:
        df = df.drop_duplicates(subset=subset)
        df.to_sql(
            table,
            con=config.ENGINE,
            schema="trafc",
            if_exists="append",
            index=False,
            method=psql_insert_copy,
        )

def create_database_tables():
    conn=config.CONN
    cur = conn.cursor()
    cur.execute(q.CREATE_AXLE_GROU_MASS_GX)
    cur.execute(q.CREATE_AXLE_GROUP_CONFIG_TABLE_CX)
    cur.execute(q.CREATE_AXLE_MASS_TABLE_AX)
    cur.execute(q.CREATE_AXLE_GROUP_MASS_TABLE_GX)
    cur.execute(q.CREATE_AXLE_SPACING_TABLE_SX)
    cur.execute(q.CREATE_IMAGES_TABLE_VX)
    cur.execute(q.CREATE_TYRE_TABLE_TX)
    cur.execute(q.CREATE_WHEEL_MASS_TABLE_WX)
    cur.commit()
    cur.close()

def psql_insert_copy(table, conn, keys, data_iter):
    """
    Execute SQL statement inserting data

    Parameters
    ----------
    table : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list of str
        Column names
    data_iter : Iterable that iterates the values to be inserted
    """
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ", ".join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = "{}.{}".format(table.schema, table.name)
        else:
            table_name = table.name

        sql = "COPY {} ({}) FROM STDIN WITH CSV".format(table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)

#### CREATE DATA FOR TRAFFIC COUNTS ####

def get_head(df) -> pd.DataFrame:
    dfh = pd.DataFrame(
        df.loc[
            (df[0].isin(["H0", "S0", "I0", "S1", "D0", "D1", "D3", "L0", "L1", "L2", "L3","L4" ,"L5","L6","L7","L8","L9","L10","L11","L12"]))
            | (
                (df[0].isin(["21", "70", "30", "13", "60"]))
                & (~df[1].isin(["0", "1", "2", "3", "4"]))
            )
            | (
                (df[0].isin(["10"]))
                & (df[1].isin(["1", "8", "5", "01", "08", "05"]))
            )
        ]
    ).dropna(axis=1, how="all")
    dfh["index"] = dfh.index
    breaks = dfh["index"].diff() != 1
    groups = breaks.cumsum()
    dfh["newindex"] = groups
    dfh = dfh.set_index("newindex")
    dfh = dfh.drop(columns=["index"])
    return dfh

def headers(dfh: pd.DataFrame) -> pd.DataFrame:
    if not dfh.empty:
        headers = pd.DataFrame()
        headers["site_id"] = dfh.loc[dfh[0] == "S0", 1].astype(str)
        if not dfh.loc[dfh[0] == "S1", 1:].empty:
            headers["station_name"] = (
                dfh.loc[dfh[0] == "S1", 1:]
                .dropna(axis=1)
                .apply(" ".join, axis=1)
                .astype(str)
            )
        else:
            headers["station_name"] = (
                dfh.loc[dfh[0] == "S0", 2:]
                .dropna(axis=1)
                .apply(" ".join, axis=1)
                .astype(str)
            )

        try:
            headers["y"] = dfh.loc[dfh[0] == "S0", 5].astype(float)
            headers["x"] = dfh.loc[dfh[0] == "S0", 6].astype(float)
        except Exception:
            pass

        headers["number_of_lanes"] = dfh.loc[dfh[0] == "L0", 2].astype(int)
        headers["primary_direction"] = int(dfh.loc[dfh[0] == 'L1', 2].unique()[0])
        try:
            headers["secondary_direction"] = dfh.loc[dfh[0] == 'L'+ headers["number_of_lanes"].iloc[0].astype(str), 2].astype(int) 
        except:
            headers["secondary_direction"] = int(dfh.loc[dfh[0] == 'L1', 2].unique()[1])
            

        try:
            headers["speedbin1"] = dfh.loc[dfh[0] == "21", 4].astype(int)
            headers["speedbin2"] = dfh.loc[dfh[0] == "21", 5].astype(int)
            headers["speedbin3"] = dfh.loc[dfh[0] == "21", 6].astype(int)
            headers["speedbin4"] = dfh.loc[dfh[0] == "21", 7].astype(int)
            headers["speedbin5"] = dfh.loc[dfh[0] == "21", 8].astype(int)
            headers["speedbin6"] = dfh.loc[dfh[0] == "21", 9].astype(int)
            headers["speedbin7"] = dfh.loc[dfh[0] == "21", 10].astype(int)
            headers["speedbin8"] = dfh.loc[dfh[0] == "21", 11].astype(int)
            headers["speedbin9"] = dfh.loc[dfh[0] == "21", 12].astype(int)
            headers["type_21_count_interval_minutes"] = dfh.loc[
                dfh[0] == "21", 1
            ].astype(int)
            headers["type_21_programmable_rear_to_rear_headway_bin"] = dfh.loc[
                dfh[0] == "21", 3
            ].astype(int)
            headers["type_21_program_id"] = "2"
        except Exception:
            pass
        try:
            headers["type_10_vehicle_classification_scheme_primary"] = dfh.loc[
                dfh[0] == "10", 1
            ].astype(int)
            headers["type_10_vehicle_classification_scheme_secondary"] = dfh.loc[
                dfh[0] == "10", 2
            ].astype(int)
            headers["type_10_maximum_gap_milliseconds"] = dfh.loc[
                dfh[0] == "10", 3
            ].astype(int)
            headers["type_10_maximum_differential_speed"] = dfh.loc[
                dfh[0] == "10", 4
            ].astype(int)
        except Exception:
            pass
        try:
            headers["type_30_summary_interval_minutes"] = dfh.loc[
                dfh[0] == "30", 2
            ].astype(int)
            headers["type_30_vehicle_classification_scheme"] = dfh.loc[
                dfh[0] == "30", 3
            ].astype(int)
        except Exception:
            pass
        try:
            headers["type_70_summary_interval_minutes"] = dfh.loc[
                dfh[0] == "70", 1
            ].astype(int)
            headers["type_70_vehicle_classification_scheme"] = dfh.loc[
                dfh[0] == "70", 2
            ].astype(int)
            headers["type_70_maximum_gap_milliseconds"] = dfh.loc[
                dfh[0] == "70", 3
            ].astype(int)
            headers["type_70_maximum_differential_speed"] = dfh.loc[
                dfh[0] == "70", 4
            ].astype(int)
            headers["type_70_error_bin_code"] = dfh.loc[dfh[0] == "70", 5].astype(
                int
            )
        except Exception:
            pass

        if not dfh.loc[dfh[0] == "D3", 1].empty:
            headers["start_datetime"] = dfh.loc[dfh[0] == "D3", 1].astype(str)
            headers["start_time"] = dfh.loc[dfh[0] == "D3", 2].astype(str)
            headers["end_datetime"] = dfh.loc[dfh[0] == "D3", 3].astype(str)
            headers["end_time"] = dfh.loc[dfh[0] == "D3", 4].astype(str)
        else:
            headers["start_datetime"] = dfh.loc[dfh[0] == "D1", 1].astype(str)
            headers["start_time"] = dfh.loc[dfh[0] == "D1", 2].astype(str)
            headers["end_datetime"] = dfh.loc[dfh[0] == "D1", 3].astype(str)
            headers["end_time"] = dfh.loc[dfh[0] == "D1", 4].astype(str)

        try:

            for idx, row in headers.iterrows():
                if len(row['start_datetime']) == 6 and len(row['start_time']) == 6:
                    row['start_datetime'] = pd.to_datetime(row['start_datetime'] + row['start_time'], format='%y%m%d%H%M%S')
                elif len(row['start_datetime']) == 8 and len(row['start_time']) == 6:
                    row['start_datetime'] = pd.to_datetime(row['start_datetime'] + row['start_time'], format='%Y%m%d%H%M%S')
                elif len(row['start_datetime']) == 6 and len(row['start_time']) == 8:
                    row['start_datetime'] = pd.to_datetime(row['start_datetime'] + row['start_time'], format='%y%m%d%H%M%S%f')
                elif len(row['start_datetime']) == 8 and len(row['start_time']) == 8:
                    row['start_datetime'] = pd.to_datetime(row['start_datetime'] + row['start_time'], format='%Y%m%d%H%M%S%f')

                if len(row['end_datetime']) == 6 and row['end_time'].strip('0') == '24':
                    row['end_datetime'] = pd.to_datetime(row['end_datetime'], format='%y%m%d')+timedelta(days=1)
                elif len(row['end_datetime']) ==8 and row['end_time'].strip('0') == '24':
                    row['end_datetime'] = pd.to_datetime(row['end_datetime'], format='%Y%m%d')+timedelta(days=1)
                elif len(row['end_datetime']) == 6 and len(row['end_time']) == 6:
                    row['start_datetime'] = pd.to_datetime(row['end_datetime'] + row['end_time'], format='%y%m%d%H%M%S')
                elif len(row['end_datetime']) == 8 and len(row['end_time']) == 6:
                    row['start_datetime'] = pd.to_datetime(row['end_datetime'] + row['end_time'], format='%Y%m%d%H%M%S')
                elif len(row['end_datetime']) == 6 and len(row['end_time']) == 8:
                    row['start_datetime'] = pd.to_datetime(row['end_datetime'] + row['end_time'], format='%y%m%d%H%M%S%f')
                elif len(row['end_datetime']) == 8 and len(row['end_time']) == 8:
                    row['start_datetime'] = pd.to_datetime(row['end_datetime'] + row['end_time'], format='%Y%m%d%H%M%S%f')

        except:
        
            headers["end_datetime"] = headers.apply(
                lambda x: pd.to_datetime(
                    x["end_datetime"] + x["end_time"], format="%y%m%d%H%M%S"
                )
                if (
                    x["end_time"] != "240000"
                    and len(x["end_datetime"]) == 6
                    and len(x["end_time"]) == 6
                )
                else (
                    pd.to_datetime(
                        x["end_datetime"] + x["end_time"], format="%y%m%d%H%M%S%f"
                    )
                    if (
                        x["end_time"] != "24000000"
                        and len(x["end_datetime"]) == 6
                        and len(x["end_time"]) == 8
                    )
                    else (
                        pd.to_datetime(
                            x["end_datetime"] + x["end_time"], format="%Y%m%d%H%M%S"
                        )
                        if (
                            x["end_time"] != "240000"
                            and len(x["end_datetime"]) == 8
                            and len(x["end_time"]) == 6
                        )
                        else (
                            pd.to_datetime(
                                x["end_datetime"] + x["end_time"],
                                format="%Y%m%d%H%M%S%f",
                            )
                            if (
                                x["end_time"] != "24000000"
                                and len(x["end_datetime"]) == 8
                                and len(x["end_time"]) == 8
                            )
                            else (
                                pd.to_datetime(x["end_datetime"], format="%y%m%d")
                                + timedelta(days=1)
                                if (
                                    x["end_time"] == "240000"
                                    and len(x["end_datetime"]) == 6
                                    and len(x["end_time"]) == 6
                                )
                                else (
                                    pd.to_datetime(x["end_datetime"], format="%y%m%d")
                                    + timedelta(days=1)
                                    if (
                                        x["end_time"] == "24000000"
                                        and len(x["end_datetime"]) == 6
                                        and len(x["end_time"]) == 8
                                    )
                                    else (
                                        pd.to_datetime(
                                            x["end_datetime"], format="%Y%m%d"
                                        )
                                        + timedelta(days=1)
                                        if (
                                            x["end_time"] == "240000"
                                            and len(x["end_datetime"]) == 8
                                            and len(x["end_time"]) == 6
                                        )
                                        else (
                                            pd.to_datetime(
                                                x["end_datetime"], format="%Y%m%d"
                                            )
                                            + timedelta(days=1)
                                            if (
                                                x["end_time"] == "24000000"
                                                and len(x["end_datetime"]) == 8
                                                and len(x["end_time"]) == 8
                                            )
                                            else (pd.to_datetime(
                                            x["end_datetime"] + x["end_time"], format="%y%m%d%H%M%S%f"
                                            if (len(x["end_datetime"]) == 6 
                                                and len(x["end_time"]) == 7)
                                                else pd.to_datetime(
                                                    x["end_datetime"] + x["end_time"]
                                                ))
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                ),
                axis=1,
            )

            headers["start_datetime"] = headers.apply(
            lambda x: pd.to_datetime(
                x["start_datetime"] + x["start_time"], format="%y%m%d%H%M%S"
            )
            if (
                x["start_time"] != "240000"
                and len(x["start_datetime"]) == 6
                and len(x["start_time"]) == 6
            )
            else (
                pd.to_datetime(
                    x["start_datetime"] + x["start_time"], format="%y%m%d%H%M%S%f"
                )
                if (
                    x["start_time"] != "24000000"
                    and len(x["start_datetime"]) == 6
                    and len(x["start_time"]) == 8
                )
                else (
                    pd.to_datetime(
                        x["start_datetime"] + x["start_time"], format="%Y%m%d%H%M%S"
                    )
                    if (
                        x["start_time"] != "240000"
                        and len(x["start_datetime"]) == 8
                        and len(x["start_time"]) == 6
                    )
                    else (
                        pd.to_datetime(
                            x["start_datetime"] + x["start_time"],
                            format="%Y%m%d%H%M%S%f",
                        )
                        if (
                            x["start_time"] != "24000000"
                            and len(x["start_datetime"]) == 8
                            and len(x["start_time"]) == 8
                        )
                        else (
                            pd.to_datetime(x["start_datetime"], format="%y%m%d")
                            + timedelta(days=1)
                            if (
                                x["start_time"] == "240000"
                                and len(x["start_datetime"]) == 6
                                and len(x["start_time"]) == 6
                            )
                            else (
                                pd.to_datetime(x["start_datetime"], format="%y%m%d")
                                + timedelta(days=1)
                                if (
                                    x["start_time"] == "24000000"
                                    and len(x["start_datetime"]) == 6
                                    and len(x["start_time"]) == 8
                                )
                                else (
                                    pd.to_datetime(
                                        x["start_datetime"], format="%Y%m%d"
                                    )
                                    + timedelta(days=1)
                                    if (
                                        x["start_time"] == "240000"
                                        and len(x["start_datetime"]) == 8
                                        and len(x["start_time"]) == 6
                                    )
                                    else (
                                        pd.to_datetime(
                                            x["start_datetime"], format="%Y%m%d"
                                        )
                                        + timedelta(days=1)
                                        if (
                                            x["start_time"] == "24000000"
                                            and len(x["start_datetime"]) == 8
                                            and len(x["start_time"]) == 8
                                        )
                                        else (
                                        pd.to_datetime(
                                            x["start_datetime"] + x["start_time"], format="%y%m%d%H%M%S%f"
                                        )
                                        if (
                                            len(x["start_datetime"]) == 6
                                            and len(x["start_time"]) == 7
                                        )
                                            else pd.to_datetime(
                                                x["start_datetime"] + x["start_time"]
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
            ),
            axis=1,
        )

        headers = headers.drop(["start_time"], axis=1)
        headers = headers.drop(["end_time"], axis=1)

        headers["start_datetime"] = pd.to_datetime(headers["start_datetime"])
        headers["end_datetime"] = pd.to_datetime(headers["end_datetime"])
        headers["site_id"] = headers["site_id"].astype(str)

        try:
            headers["instrumentation_description"] = (
                dfh.loc[dfh[0] == "I0", 1:]
                .dropna(axis=1)
                .apply(" ".join, axis=1)
                .astype(str)
            )
        except Exception:
            headers["instrumentation_description"] = None

        try:
            headers["type_30_summary_interval_minutes"] = headers[
                "type_21_count_interval_minutes"
            ]
        except Exception:
            pass
        try:
            headers["type_70_summary_interval_minutes"] = headers[
                "type_21_count_interval_minutes"
            ]
            headers["type_70_vehicle_classification_scheme"] = headers[
                "type_21_count_interval_minutes"
            ]
            headers["type_70_vehicle_classification_scheme"] = headers[
                "type_21_count_interval_minutes"
            ]
        except Exception:
            pass

        headers = headers.fillna(method="ffill")
        headers = headers.fillna(method="bfill")

        headers = headers.drop_duplicates(ignore_index=True)

        headers["header_id"] = ""
        headers["header_id"] = headers["header_id"].apply(
            lambda x: str(uuid.uuid4())
        )

    else:
        pass
    return headers

def lanes(dfh: pd.DataFrame) -> pd.DataFrame:
    if not dfh.empty:
        lanes_df = dfh.loc[
            (dfh[0].isin(["L0", "L1", "L2", "L3","L4" ,"L5","L6","L7","L8","L9","L10","L11","L12"]))
        ]
    else:
        pass
    return lanes_df


#### CREATE DATA FOR TRAFFIC COUNTS ####

def dtype21(df: pd.DataFrame) -> pd.DataFrame:
    data = df.loc[(df[0] == "21") & (df[1].isin(["0", "1", "2", "3", "4"]))].dropna(
        axis=1, how="all"
    )
    dfh2 = pd.DataFrame(df.loc[(df[0].isin(["S0", "L1"]))]).dropna(
        axis=1, how="all"
    )

    if data.empty:
        pass
    else:
        if (data[1] == "0").any():
            ddf = data.iloc[:, 2:]
            ddf = pd.DataFrame(ddf).dropna(axis=1, how="all")
            ddf.columns = [
                "end_datetime",
                "end_time",
                "duration_min",
                "lane_number",
                "speedbin1",
                "speedbin2",
                "speedbin3",
                "speedbin4",
                "speedbin5",
                "speedbin6",
                "speedbin7",
                "speedbin8",
                "speedbin9",
                "speedbin10",
                "sum_of_heavy_vehicle_speeds",
                "short_heavy_vehicles",
                "medium_heavy_vehicles",
                "long_heavy_vehicles",
                "rear_to_rear_headway_shorter_than_2_seconds",
                "rear_to_rear_headways_shorter_than_programmed_time",
            ]
            ddf["speedbin0"] = 0

        elif (data[1] == "1").any():
            ddf = data.iloc[:, 3:]
            ddf = pd.DataFrame(ddf).dropna(axis=1, how="all")
            ddf.columns = [
                "end_datetime",
                "end_time",
                "duration_min",
                "lane_number",
                "speedbin0",
                "speedbin1",
                "speedbin2",
                "speedbin3",
                "speedbin4",
                "speedbin5",
                "speedbin6",
                "speedbin7",
                "speedbin8",
                "speedbin9",
                "speedbin10",
                "sum_of_heavy_vehicle_speeds",
                "short_heavy_vehicles",
                "medium_heavy_vehicles",
                "long_heavy_vehicles",
                "rear_to_rear_headway_shorter_than_2_seconds",
                "rear_to_rear_headways_shorter_than_programmed_time",
            ]

        ddf = ddf.fillna(0)
        ddf["duration_min"] = ddf["duration_min"].astype(int)
        ddf["lane_number"] = ddf["lane_number"].astype(int)
        ddf["speedbin0"] = ddf["speedbin0"].astype(int)
        ddf["speedbin1"] = ddf["speedbin1"].astype(int)
        ddf["speedbin2"] = ddf["speedbin2"].astype(int)
        ddf["speedbin3"] = ddf["speedbin3"].astype(int)
        ddf["speedbin4"] = ddf["speedbin4"].astype(int)
        ddf["speedbin5"] = ddf["speedbin5"].astype(int)
        ddf["speedbin6"] = ddf["speedbin6"].astype(int)
        ddf["speedbin7"] = ddf["speedbin7"].astype(int)
        ddf["speedbin8"] = ddf["speedbin8"].astype(int)
        ddf["speedbin9"] = ddf["speedbin9"].astype(int)
        ddf["speedbin10"] = ddf["speedbin10"].astype(int)
        ddf["sum_of_heavy_vehicle_speeds"] = ddf[
            "sum_of_heavy_vehicle_speeds"
        ].astype(int)
        ddf["short_heavy_vehicles"] = ddf["short_heavy_vehicles"].astype(int)
        ddf["medium_heavy_vehicles"] = ddf["medium_heavy_vehicles"].astype(int)
        ddf["long_heavy_vehicles"] = ddf["long_heavy_vehicles"].astype(int)
        ddf["rear_to_rear_headway_shorter_than_2_seconds"] = ddf[
            "rear_to_rear_headway_shorter_than_2_seconds"
        ].astype(int)
        ddf["rear_to_rear_headways_shorter_than_programmed_time"] = ddf[
            "rear_to_rear_headways_shorter_than_programmed_time"
        ].astype(int)

        ddf["total_heavy_vehicles_type21"] = (
            ddf["short_heavy_vehicles"]
            + ddf["medium_heavy_vehicles"]
            + ddf["long_heavy_vehicles"]
        )
        ddf["total_heavy_vehicles_type21"] = ddf[
            "total_heavy_vehicles_type21"
        ].astype(int)

        ddf["total_light_vehicles_type21"] = (
            ddf["speedbin1"]
            + ddf["speedbin2"]
            + ddf["speedbin3"]
            + ddf["speedbin4"]
            + ddf["speedbin5"]
            + ddf["speedbin6"]
            + ddf["speedbin7"]
            + ddf["speedbin8"]
            + ddf["speedbin9"]
            + ddf["speedbin10"]
            - ddf["short_heavy_vehicles"]
            - ddf["medium_heavy_vehicles"]
            - ddf["long_heavy_vehicles"]
        )
        ddf["total_light_vehicles_type21"] = ddf[
            "total_light_vehicles_type21"
        ].astype(int)

        ddf["total_vehicles_type21"] = (
            ddf["speedbin1"]
            + ddf["speedbin2"]
            + ddf["speedbin3"]
            + ddf["speedbin4"]
            + ddf["speedbin5"]
            + ddf["speedbin6"]
            + ddf["speedbin7"]
            + ddf["speedbin8"]
            + ddf["speedbin9"]
            + ddf["speedbin10"]
        )
        ddf["total_vehicles_type21"] = ddf["total_vehicles_type21"].astype(int)

        max_lanes = ddf["lane_number"].max()
        ddf["direction"] = ddf.apply(
            lambda x: "P" if x["lane_number"] <= (int(max_lanes) / 2) else "N",
            axis=1,
        )

        direction = dfh2.loc[dfh2[0] == "L1", 1:3].astype(int)
        direction = direction.drop_duplicates()
        try:
            ddf["forward_direction_code"] = ddf.apply(
                lambda x: Data.get_direction(x["lane_number"], direction), axis=1
            )
            # FIXME: ddf['lane_position_code']=ddf.apply(lambda x: Data.get_lane_position(x['lane_number'],direction),axis=1)
        except Exception:
            ddf["forward_direction_code"] = None
            # ddf['lane_position_code']=None

        ddf["end_datetime"] = ddf.apply(
            lambda x: pd.to_datetime(
                x["end_datetime"] + x["end_time"], format="%y%m%d%H%M"
            )
            if (
                x["end_time"] != "2400"
                and len(str(x["end_datetime"])) == 6
                and len(x["end_time"]) == 4
            )
            else (
                pd.to_datetime(
                    x["end_datetime"] + x["end_time"], format="%Y%m%d%H%M"
                )
                if (
                    x["end_time"] != "2400"
                    and len(str(x["end_datetime"])) == 8
                    and len(x["end_time"]) == 4
                )
                else (
                    pd.to_datetime(x["end_datetime"], format="%y%m%d")
                    + timedelta(days=1)
                    if (
                        x["end_time"] == "2400"
                        and len(str(x["end_datetime"])) == 6
                        and len(x["end_time"]) == 4
                    )
                    else (
                        pd.to_datetime(x["end_datetime"], format="%Y%m%d")
                        + timedelta(days=1)
                        if (
                            x["end_time"] != "2400"
                            and len(str(x["end_datetime"])) == 8
                            and len(x["end_time"]) == 4
                        )
                        else pd.to_datetime(x["end_datetime"])
                    )
                )
            ),
            axis=1,
        )
        ddf = ddf.drop(["end_time"], axis=1)
        ddf["start_datetime"] = pd.to_datetime(
            ddf["end_datetime"]
        ) - pd.to_timedelta(ddf["duration_min"].astype(int), unit="m")
        # ddf['year'] = ddf['start_datetime'].dt.year
        t1 = dfh2.loc[dfh2[0] == "S0", 1].unique()
        ddf["site_id"] = str(t1[0])

        ddf = ddf.drop_duplicates()
        ddf["start_datetime"] = ddf["start_datetime"].astype("datetime64[ns]")

        return ddf

def dtype30(df: pd.DataFrame) -> pd.DataFrame:
    data = df.loc[(df[0] == "30") & (df[1].isin(["0", "1", "2", "3", "4"]))].dropna(
        axis=1, how="all"
    )
    dfh2 = pd.DataFrame(df.loc[(df[0].isin(["S0", "L1"]))]).dropna(
        axis=1, how="all"
    )
    if data.empty:
        pass
    else:
        if data[1].all() == "1":
            ddf = data.iloc[:, 3:]
            ddf = pd.DataFrame(ddf).dropna(axis=1, how="all")
            ddf.columns = [
                "end_datetime",
                "end_time",
                "duration_min",
                "lane_number",
                "unknown_vehicle_error_class",
                "motorcycle",
                "light_motor_vehicles",
                "light_motor_vehicles_towing",
                "two_axle_busses",
                "two_axle_6_tyre_single_units",
                "busses_with_3_or_4_axles",
                "two_axle_6_tyre_single_unit_with_light_trailer_4_axles_max",
                "three_axle_single_unit_including_single_axle_light_trailer",
                "four_or_less_axle_including_a_single_trailer",
                "buses_with_5_or_more_axles",
                "three_axle_single_unit_and_light_trailer_more_than_4_axles",
                "five_axle_single_trailer",
                "six_axle_single_trailer",
                "five_or_less_axle_multi_trailer",
                "six_axle_multi_trailer",
                "seven_axle_multi_trailer",
                "eight_or_more_axle_multi_trailer",
            ]
            ddf["heavy_vehicle"] = 0

        elif data[1].isin(["0", "2"]).any():
            ddf = data.iloc[:, 2:]
            ddf = pd.DataFrame(ddf).dropna(axis=1, how="all")
            if ddf.shape[1] == 7:
                ddf.columns = [
                    "end_datetime",
                    "end_time",
                    "duration_min",
                    "lane_number",
                    "unknown_vehicle_error_class",
                    "light_motor_vehicles",
                    "heavy_vehicle",
                ]
                ddf = pd.concat(
                    [
                        ddf,
                        pd.DataFrame(
                            columns=[
                                "motorcycle",
                                "light_motor_vehicles",
                                "light_motor_vehicles_towing",
                                "two_axle_busses",
                                "two_axle_6_tyre_single_units",
                                "busses_with_3_or_4_axles",
                                "two_axle_6_tyre_single_unit_with_light_trailer_4_axles_max",
                                "three_axle_single_unit_including_single_axle_light_trailer",
                                "four_or_less_axle_including_a_single_trailer",
                                "buses_with_5_or_more_axles",
                                "three_axle_single_unit_and_light_trailer_more_than_4_axles",
                                "five_axle_single_trailer",
                                "six_axle_single_trailer",
                                "five_or_less_axle_multi_trailer",
                                "six_axle_multi_trailer",
                                "seven_axle_multi_trailer",
                                "eight_or_more_axle_multi_trailer",
                            ]
                        ),
                    ]
                )
            else:
                ddf.columns = [
                    "end_datetime",
                    "end_time",
                    "duration_min",
                    "lane_number",
                    "unknown_vehicle_error_class",
                    "motorcycle",
                    "light_motor_vehicles",
                    "light_motor_vehicles_towing",
                    "two_axle_busses",
                    "two_axle_6_tyre_single_units",
                    "busses_with_3_or_4_axles",
                    "two_axle_6_tyre_single_unit_with_light_trailer_4_axles_max",
                    "three_axle_single_unit_including_single_axle_light_trailer",
                    "four_or_less_axle_including_a_single_trailer",
                    "buses_with_5_or_more_axles",
                    "three_axle_single_unit_and_light_trailer_more_than_4_axles",
                    "five_axle_single_trailer",
                    "six_axle_single_trailer",
                    "five_or_less_axle_multi_trailer",
                    "six_axle_multi_trailer",
                    "seven_axle_multi_trailer",
                    "eight_or_more_axle_multi_trailer",
                ]

        ddf = ddf.dropna(
            axis=0,
            how="all",
            subset=[
                "unknown_vehicle_error_class",
                "light_motor_vehicles",
                "motorcycle",
                "light_motor_vehicles",
                "light_motor_vehicles_towing",
                "two_axle_busses",
                "two_axle_6_tyre_single_units",
                "busses_with_3_or_4_axles",
                "two_axle_6_tyre_single_unit_with_light_trailer_4_axles_max",
                "three_axle_single_unit_including_single_axle_light_trailer",
                "four_or_less_axle_including_a_single_trailer",
                "buses_with_5_or_more_axles",
                "three_axle_single_unit_and_light_trailer_more_than_4_axles",
                "five_axle_single_trailer",
                "six_axle_single_trailer",
                "five_or_less_axle_multi_trailer",
                "six_axle_multi_trailer",
                "seven_axle_multi_trailer",
                "eight_or_more_axle_multi_trailer",
            ],
        )

        ddf=ddf.fillna(0)
        ddf["duration_min"] = ddf["duration_min"].astype(int)
        ddf["lane_number"] = ddf["lane_number"].astype(int)
        ddf["unknown_vehicle_error_class"] = ddf[
            "unknown_vehicle_error_class"
        ].astype(int)
        ddf["light_motor_vehicles"] = ddf["light_motor_vehicles"].astype(int)
        ddf["heavy_vehicle"] = ddf["heavy_vehicle"].astype(int)
        ddf["light_motor_vehicles_towing"] = ddf[
            "light_motor_vehicles_towing"
        ].astype(int)
        ddf["two_axle_busses"] = ddf["two_axle_busses"].astype(int)
        ddf["two_axle_6_tyre_single_units"] = ddf[
            "two_axle_6_tyre_single_units"
        ].astype(int)
        ddf["busses_with_3_or_4_axles"] = ddf["busses_with_3_or_4_axles"].astype(
            int
        )
        ddf["two_axle_6_tyre_single_unit_with_light_trailer_4_axles_max"] = ddf[
            "two_axle_6_tyre_single_unit_with_light_trailer_4_axles_max"
        ].astype(int)
        ddf["three_axle_single_unit_including_single_axle_light_trailer"] = ddf[
            "three_axle_single_unit_including_single_axle_light_trailer"
        ].astype(int)
        ddf["four_or_less_axle_including_a_single_trailer"] = ddf[
            "four_or_less_axle_including_a_single_trailer"
        ].astype(int)
        ddf["buses_with_5_or_more_axles"] = ddf[
            "buses_with_5_or_more_axles"
        ].astype(int)
        ddf["three_axle_single_unit_and_light_trailer_more_than_4_axles"] = ddf[
            "three_axle_single_unit_and_light_trailer_more_than_4_axles"
        ].astype(int)
        ddf["five_axle_single_trailer"] = ddf["five_axle_single_trailer"].astype(
            int
        )
        ddf["six_axle_single_trailer"] = ddf["six_axle_single_trailer"].astype(int)
        ddf["five_or_less_axle_multi_trailer"] = ddf[
            "five_or_less_axle_multi_trailer"
        ].astype(int)
        ddf["six_axle_multi_trailer"] = ddf["six_axle_multi_trailer"].astype(int)
        ddf["seven_axle_multi_trailer"] = ddf["seven_axle_multi_trailer"].astype(
            int
        )
        ddf["eight_or_more_axle_multi_trailer"] = ddf[
            "eight_or_more_axle_multi_trailer"
        ].astype(int)

        ddf["total_light_vehicles_type30"] = (
            ddf["motorcycle"].astype(int)
            + ddf["light_motor_vehicles"].astype(int)
            + ddf["light_motor_vehicles_towing"].astype(int)
        )
        ddf["total_heavy_vehicles_type30"] = (
            ddf["two_axle_busses"].astype(int)
            + ddf["two_axle_6_tyre_single_units"].astype(int)
            + ddf["busses_with_3_or_4_axles"].astype(int)
            + ddf[
                "two_axle_6_tyre_single_unit_with_light_trailer_4_axles_max"
            ].astype(int)
            + ddf[
                "three_axle_single_unit_including_single_axle_light_trailer"
            ].astype(int)
            + ddf["four_or_less_axle_including_a_single_trailer"].astype(int)
            + ddf["buses_with_5_or_more_axles"].astype(int)
            + ddf[
                "three_axle_single_unit_and_light_trailer_more_than_4_axles"
            ].astype(int)
            + ddf["five_axle_single_trailer"].astype(int)
            + ddf["six_axle_single_trailer"].astype(int)
            + ddf["five_or_less_axle_multi_trailer"].astype(int)
            + ddf["six_axle_multi_trailer"].astype(int)
            + ddf["seven_axle_multi_trailer"].astype(int)
            + ddf["eight_or_more_axle_multi_trailer"].astype(int)
        )

        ddf["total_vehicles_type30"] = (
            ddf["motorcycle"].astype(int)
            + ddf["light_motor_vehicles"].astype(int)
            + ddf["light_motor_vehicles_towing"].astype(int)
            + ddf["two_axle_busses"].astype(int)
            + ddf["two_axle_6_tyre_single_units"].astype(int)
            + ddf["busses_with_3_or_4_axles"].astype(int)
            + ddf[
                "two_axle_6_tyre_single_unit_with_light_trailer_4_axles_max"
            ].astype(int)
            + ddf[
                "three_axle_single_unit_including_single_axle_light_trailer"
            ].astype(int)
            + ddf["four_or_less_axle_including_a_single_trailer"].astype(int)
            + ddf["buses_with_5_or_more_axles"].astype(int)
            + ddf[
                "three_axle_single_unit_and_light_trailer_more_than_4_axles"
            ].astype(int)
            + ddf["five_axle_single_trailer"].astype(int)
            + ddf["six_axle_single_trailer"].astype(int)
            + ddf["five_or_less_axle_multi_trailer"].astype(int)
            + ddf["six_axle_multi_trailer"].astype(int)
            + ddf["seven_axle_multi_trailer"].astype(int)
            + ddf["eight_or_more_axle_multi_trailer"].astype(int)
        )

        max_lanes = ddf["lane_number"].max()
        ddf["direction"] = ddf.apply(
            lambda x: "P" if x["lane_number"] <= (int(max_lanes) / 2) else "N",
            axis=1,
        )
        direction = dfh2.loc[dfh2[0] == "L1", 1:3].astype(int)
        direction = direction.drop_duplicates()
        try:
            ddf["forward_direction_code"] = ddf.apply(
                lambda x: Data.get_direction(x["lane_number"], direction), axis=1
            )
            # FIXME: ddf['lane_position_code']=ddf.apply(lambda x: Data.get_lane_position(x['lane_number'],direction),axis=1)
        except Exception:
            ddf["forward_direction_code"] = None
            # ddf['lane_position_code']=None

        ddf["end_datetime"] = ddf.apply(
            lambda x: pd.to_datetime(
                x["end_datetime"] + x["end_time"], format="%y%m%d%H%M"
            )
            if (
                x["end_time"] != "2400"
                and len(str(x["end_datetime"])) == 6
                and len(x["end_time"]) == 4
            )
            else (
                pd.to_datetime(
                    x["end_datetime"] + x["end_time"], format="%Y%m%d%H%M"
                )
                if (
                    x["end_time"] != "2400"
                    and len(str(x["end_datetime"])) == 8
                    and len(x["end_time"]) == 4
                )
                else (
                    pd.to_datetime(x["end_datetime"], format="%y%m%d")
                    + timedelta(days=1)
                    if (
                        x["end_time"] == "2400"
                        and len(str(x["end_datetime"])) == 6
                        and len(x["end_time"]) == 4
                    )
                    else (
                        pd.to_datetime(x["end_datetime"], format="%Y%m%d")
                        + timedelta(days=1)
                        if (
                            x["end_time"] != "2400"
                            and len(str(x["end_datetime"])) == 8
                            and len(x["end_time"]) == 4
                        )
                        else pd.to_datetime(x["end_datetime"])
                    )
                )
            ),
            axis=1,
        )

        ddf = ddf.drop(["end_time"], axis=1)

        ddf["start_datetime"] = pd.to_datetime(
            ddf["end_datetime"]
        ) - pd.to_timedelta(ddf["duration_min"].astype(int), unit="m")
        # ddf['year'] = ddf['start_datetime'].dt.year
        t1 = dfh2.loc[dfh2[0] == "S0", 1].unique()
        ddf["site_id"] = str(t1[0])

        ddf = ddf.drop_duplicates()
        ddf["start_datetime"] = ddf["start_datetime"].astype("datetime64[ns]")

        return ddf

def dtype70(df: pd.DataFrame) -> pd.DataFrame:
    data = df.loc[(df[0] == "70") & (df[1].isin(["0", "1", "2", "3", "4"]))].dropna(
        axis=1, how="all"
    )
    dfh2 = pd.DataFrame(df.loc[(df[0].isin(["S0", "L1"]))]).dropna(
        axis=1, how="all"
    )
    if data.empty:
        pass
    else:
        if data[1].all() == "1":
            ddf = data.iloc[:, 3:]
            ddf = pd.DataFrame(ddf).dropna(axis=1, how="all")
            ddf.columns = [
                "end_datetime",
                "end_time",
                "duration_min",
                "lane_number",
                "number_of_error_vehicles",
                "total_free_flowing_light_vehicles",
                "total_following_light_vehicles",
                "total_free_flowing_heavy_vehicles",
                "total_following_heavy_vehicles",
                "sum_of_inverse_of_speeds_for_free_flowing_lights",
                "sum_of_inverse_of_speeds_for_following_lights",
                "sum_of_inverse_of_speeds_for_free_flowing_heavies",
                "sum_of_inverse_of_speeds_for_following_heavies",
                "sum_of_speeds_for_free_flowing_lights",
                "sum_of_speeds_for_following_lights",
                "sum_of_speeds_for_free_flowing_heavies",
                "sum_of_speeds_for_following_heavies",
                "sum_of_squared_speeds_of_free_flowing_lights",
                "sum_of_squared_speeds_for_following_lights",
                "sum_of_squared_speeds_of_free_flowing_heavies",
                "sum_of_squared_speeds_for_following_heavies",
            ]

        else:  # data[1].all() == '0':
            ddf = data.iloc[:, 2:]
            ddf = pd.DataFrame(ddf).dropna(axis=1, how="all")
            ddf.columns = [
                "end_datetime",
                "end_time",
                "duration_min",
                "lane_number",
                # 			 'number_of_error_vehicles',
                "total_free_flowing_light_vehicles",
                "total_following_light_vehicles",
                "total_free_flowing_heavy_vehicles",
                "total_following_heavy_vehicles",
                "sum_of_inverse_of_speeds_for_free_flowing_lights",
                "sum_of_inverse_of_speeds_for_following_lights",
                "sum_of_inverse_of_speeds_for_free_flowing_heavies",
                "sum_of_inverse_of_speeds_for_following_heavies",
                "sum_of_speeds_for_free_flowing_lights",
                "sum_of_speeds_for_following_lights",
                "sum_of_speeds_for_free_flowing_heavies",
                "sum_of_speeds_for_following_heavies",
                "sum_of_squared_speeds_of_free_flowing_lights",
                "sum_of_squared_speeds_for_following_lights",
                "sum_of_squared_speeds_of_free_flowing_heavies",
                "sum_of_squared_speeds_for_following_heavies",
            ]
            ddf["number_of_error_vehicles"] = 0

        ddf = ddf.fillna(0)
        ddf["duration_min"] = ddf["duration_min"].astype(int)
        ddf["lane_number"] = ddf["lane_number"].astype(int)
        ddf["number_of_error_vehicles"] = ddf["number_of_error_vehicles"].astype(
            int
        )
        ddf["total_free_flowing_light_vehicles"] = ddf[
            "total_free_flowing_light_vehicles"
        ].astype(int)
        ddf["total_following_light_vehicles"] = ddf[
            "total_following_light_vehicles"
        ].astype(int)
        ddf["total_free_flowing_heavy_vehicles"] = ddf[
            "total_free_flowing_heavy_vehicles"
        ].astype(int)
        ddf["total_following_heavy_vehicles"] = ddf[
            "total_following_heavy_vehicles"
        ].astype(int)
        ddf["sum_of_inverse_of_speeds_for_free_flowing_lights"] = ddf[
            "sum_of_inverse_of_speeds_for_free_flowing_lights"
        ].astype(int)
        ddf["sum_of_inverse_of_speeds_for_following_lights"] = ddf[
            "sum_of_inverse_of_speeds_for_following_lights"
        ].astype(int)
        ddf["sum_of_inverse_of_speeds_for_free_flowing_heavies"] = ddf[
            "sum_of_inverse_of_speeds_for_free_flowing_heavies"
        ].astype(int)
        ddf["sum_of_inverse_of_speeds_for_following_heavies"] = ddf[
            "sum_of_inverse_of_speeds_for_following_heavies"
        ].astype(int)
        ddf["sum_of_speeds_for_free_flowing_lights"] = ddf[
            "sum_of_speeds_for_free_flowing_lights"
        ].astype(int)
        ddf["sum_of_speeds_for_following_lights"] = ddf[
            "sum_of_speeds_for_following_lights"
        ].astype(int)
        ddf["sum_of_speeds_for_free_flowing_heavies"] = ddf[
            "sum_of_speeds_for_free_flowing_heavies"
        ].astype(int)
        ddf["sum_of_speeds_for_following_heavies"] = ddf[
            "sum_of_speeds_for_following_heavies"
        ].astype(int)
        ddf["sum_of_squared_speeds_of_free_flowing_lights"] = ddf[
            "sum_of_squared_speeds_of_free_flowing_lights"
        ].astype(int)
        ddf["sum_of_squared_speeds_for_following_lights"] = ddf[
            "sum_of_squared_speeds_for_following_lights"
        ].astype(int)
        ddf["sum_of_squared_speeds_of_free_flowing_heavies"] = ddf[
            "sum_of_squared_speeds_of_free_flowing_heavies"
        ].astype(int)
        ddf["sum_of_squared_speeds_for_following_heavies"] = ddf[
            "sum_of_squared_speeds_for_following_heavies"
        ].astype(int)

        ddf["lane_number"] = ddf["lane_number"].astype(int)
        max_lanes = ddf["lane_number"].max()
        ddf["direction"] = ddf.apply(
            lambda x: "P" if x["lane_number"] <= (int(max_lanes) / 2) else "N",
            axis=1,
        )
        direction = dfh2.loc[dfh2[0] == "L1", 1:3].astype(int)
        direction = direction.drop_duplicates()
        try:
            ddf["forward_direction_code"] = ddf.apply(
                lambda x: Data.get_direction(x["lane_number"], direction), axis=1
            )
            # FIXME: ddf['lane_position_code']=ddf.apply(lambda x: Data.get_lane_position(x['lane_number'],direction),axis=1)
        except Exception:
            ddf["forward_direction_code"] = None
            # ddf['lane_position_code']=None

        ddf["end_datetime"] = ddf.apply(
            lambda x: pd.to_datetime(
                x["end_datetime"] + x["end_time"], format="%y%m%d%H%M"
            )
            if (
                x["end_time"] != "2400"
                and len(str(x["end_datetime"])) == 6
                and len(x["end_time"]) == 4
            )
            else (
                pd.to_datetime(
                    x["end_datetime"] + x["end_time"], format="%Y%m%d%H%M"
                )
                if (
                    x["end_time"] != "2400"
                    and len(str(x["end_datetime"])) == 8
                    and len(x["end_time"]) == 4
                )
                else (
                    pd.to_datetime(x["end_datetime"], format="%y%m%d")
                    + timedelta(days=1)
                    if (
                        x["end_time"] == "2400"
                        and len(str(x["end_datetime"])) == 6
                        and len(x["end_time"]) == 4
                    )
                    else (
                        pd.to_datetime(x["end_datetime"], format="%Y%m%d")
                        + timedelta(days=1)
                        if (
                            x["end_time"] != "2400"
                            and len(str(x["end_datetime"])) == 8
                            and len(x["end_time"]) == 4
                        )
                        else pd.to_datetime(x["end_datetime"])
                    )
                )
            ),
            axis=1,
        )

        ddf = ddf.drop(["end_time"], axis=1)

        ddf["start_datetime"] = pd.to_datetime(
            ddf["end_datetime"]
        ) - pd.to_timedelta(ddf["duration_min"].astype(int), unit="m")
        # ddf['year'] = ddf['start_datetime'].dt.year
        t1 = dfh2.loc[dfh2[0] == "S0", 1].unique()
        ddf["site_id"] = str(t1[0])
        ddf["site_id"] = ddf["site_id"].astype(str)

        ddf = ddf.drop_duplicates()
        ddf["start_datetime"] = ddf["start_datetime"].astype("datetime64[ns]")

        return ddf

def dtype10(df: pd.DataFrame) -> pd.DataFrame:
    data = df.loc[(df[0] == "10") & (df[3].isin(["0", "1", "2", "3", "4"]))].dropna(
        axis=1, how="all"
    )
    dfh2 = pd.DataFrame(df.loc[(df[0].isin(["S0", "L1"]))]).dropna(
        axis=1, how="all"
    )

    if data.empty:
        print("data empty")
        print(data)
    else:
        num_of_fields = int(data.iloc[:,1].unique()[0])
        ddf = data.iloc[:,: 2 + num_of_fields]
        ddf.reset_index(inplace=True)

        cols = ['index']
        for i in range(ddf.shape[1]-1):
            cols.append(config.TYPE10_DATA_COLUMN_NAMES[i])
        ddf = pd.DataFrame(ddf.values, columns=cols)
        ddf["data_id"] = ddf.apply(lambda x: uuid.uuid4(), axis=1)

        if data.shape[1] > ddf.shape[1]:
            sub_data_df = pd.DataFrame(columns=['index','sub_data_type_code','offset_sensor_detection_code','mass_measurement_resolution_kg', 'number','value'])
            for index, row in data.iterrows():
                col = int(row[1]) + 2
                while col < len(row) and row[col] != None:
                    sub_data_type = row[col]
                    col += 1
                    NoOfType = int(row[col])        
                    col +=1
                    if sub_data_type[0].lower() in ['w','a','g']:
                        odc = row[col]
                        col += 1
                        mmr = row[col]
                        col +=1
                        for i in range(0,NoOfType):
                            tempdf = pd.DataFrame([[index,
                            sub_data_type,
                            odc,
                            mmr,
                            i + 1,
                            row[col]]
                            ], columns = ['index',
                            'sub_data_type_code',
                            'offset_sensor_detection_code',
                            'mass_measurement_resolution_kg',
                            'number',
                            'value'
                            ])
                            sub_data_df = pd.concat([sub_data_df, tempdf])
                            col += 1
                    elif sub_data_type[0].lower() in ['s','t','c']:
                        for i in range(0,NoOfType):
                            tempdf = pd.DataFrame([[index, 
                            sub_data_type,
                            i + 1,
                            row[col]]], columns = ['index' ,
                            'sub_data_type_code',
                            'number',
                            'value'])
                            sub_data_df = pd.concat([sub_data_df, tempdf])
                            col += 1
                    elif sub_data_type[0].lower() in ['v']:
                        odc = row[col]
                        col += 1
                        for i in range(0,NoOfType):
                            tempdf = pd.DataFrame([[index,
                            sub_data_type,
                            odc,
                            i + 1,
                            row[col]]
                            ], columns = ['index',
                            'sub_data_type_code',
                            'offset_sensor_detection_code',
                            'number',
                            'value'
                            ])
                            sub_data_df = pd.concat([sub_data_df, tempdf])
                            col += 1

        sub_data_df = sub_data_df.merge(ddf[['index', 'data_id']], how='left', on='index')
        
        ddf = ddf.fillna(0)
        ddf["lane_number"] = ddf["lane_number"].astype(int)
        max_lanes = ddf["lane_number"].max()
        try:
            ddf["direction"] = ddf.apply(
            lambda x: "P" if x["lane_number"] <= (int(max_lanes) / 2) else "N",
            axis=1,
        )
            direction = dfh2.loc[dfh2[0] == "L1", 1:3]
            direction = direction.drop_duplicates()
        except:
            pass

        if ddf["start_datetime"].map(len).isin([8]).all():
            ddf["start_datetime"] = pd.to_datetime(
                ddf["start_datetime"] + ddf["departure_time"],
                format="%Y%m%d%H%M%S%f",
            )
        elif ddf["start_datetime"].map(len).isin([6]).all():
            ddf["start_datetime"] = pd.to_datetime(
                ddf["start_datetime"] + ddf["departure_time"],
                format="%y%m%d%H%M%S%f",
            )
        ddf['year'] = ddf['start_datetime'].dt.year
        t1 = dfh2.loc[dfh2[0] == "S0", 1].unique()
        ddf["site_id"] = str(t1[0])
        ddf["site_id"] = ddf["site_id"].astype(str)

        ddf = ddf.drop(["departure_time"], axis=1)

        ddf = ddf.drop_duplicates()
        ddf["start_datetime"] = ddf["start_datetime"].astype("datetime64[ns]")

        ddf = ddf.replace(r'^\s*$', np.NaN, regex=True)
        sub_data_df = sub_data_df.replace(r'^\s*$', np.NaN, regex=True)
        sub_data_df = sub_data_df.drop("index", axis=1)

        scols = ddf.select_dtypes('object').columns
        
        ddf[scols] = ddf[scols].apply(pd.to_numeric, axis=1, errors='ignore')

        ddf = ddf[ddf.columns.intersection(config.TYPE10_DATA_TABLE_COL_LIST)]

        return ddf, sub_data_df

def dtype60(df: pd.DataFrame) -> pd.DataFrame:
    data = df.loc[(df[0] == "60") & (df[1].isin(["15", "17", "19"]))].dropna(
        axis=1, how="all"
    )
    dfh = pd.DataFrame(
        df.loc[
            (df[0].isin(["21", "70", "30", "13", "60"]))
            & (~df[1].isin(["0", "1", "2", "3", "4"]))
        ]
    ).dropna(axis=1, how="all")
    dfh2 = pd.DataFrame(df.loc[(df[0].isin(["S0", "L1"]))]).dropna(
        axis=1, how="all"
    )
    lengthBinCode = dfh[2]
    numberOfBins = dfh[3].max()
    if data.empty:
        pass
    else:
        ddf = data.iloc[:, 0:]
        ddf = pd.DataFrame(ddf).dropna(axis=1, how="all")
        ddf["data_type_code"] = data[0]
        ddf["data_source_code"] = data[1]
        ddf["edit_code"] = data[2]
        ddf["end_datetime"] = data[3]
        ddf["end_time"] = data[4]
        ddf["duration_min"] = data[5]

        ddf["lane_number"] = data[6]
        ddf["lane_number"] = ddf["lane_number"].astype(int)

        if lengthBinCode.all() == "0":
            ddf["number_of_vehicles_in_length_bin_0_error_bin"] = data[7]
            for i in range(numberOfBins):
                i = i + 1
                newcolumn = "number_of_vehicles_in_length_Bin_" + str(i)
                ddf[newcolumn] = data[7 + i]
        elif lengthBinCode.all() == "2":
            ddf["number_of_vehicles_in_length_bin_0_error_bin"] = 0
            for i in range(numberOfBins):
                i = i + 1
                newcolumn = "number_of_vehicles_in_length_Bin_" + str(i)
                ddf[newcolumn] = data[6 + i]
        else:
            ddf["number_of_vehicles_in_length_bin_0_error_bin"] = data[7]
            for i in range(numberOfBins):
                i = i + 1
                newcolumn = "number_of_vehicles_in_length_bin_" + str(i)
                ddf[newcolumn] = data[7 + i]
            pass

        max_lanes = ddf["lane_number"].max()
        ddf["direction"] = ddf.apply(
            lambda x: "P" if x["lane_number"] <= (int(max_lanes) / 2) else "N",
            axis=1,
        )
        direction = dfh2.loc[dfh2[0] == "L1", 1:3].astype(int)
        direction = direction.drop_duplicates()
        try:
            ddf["forward_direction_code"] = ddf.apply(
                lambda x: Data.get_direction(x["lane_number"], direction), axis=1
            )
            # FIXME: ddf['lane_position_code']=ddf.apply(lambda x: Data.get_lane_position(x['lane_number'],direction),axis=1)
        except Exception:
            ddf["forward_direction_code"] = None
            # ddf['lane_position_code']=None

        ddf["end_datetime"] = ddf.apply(
            lambda x: pd.to_datetime(
                x["end_datetime"] + x["end_time"], format="%y%m%d%H%M"
            )
            if (
                x["end_time"] != "2400"
                and len(str(x["end_datetime"])) == 6
                and len(x["end_time"]) == 4
            )
            else (
                pd.to_datetime(
                    x["end_datetime"] + x["end_time"], format="%Y%m%d%H%M"
                )
                if (
                    x["end_time"] != "2400"
                    and len(str(x["end_datetime"])) == 8
                    and len(x["end_time"]) == 4
                )
                else (
                    pd.to_datetime(x["end_datetime"], format="%y%m%d")
                    + timedelta(days=1)
                    if (
                        x["end_time"] == "2400"
                        and len(str(x["end_datetime"])) == 6
                        and len(x["end_time"]) == 4
                    )
                    else (
                        pd.to_datetime(x["end_datetime"], format="%Y%m%d")
                        + timedelta(days=1)
                        if (
                            x["end_time"] != "2400"
                            and len(str(x["end_datetime"])) == 8
                            and len(x["end_time"]) == 4
                        )
                        else pd.to_datetime(x["end_datetime"])
                    )
                )
            ),
            axis=1,
        )

        ddf = ddf.drop(["end_time"], axis=1)

        t1 = dfh2.loc[dfh2[0] == "S0", 1].unique()
        ddf["site_id"] = str(t1[0])
        ddf["site_id"] = ddf["site_id"].astype(str)
        ddf["start_datetime"] = pd.to_datetime(
            ddf["end_datetime"]
        ) - pd.to_timedelta(ddf["duration_min"].astype(int), unit="m")
        # ddf['year'] = ddf['start_datetime'].dt.year
        ddf = ddf.drop_duplicates()
        ddf["start_datetime"] = ddf["start_datetime"].astype("datetime64[ns]")

        return ddf

def header_calcs(header: pd.DataFrame, data: pd.DataFrame, dtype: int):
    speed_limit_qry = f"select max_speed from trafc.countstation where tcname = '{data['site_id'][0]}' ;"
    speed_limit = pd.read_sql_query(speed_limit_qry,config.ENGINE)
    speed_limit = speed_limit['max_speed'][0]
    data['start_datetime'] = pd.to_datetime(data['start_datetime'])
    if dtype == 21:
        header['adt_total'] = data['total_vehicles_type21'].groupby([data['start_datetime'].dt.to_period('D'), data['header_id']]).sum().mean()
        header['adt_positive_direction'] = data['total_vehicles_type21'].groupby([data['start_datetime'].dt.to_period('D'), data['direction'].loc[data['direction'] == 'P'], data['header_id']]).sum().mean()
        header['adt_positive_direction'] = data['total_vehicles_type21'].groupby([data['start_datetime'].dt.to_period('D'), data['direction'].loc[data['direction'] == 'N'], data['header_id']]).sum().mean()

        header['adtt_total'] = data['total_heavy_vehicles_type21'].groupby([data['start_datetime'].dt.to_period('D'), data['header_id']]).sum().mean()
        header['adtt_positive_direction'] = data['total_vehicles_type21'].groupby([data['start_datetime'].dt.to_period('D'), data['direction'].loc[data['direction'] == 'P'], data['header_id']]).sum().mean()
        header['adtt_positive_direction'] = data['total_vehicles_type21'].groupby([data['start_datetime'].dt.to_period('D'), data['direction'].loc[data['direction'] == 'N'], data['header_id']]).sum().mean()

        header['total_vehicles'] = data['total_vehicles_type21'].groupby(data['header_id']).sum()[0]
        header['total_vehicles_positive_direction'] = data['total_vehicles_type21'].groupby(data['direction'].loc[data['direction'] == 'P']).sum()[0]
        header['total_vehicles_positive_direction'] = data['total_vehicles_type21'].groupby(data['direction'].loc[data['direction'] == 'N']).sum()[0]

        header['total_heavy_vehicles'] = data['total_heavy_vehicles_type21'].groupby(data['header_id']).sum()[0]
        header['total_heavy_positive_direction'] = data['total_heavy_vehicles_type21'].groupby([data['direction'].loc[data['direction'] == 'N'], data['header_id']]).sum()[0]
        header['total_heavy_positive_direction'] = data['total_heavy_vehicles_type21'].groupby([data['direction'].loc[data['direction'] == 'P'], data['header_id']]).sum()[0]
        header['truck_split_positive_direction'] = data['total_heavy_vehicles_type21'].groupby([data['direction'].loc[data['direction'] == 'N'], data['header_id']]).sum()[0] / data['total_heavy_vehicles_type21'].groupby(data['header_id']).sum()[0]
        header['truck_split_positive_direction'] = data['total_heavy_vehicles_type21'].groupby([data['direction'].loc[data['direction'] == 'P'], data['header_id']]).sum()[0] / data['total_heavy_vehicles_type21'].groupby(data['header_id']).sum()[0]

        header['total_light_vehicles'] = data['total_light_vehicles_type21'].groupby(data['header_id']).sum()[0]
        header['total_light_positive_direction'] = data['total_light_vehicles_type21'].groupby([data['direction'].loc[data['direction'] == 'P'], data['header_id']]).sum()[0]
        header['total_light_positive_direction'] = data['total_light_vehicles_type21'].groupby([data['direction'].loc[data['direction'] == 'N'], data['header_id']]).sum()[0]

        header['short_heavy_vehicles'] = data['short_heavy_vehicles'].groupby(data['header_id']).sum()[0]
        header['short_heavy_positive_direction'] = data['short_heavy_vehicles'].groupby([data['direction'].loc[data['direction'] == 'P'], data['header_id']]).sum()[0]
        header['short_heavy_positive_direction'] = data['short_heavy_vehicles'].groupby([data['direction'].loc[data['direction'] == 'N'], data['header_id']]).sum()[0]

        header['Medium_heavy_vehicles'] = data['medium_heavy_vehicles'].groupby(data['header_id']).sum()[0]
        header['Medium_heavy_positive_direction'] = data['medium_heavy_vehicles'].groupby([data['direction'].loc[data['direction'] == 'N'], data['header_id']]).sum()[0]
        header['Medium_heavy_positive_direction'] = data['medium_heavy_vehicles'].groupby([data['direction'].loc[data['direction'] == 'P'], data['header_id']]).sum()[0]

        header['long_heavy_vehicles'] = data['long_heavy_vehicles'].groupby(data['header_id']).sum()[0]
        header['long_heavy_positive_direction'] = data['long_heavy_vehicles'].groupby([data['direction'].loc[data['direction'] == 'P'], data['header_id']]).sum()[0]
        header['long_heavy_positive_direction'] = data['long_heavy_vehicles'].groupby([data['direction'].loc[data['direction'] == 'N'], data['header_id']]).sum()[0]

        header['vehicles_with_rear_to_rear_headway_less_than_2sec_positive_dire'] = data['rear_to_rear_headway_shorter_than_2_seconds'].groupby([data['direction'].loc[data['direction'] == 'P'], data['header_id']]).sum()[0]
        header['vehicles_with_rear_to_rear_headway_less_than_2sec_negative_dire'] = data['rear_to_rear_headway_shorter_than_2_seconds'].groupby([data['direction'].loc[data['direction'] == 'N'], data['header_id']]).sum()[0]
        header['vehicles_with_rear_to_rear_headway_less_than_2sec_total'] = data['rear_to_rear_headway_shorter_than_2_seconds'].groupby(data['header_id']).sum()[0]
    
        header['type_21_count_interval_minutes'] = data['duration_min'].mean()

        header['highest_volume_per_hour_positive_direction'] = data['total_vehicles_type21'].groupby([data['start_datetime'].dt.to_period('H'), data['direction'].loc[data['direction'] == 'P'], data['header_id']]).sum().max()
        header['highest_volume_per_hour_negative_direction'] = data['total_vehicles_type21'].groupby([data['start_datetime'].dt.to_period('H'), data['direction'].loc[data['direction'] == 'N'], data['header_id']]).sum().max()
        header['highest_volume_per_hour_total'] = data['total_vehicles_type21'].groupby([data['start_datetime'].dt.to_period('H'), data['header_id']]).sum().max()

        header['15th_highest_volume_per_hour_positive_direction'] = round(data['total_vehicles_type21'].groupby([data['start_datetime'].dt.to_period('H'), data['direction'].loc[data['direction'] == 'P'], data['header_id']]).sum().quantile(q=0.15,  interpolation='linear'))
        header['15th_highest_volume_per_hour_negative_direction'] = round(data['total_vehicles_type21'].groupby([data['start_datetime'].dt.to_period('H'), data['direction'].loc[data['direction'] == 'N'], data['header_id']]).sum().quantile(q=0.15,  interpolation='linear'))
        header['15th_highest_volume_per_hour_total'] = round(data['total_vehicles_type21'].groupby([data['start_datetime'].dt.to_period('H'), data['header_id']]).sum().quantile(q=0.15, interpolation='linear'))
        
        header['30th_highest_volume_per_hour_positive_direction'] = round(data['total_vehicles_type21'].groupby([data['start_datetime'].dt.to_period('H'), data['direction'].loc[data['direction'] == 'P'], data['header_id']]).sum().quantile(q=0.3,  interpolation='linear'))
        header['30th_highest_volume_per_hour_negative_direction'] = round(data['total_vehicles_type21'].groupby([data['start_datetime'].dt.to_period('H'), data['direction'].loc[data['direction'] == 'N'], data['header_id']]).sum().quantile(q=0.3, interpolation='linear'))
        header['30th_highest_volume_per_hour_total'] = round(data['total_vehicles_type21'].groupby([data['start_datetime'].dt.to_period('H'), data['header_id']]).sum().quantile(q=0.3, interpolation='linear'))

        # header['average_speed_positive_direction'] = 
        # header['average_speed_negative_direction'] = 
        header['average_speed'] = ((
            (header['speedbin1'] * data['speedbin1'].groupby(data['header_id']).sum()[0]) +
            (header['speedbin2'] * data['speedbin2'].groupby(data['header_id']).sum()[0]) +
            (header['speedbin3'] * data['speedbin3'].groupby(data['header_id']).sum()[0]) +
            (header['speedbin4'] * data['speedbin4'].groupby(data['header_id']).sum()[0]) +
            (header['speedbin5'] * data['speedbin5'].groupby(data['header_id']).sum()[0]) +
            (header['speedbin6'] * data['speedbin6'].groupby(data['header_id']).sum()[0]) +
            (header['speedbin7'] * data['speedbin7'].groupby(data['header_id']).sum()[0]) +
            (header['speedbin8'] * data['speedbin8'].groupby(data['header_id']).sum()[0]) +
            (header['speedbin9'] * data['speedbin9'].groupby(data['header_id']).sum()[0]) 
            )
            / data['sum_of_heavy_vehicle_speeds'].groupby(data['header_id']).sum()[0]
            )
        # header['average_speed_light_vehicles_positive_direction'] = 
        # header['average_speed_light_vehicles_negative_direction'] = 
        header['average_speed_light_vehicles'] = ((
            (header['speedbin1'] * data['speedbin1'].groupby(data['header_id']).sum()[0]) +
            (header['speedbin2'] * data['speedbin2'].groupby(data['header_id']).sum()[0]) +
            (header['speedbin3'] * data['speedbin3'].groupby(data['header_id']).sum()[0]) +
            (header['speedbin4'] * data['speedbin4'].groupby(data['header_id']).sum()[0]) +
            (header['speedbin5'] * data['speedbin5'].groupby(data['header_id']).sum()[0]) +
            (header['speedbin6'] * data['speedbin6'].groupby(data['header_id']).sum()[0]) +
            (header['speedbin7'] * data['speedbin7'].groupby(data['header_id']).sum()[0]) +
            (header['speedbin8'] * data['speedbin8'].groupby(data['header_id']).sum()[0]) +
            (header['speedbin9'] * data['speedbin9'].groupby(data['header_id']).sum()[0]) -
            data['sum_of_heavy_vehicle_speeds'].groupby(data['header_id']).sum()[0]
            )
            / data['sum_of_heavy_vehicle_speeds'].groupby(data['header_id']).sum()[0]
            )
        
        # header['average_speed_heavy_vehicles_positive_direction'] = 
        # header['average_speed_heavy_vehicles_negative_direction'] = 
        # header['average_speed_heavy_vehicles'] = 

        header['truck_split_positive_direction'] = (str(round(data['short_heavy_vehicles'].groupby([data['header_id'], data['direction'].loc[data['direction'] == 'P']]).sum()[0] / 
        data['total_heavy_vehicles_type21'].groupby([data['header_id'], data['direction'].loc[data['direction'] == 'P']]).sum()[0]*100)) + ' : ' +
        str(round(data['medium_heavy_vehicles'].groupby([data['header_id'], data['direction'].loc[data['direction'] == 'P']]).sum()[0] / 
        data['total_heavy_vehicles_type21'].groupby([data['header_id'], data['direction'].loc[data['direction'] == 'P']]).sum()[0]*100)) + ' : ' +
        str(round(data['long_heavy_vehicles'].groupby([data['header_id'], data['direction'].loc[data['direction'] == 'P']]).sum()[0] / 
        data['total_heavy_vehicles_type21'].groupby([data['header_id'], data['direction'].loc[data['direction'] == 'P']]).sum()[0]*100))
        )
        header['truck_split_negative_direction'] = (str(round(data['short_heavy_vehicles'].groupby([data['header_id'], data['direction'].loc[data['direction'] == 'N']]).sum()[0] / 
        data['total_heavy_vehicles_type21'].groupby([data['header_id'], data['direction'].loc[data['direction'] == 'N']]).sum()[0]*100)) + ' : ' +
        str(round(data['medium_heavy_vehicles'].groupby([data['header_id'], data['direction'].loc[data['direction'] == 'N']]).sum()[0] / 
        data['total_heavy_vehicles_type21'].groupby([data['header_id'], data['direction'].loc[data['direction'] == 'N']]).sum()[0]*100)) + ' : ' +
        str(round(data['long_heavy_vehicles'].groupby([data['header_id'], data['direction'].loc[data['direction'] == 'N']]).sum()[0] / 
        data['total_heavy_vehicles_type21'].groupby([data['header_id'], data['direction'].loc[data['direction'] == 'N']]).sum()[0]*100))
        )
        header['truck_split_total'] = (str(round(data['short_heavy_vehicles'].groupby(data['header_id']).sum()[0] / 
        data['total_heavy_vehicles_type21'].groupby(data['header_id']).sum()[0]*100)) + ' : ' +
        str(round(data['medium_heavy_vehicles'].groupby(data['header_id']).sum()[0] / 
        data['total_heavy_vehicles_type21'].groupby(data['header_id']).sum()[0]*100)) + ' : ' +
        str(round(data['long_heavy_vehicles'].groupby(data['header_id']).sum()[0] / 
        data['total_heavy_vehicles_type21'].groupby(data['header_id']).sum()[0]*100))
        )

        return header
    elif dtype == 30:
        if header['adt_total'].isnull().all():
            header['adt_total'] = data['total_vehicles_type30'].groupby([data['start_datetime'].dt.to_period('D'), data['header_id']]).sum().mean()
            header['adt_positive_direction'] = data['total_vehicles_type30'].groupby([data['start_datetime'].dt.to_period('D'), data['direction'].loc[data['direction'] == 'P'], data['header_id']]).sum().mean()
            header['adt_positive_direction'] = data['total_vehicles_type30'].groupby([data['start_datetime'].dt.to_period('D'), data['direction'].loc[data['direction'] == 'N'], data['header_id']]).sum().mean()
        else:
            pass

        if header['adtt_total'].isnull().all():
            header['adtt_total'] = data['total_heavy_vehicles_type30'].groupby([data['start_datetime'].dt.to_period('D'), data['header_id']]).sum().mean()
            header['adtt_positive_direction'] = data['total_vehicles_type30'].groupby([data['start_datetime'].dt.to_period('D'), data['direction'].loc[data['direction'] == 'P'], data['header_id']]).sum().mean()
            header['adtt_positive_direction'] = data['total_vehicles_type30'].groupby([data['start_datetime'].dt.to_period('D'), data['direction'].loc[data['direction'] == 'N'], data['header_id']]).sum().mean()
        else:
            pass

        if header['total_vehicles'].isnull().all():
            header['total_vehicles'] = data['total_vehicles_type30'].groupby(data['header_id']).sum()[0]
            header['total_vehicles_positive_direction'] = data['total_vehicles_type30'].groupby(data['direction'].loc[data['direction'] == 'P']).sum()[0]
            header['total_vehicles_positive_direction'] = data['total_vehicles_type30'].groupby(data['direction'].loc[data['direction'] == 'N']).sum()[0]
        else:
            pass
        
        if header['total_heavy_vehicles'].isnull().all():
            header['total_heavy_vehicles'] = data['total_heavy_vehicles_type21'].groupby(data['header_id']).sum()[0]
            header['total_heavy_positive_direction'] = data['total_heavy_vehicles_type21'].groupby([data['direction'].loc[data['direction'] == 'N'], data['header_id']]).sum()[0]
            header['total_heavy_positive_direction'] = data['total_heavy_vehicles_type21'].groupby([data['direction'].loc[data['direction'] == 'P'], data['header_id']]).sum()[0]
            header['truck_split_positive_direction'] = data['total_heavy_vehicles_type21'].groupby([data['direction'].loc[data['direction'] == 'N'], data['header_id']]).sum()[0] / data['total_heavy_vehicles_type21'].groupby(data['header_id']).sum()[0]
            header['truck_split_positive_direction'] = data['total_heavy_vehicles_type21'].groupby([data['direction'].loc[data['direction'] == 'P'], data['header_id']]).sum()[0] / data['total_heavy_vehicles_type21'].groupby(data['header_id']).sum()[0]
        else:
            pass

        if header['total_light_vehicles'].isnull().all():
            header['total_light_vehicles'] = data['total_light_vehicles_type30'].groupby(data['header_id']).sum()[0]
            header['total_light_positive_direction'] = data['total_light_vehicles_type30'].groupby([data['direction'].loc[data['direction'] == 'P'], data['header_id']]).sum()[0]
            header['total_light_positive_direction'] = data['total_light_vehicles_type30'].groupby([data['direction'].loc[data['direction'] == 'N'], data['header_id']]).sum()[0]
        else:
            pass

        return header

    elif dtype == 70:
    
        return header

    elif dtype == 10:
        header['total_light_positive_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme']<=1)&(data['direction']=='N')].count()[0].round().astype(int)
        header['total_light_negative_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme']<=1)&(data['direction']=='P')].count()[0].round().astype(int)
        header['total_light_vehicles'] = data.loc[data['vehicle_class_code_secondary_scheme']<=1].count()[0].round().astype(int)
        header['total_heavy_positive_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='N')].count()[0].round().astype(int)
        header['total_heavy_negative_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='P')].count()[0].round().astype(int)
        header['total_heavy_vehicles'] = data.loc[data['vehicle_class_code_secondary_scheme']>1].count()[0].round().astype(int)
        header['total_short_heavy_positive_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='N')].count()[0].round().astype(int)
        header['total_short_heavy_negative_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='P')].count()[0].round().astype(int)
        header['total_short_heavy_vehicles'] = data.loc[data['vehicle_class_code_secondary_scheme']==2].count()[0].round().astype(int)
        header['total_medium_heavy_positive_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='N')].count()[0].round().astype(int)
        header['total_medium_heavy_negative_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='P')].count()[0].round().astype(int)
        header['total_medium_heavy_vehicles'] = data.loc[data['vehicle_class_code_secondary_scheme']==3].count()[0].round().astype(int)
        header['total_long_heavy_positive_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='N')].count()[0].round().astype(int)
        header['total_long_heavy_negative_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='P')].count()[0].round().astype(int)
        header['total_long_heavy_vehicles'] = data.loc[data['vehicle_class_code_secondary_scheme']==4].count()[0].round().astype(int)
        header['total_vehicles_positive_direction'] = data.loc[data['direction']=='N'].count()[0].round().astype(int)
        header['total_vehicles_negative_direction'] = data.loc[data['direction']=='P'].count()[0].round().astype(int)
        header['total_vehicles'] = data.count()[0]
        header['average_speed_positive_direction'] = data.loc[data['direction']=='N']['vehicle_speed'].mean().round(2)
        header['average_speed_negative_direction'] = data.loc[data['direction']=='P']['vehicle_speed'].mean().round(2)
        header['average_speed'] = data['vehicle_speed'].mean().round(2)
        header['average_speed_light_vehicles_positive_direction'] = data['vehicle_speed'].loc[(data['vehicle_class_code_secondary_scheme']<=1)&(data['direction']=='N')].mean().round(2)
        header['average_speed_light_vehicles_negative_direction'] = data['vehicle_speed'].loc[(data['vehicle_class_code_secondary_scheme']<=1)&(data['direction']=='P')].mean().round(2)
        header['average_speed_light_vehicles'] = data['vehicle_speed'].loc[data['vehicle_class_code_secondary_scheme']<=1].mean().round(2)
        header['average_speed_heavy_vehicles_positive_direction'] = data['vehicle_speed'].loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='N')].mean().round(2)
        header['average_speed_heavy_vehicles_negative_direction'] = data['vehicle_speed'].loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='P')].mean().round(2)
        header['average_speed_heavy_vehicles'] = data['vehicle_speed'].loc[data['vehicle_class_code_secondary_scheme']>1].mean().round(2)
        header['truck_split_positive_direction'] = {str((((data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='N')].count()/data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='N')].count())[0])*100).round().astype(int)) +":"+ str((((data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='P')].count()/data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='P')].count())[0])*100).round().astype(int)) +":"+ str((((data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='N')].count()/data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='P')].count())[0])*100).round().astype(int))}
        header['truck_split_negative_direction'] = {str((((data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='P')].count()/data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='P')].count())[0])*100).round().astype(int)) +":"+ str((((data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='N')].count()/data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='N')].count())[0])*100).round().astype(int)) +":"+ str((((data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='P')].count()/data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='N')].count())[0])*100).round().astype(int))}
        header['truck_split_total'] = {str((((data.loc[data['vehicle_class_code_secondary_scheme']==2].count()/data.loc[data['vehicle_class_code_secondary_scheme']>1].count())[0])*100).round().astype(int)) +":"+ str((((data.loc[data['vehicle_class_code_secondary_scheme']==3].count()/data.loc[data['vehicle_class_code_secondary_scheme']>1].count())[0])*100).round().astype(int)) +":"+ str((((data.loc[data['vehicle_class_code_secondary_scheme']==4].count()/data.loc[data['vehicle_class_code_secondary_scheme']>1].count())[0])*100).round().astype(int))}
        header['estimated_axles_per_truck_negative_direction'] = ((data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='N')].count()[0]*2+data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='N')].count()[0]*5+data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='N')].count()[0]*7)/(data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='N')].count()[0]+data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='N')].count()[0]+data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='N')].count()[0])).round(2)
        header['estimated_axles_per_truck_positive_direction'] = ((data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='P')].count()[0]*2+data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='P')].count()[0]*5+data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='P')].count()[0]*7)/(data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='P')].count()[0]+data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='P')].count()[0]+data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='P')].count()[0])).round(2)
        header['estimated_axles_per_truck_total'] = ((data.loc[data['vehicle_class_code_secondary_scheme']==2].count()[0]*2+data.loc[data['vehicle_class_code_secondary_scheme']==3].count()[0]*5+data.loc[data['vehicle_class_code_secondary_scheme']==4].count()[0]*7)/(data.loc[data['vehicle_class_code_secondary_scheme']==2].count()[0]+data.loc[data['vehicle_class_code_secondary_scheme']==3].count()[0]+data.loc[data['vehicle_class_code_secondary_scheme']==4].count()[0])).round(2)
        header['percentage_speeding_positive_direction'] = ((data.loc[(data['vehicle_speed']>speed_limit)&(data['direction']=='P')].count()[0]/data.loc[data['direction'=='P']].count()[0])*100).round(2)
        header['percentage_speeding_negative_direction'] = ((data.loc[(data['vehicle_speed']>speed_limit)&(data['direction']=='N')].count()[0]/data.loc[data['direction'=='N']].count()[0])*100).round(2)
        header['percentage_speeding_total'] = ((data.loc[data['vehicle_speed']>speed_limit].count()[0]/data.count()[0])*100).round(2)
        header['vehicles_with_rear_to_rear_headway_less_than_2sec_positive_dire'] = data.loc[(data['vehicle_following_code']==2)&data['direction']=='N'].count()[0]
        header['vehicles_with_rear_to_rear_headway_less_than_2sec_negative_dire'] = data.loc[(data['vehicle_following_code']==2)&data['direction']=='P'].count()[0]
        header['vehicles_with_rear_to_rear_headway_less_than_2sec_total'] = data.loc[data['vehicle_following_code']==2].count()[0]
        header['estimated_e80_positive_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='N')].count()[0]*0.6+data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='N')].count()[0]*2.5+data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='N')].count()[0]*2.1
        header['estimated_e80_negative_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='P')].count()[0]*0.6+data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='P')].count()[0]*2.5+data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='P')].count()[0]*2.1
        header['estimated_e80_on_road'] = data.loc[data['vehicle_class_code_secondary_scheme']==2].count()[0]*0.6+data.loc[data['vehicle_class_code_secondary_scheme']==3].count()[0]*2.5+data.loc[data['vehicle_class_code_secondary_scheme']==4].count()[0]*2.1
        header['adt_positive_direction'] = data.loc[data['direction']=='N'].groupby(pd.Grouper(key='start_datetime',freq='D')).count().mean()[0].round().astype(int)
        header['adt_negative_direction'] = data.loc[data['direction']=='P'].groupby(pd.Grouper(key='start_datetime',freq='D')).count().mean()[0].round().astype(int)
        header['adt_total'] = data.groupby(pd.Grouper(key='start_datetime',freq='D')).count().mean()[0].round().astype(int)
        header['adtt_positive_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='N')].groupby(pd.Grouper(key='start_datetime',freq='D')).count().mean()[0].round().astype(int)
        header['adtt_negative_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='P')].groupby(pd.Grouper(key='start_datetime',freq='D')).count().mean()[0].round().astype(int)
        header['adtt_total'] = data.loc[data['vehicle_class_code_secondary_scheme']>1].groupby(pd.Grouper(key='start_datetime',freq='D')).count().mean()[0].round().astype(int)
        header['highest_volume_per_hour_positive_direction'] = data.loc[data['direction']=='N'].groupby(pd.Grouper(key='start_datetime',freq='H')).count().max()[0]
        header['highest_volume_per_hour_negative_direction'] = data.loc[data['direction']=='P'].groupby(pd.Grouper(key='start_datetime',freq='H')).count().max()[0]
        header['highest_volume_per_hour_total'] = data.groupby(pd.Grouper(key='start_datetime',freq='H')).count().max()[0]
        header["15th_highest_volume_per_hour_positive_direction"] = data.loc[data['direction']=='N'].groupby(pd.Grouper(key='start_datetime',freq='D')).count().quantile(0.15)[0].astype(int)
        header["15th_highest_volume_per_hour_negative_direction"] = data.loc[data['direction']=='P'].groupby(pd.Grouper(key='start_datetime',freq='D')).count().quantile(0.15)[0].astype(int)
        header["15th_highest_volume_per_hour_total"] = data.groupby(pd.Grouper(key='start_datetime',freq='D')).count().quantile(0.15)[0].astype(int)
        header["30th_highest_volume_per_hour_positive_direction"] = data.loc[data['direction']=='N'].groupby(pd.Grouper(key='start_datetime',freq='D')).count().quantile(0.30)[0].astype(int)
        header["30th_highest_volume_per_hour_negative_direction"] = data.loc[data['direction']=='P'].groupby(pd.Grouper(key='start_datetime',freq='D')).count().quantile(0.30)[0].astype(int)
        header["30th_highest_volume_per_hour_total"] = data.groupby(pd.Grouper(key='start_datetime',freq='D')).count().quantile(0.30)[0].astype(int)
        header["15th_percentile_speed_positive_direction"] = data.loc[data['direction']=='N']['vehicle_speed'].quantile(0.15).round(2)
        header["15th_percentile_speed_negative_direction"] = data.loc[data['direction']=='P']['vehicle_speed'].quantile(0.15).round(2)
        header["15th_percentile_speed_total"] = data['vehicle_speed'].quantile(0.15).round(2)
        header["85th_percentile_speed_positive_direction"] = data.loc[data['direction']=='N']['vehicle_speed'].quantile(0.85).round(2)
        header["85th_percentile_speed_negative_direction"] = data.loc[data['direction']=='P']['vehicle_speed'].quantile(0.85).round(2)
        header["85th_percentile_speed_total"] = data['vehicle_speed'].quantile(0.85).round(2)
        header['avg_weekday_traffic'] = data.groupby(pd.Grouper(key='start_datetime',freq='B')).count().mean()[0].round().astype(int)
        header['number_of_days_counted'] = data.groupby([data['start_datetime'].dt.to_period('D')]).count().count()[0]
        header['duration_hours'] = data.groupby([data['start_datetime'].dt.to_period('H')]).count().count()[0]
    
        return header

    elif dtype == 60:
        
        return header

    else:
        return header

def data_update_type21(row):
    qry = f"""
        UPDATE trafc.electronic_count_data_partitioned SET 
        header_id = '{row['header_id']}',
        end_datetime = '{row['end_datetime']}',
        duration_min = {row['duration_min']},
        lane_number = {row['lane_number']},
        speedbin1 = {row['speedbin1']},
        speedbin2 = {row['speedbin2']},
        speedbin3 = {row['speedbin3']},
        speedbin4 = {row['speedbin4']},
        speedbin5 = {row['speedbin5']},
        speedbin6 = {row['speedbin6']},
        speedbin7 = {row['speedbin7']},
        speedbin8 = {row['speedbin8']},
        speedbin9 = {row['speedbin9']},
        speedbin10 = {row['speedbin10']},
        sum_of_heavy_vehicle_speeds = {row['sum_of_heavy_vehicle_speeds']},
        short_heavy_vehicles = {row['short_heavy_vehicles']},
        medium_heavy_vehicles = {row['medium_heavy_vehicles']},
        long_heavy_vehicles = {row['long_heavy_vehicles']},
        rear_to_rear_headway_shorter_than_2_seconds = {row['rear_to_rear_headway_shorter_than_2_seconds']},
        rear_to_rear_headways_shorter_than_programmed_time = {row['rear_to_rear_headways_shorter_than_programmed_time']},
        speedbin0 = {row['speedbin0']},
        total_heavy_vehicles_type21 = {row['total_heavy_vehicles_type21']},
        total_light_vehicles_type21 = {row['total_light_vehicles_type21']},
        total_vehicles_type21 = {row['total_vehicles_type21']},
        direction = '{row['direction']}',
        start_datetime = '{row['start_datetime']}',
        year = 	{row['year']},
        site_id = '{row['site_id']}'
        where site_id = '{row['site_id']}' and start_datetime = '{row['start_datetime']}' and lane_number = {row['lane_number']}
    """
    return qry

def data_insert_type21(row):
	qry = f"""insert into
	trafc.electronic_count_data_partitioned
	(
	header_id,
	end_datetime,
	duration_min,
	lane_number,
	speedbin1,
	speedbin2,
	speedbin3,
	speedbin4,
	speedbin5,
	speedbin6,
	speedbin7,
	speedbin8,
	speedbin9,
	speedbin10,
	sum_of_heavy_vehicle_speeds,
	short_heavy_vehicles,
	medium_heavy_vehicles,
	long_heavy_vehicles,
	rear_to_rear_headway_shorter_than_2_seconds,
	rear_to_rear_headways_shorter_than_programmed_time,
	speedbin0,
	total_heavy_vehicles_type21,
	total_light_vehicles_type21,
	total_vehicles_type21,
	direction,
	start_datetime,
	year,
	site_id)
	values(
	'{row['header_id']}',
	'{row['end_datetime']}',
	{row['duration_min']},
	{row['lane_number']},
	{row['speedbin1']},
	{row['speedbin2']},
	{row['speedbin3']},
	{row['speedbin4']},
	{row['speedbin5']},
	{row['speedbin6']},
	{row['speedbin7']},
	{row['speedbin8']},
	{row['speedbin9']},
	{row['speedbin10']},
	{row['sum_of_heavy_vehicle_speeds']},
	{row['short_heavy_vehicles']},
	{row['medium_heavy_vehicles']},
	{row['long_heavy_vehicles']},
	{row['rear_to_rear_headway_shorter_than_2_seconds']},
	{row['rear_to_rear_headways_shorter_than_programmed_time']},
	{row['speedbin0']},
	{row['total_heavy_vehicles_type21']},
	{row['total_light_vehicles_type21']},
	{row['total_vehicles_type21']},
	'{row['direction']}',
	'{row['start_datetime']}',
	{row['year']},
	'{row['site_id']}'
	);
	"""
	return qry

def data_insert_type30(row):
    qry = f"""
    insert into	trafc.electronic_count_data_partitioned (
        end_datetime,
        end_time,
        duration_min,
        lane_number,
        unknown_vehicle_error_class,
        motorcycle,
        light_motor_vehicles,
        light_motor_vehicles_towing,
        two_axle_busses,
        two_axle_6_tyre_single_units,
        busses_with_3_or_4_axles,
        two_axle_6_tyre_single_unit_with_light_trailer_4_axles_max,
        three_axle_single_unit_including_single_axle_light_trailer,
        four_or_less_axle_including_a_single_trailer,
        buses_with_5_or_more_axles,
        three_axle_single_unit_and_light_trailer_more_than_4_axles,
        five_axle_single_trailer,
        six_axle_single_trailer,
        five_or_less_axle_multi_trailer,
        six_axle_multi_trailer,
        seven_axle_multi_trailer,
        eight_or_more_axle_multi_trailer,
        heavy_vehicle,
        direction,
        start_datetime,
        year,
        site_id       
    )
    VALUES (
        '{row['end_datetime']}',
        '{row['end_time']}',
        {row['duration_min']},
        {row['lane_number']},
        {row['unknown_vehicle_error_class']},
        {row['motorcycle']},
        {row['light_motor_vehicles']},
        {row['light_motor_vehicles_towing']},
        {row['two_axle_busses']},
        {row['two_axle_6_tyre_single_units']},
        {row['busses_with_3_or_4_axles']},
        {row['two_axle_6_tyre_single_unit_with_light_trailer_4_axles_max']},
        {row['three_axle_single_unit_including_single_axle_light_trailer']},
        {row['four_or_less_axle_including_a_single_trailer']},
        {row['buses_with_5_or_more_axles']},
        {row['three_axle_single_unit_and_light_trailer_more_than_4_axles']},
        {row['five_axle_single_trailer']},
        {row['six_axle_single_trailer']},
        {row['five_or_less_axle_multi_trailer']},
        {row['six_axle_multi_trailer']},
        {row['seven_axle_multi_trailer']},
        {row['eight_or_more_axle_multi_trailer']},
        {row['heavy_vehicle']},
        '{row['direction']}',
        '{row['start_datetime']}',
        {row['year']},
        '{row['site_id']}'
    )
    """
    return qry

def data_update_type30(row):
    qry = f"""
    UPDATE trafc.electronic_count_data_partitioned SET 
        end_datetime = '{row['end_datetime']}',
        end_time = '{row['end_time']}',
        duration_min = {row['duration_min']},
        lane_number = {row['lane_number']},
        unknown_vehicle_error_class = {row['unknown_vehicle_error_class']},
        motorcycle = {row['motorcycle']},
        light_motor_vehicles = {row['light_motor_vehicles']},
        light_motor_vehicles_towing = {row['light_motor_vehicles_towing']},
        two_axle_busses = {row['two_axle_busses']},
        two_axle_6_tyre_single_units = {row['two_axle_6_tyre_single_units']},
        busses_with_3_or_4_axles = {row['busses_with_3_or_4_axles']},
        two_axle_6_tyre_single_unit_with_light_trailer_4_axles_max = {row['two_axle_6_tyre_single_unit_with_light_trailer_4_axles_max']},
        three_axle_single_unit_including_single_axle_light_trailer = {row['three_axle_single_unit_including_single_axle_light_trailer']},
        four_or_less_axle_including_a_single_trailer = {row['four_or_less_axle_including_a_single_trailer']},
        buses_with_5_or_more_axles = {row['buses_with_5_or_more_axles']},
        three_axle_single_unit_and_light_trailer_more_than_4_axles = {row['three_axle_single_unit_and_light_trailer_more_than_4_axles']},
        five_axle_single_trailer = {row['five_axle_single_trailer']},
        six_axle_single_trailer = {row['six_axle_single_trailer']},
        five_or_less_axle_multi_trailer = {row['five_or_less_axle_multi_trailer']},
        six_axle_multi_trailer = {row['six_axle_multi_trailer']},
        seven_axle_multi_trailer = {row['seven_axle_multi_trailer']},
        eight_or_more_axle_multi_trailer = {row['eight_or_more_axle_multi_trailer']},
        heavy_vehicle = {row['heavy_vehicle']},
        direction = '{row['direction']}',
        start_datetime = '{row['start_datetime']}',
        year = {row['year']},
        site_id = '{row['site_id']}'
        where site_id = '{row['site_id']}' and start_datetime = '{row['start_datetime']}' and lane_number = {row['lane_number']}
    """
    return qry

def data_insert_type10(row):
    qry = f"""
    INSERT INTO trafc.electronic_count_data_type_10 (
        site_id,
        header_id,
        "year",
        number_of_fields_associated_with_the_basic_vehicle_data,
        data_source_code,
        edit_code,
        departure_date,
        departure_time,
        assigned_lane_number,
        physical_lane_number,
        forward_reverse_code,
        vehicle_category,
        vehicle_class_code_primary_scheme,
        vehicle_class_code_secondary_scheme,
        vehicle_speed,
        vehicle_length,
        site_occupancy_time_in_milliseconds,
        chassis_height_code,
        vehicle_following_code,
        vehicle_tag_code,
        trailer_count,
        axle_count,
        bumper_to_1st_axle_spacing,
        tyre_type,
        start_datetime,
        direction,
        data_id
    )
    VALUES (
        '{row['site_id']}',
        '{row['header_id']}',
        {row['year']},
        {row['number_of_fields_associated_with_the_basic_vehicle_data']},
        {row['data_source_code']},
        {row['edit_code']},
        '{row['departure_date']}',
        '{row['departure_time']}',
        {row['assigned_lane_number']},
        {row['physical_lane_number']},
        {row['forward_reverse_code']},
        {row['vehicle_category']},
        {row['vehicle_class_code_primary_scheme']},
        {row['vehicle_class_code_secondary_scheme']},
        {row['vehicle_speed']},
        {row['vehicle_length']},
        {row['site_occupancy_time_in_milliseconds']},
        {row['chassis_height_code']},
        {row['vehicle_following_code']},
        {row['vehicle_tag_code']},
        {row['trailer_count']},
        {row['axle_count']},
        {row['bumper_to_1st_axle_spacing']},
        {row['tyre_type']},
        '{row['start_datetime']}',
        '{row['direction']}',
        '{row['data_id']}'
    )
    """
    return qry

def data_update_type10(row):
    qry = f"""
    UPDATE trafc.electronic_count_data_type_10 SET
        site_id = '{row['site_id']}',
        header_id = '{row['header_id']}',
        "year" = {row['year']},
        number_of_fields_associated_with_the_basic_vehicle_data = {row['number_of_fields_associated_with_the_basic_vehicle_data']},
        data_source_code = {row['data_source_code']},
        edit_code = {row['edit_code']},
        departure_date = '{row['departure_date']}',
        departure_time = '{row['departure_time']}',
        assigned_lane_number = {row['assigned_lane_number']},
        physical_lane_number = {row['physical_lane_number']},
        forward_reverse_code = {row['forward_reverse_code']},
        vehicle_category = {row['vehicle_category']},
        vehicle_class_code_primary_scheme = {row['vehicle_class_code_primary_scheme']},
        vehicle_class_code_secondary_scheme = {row['vehicle_class_code_secondary_scheme']},
        vehicle_speed = {row['vehicle_speed']},
        vehicle_length = {row['vehicle_length']},
        site_occupancy_time_in_milliseconds = {row['site_occupancy_time_in_milliseconds']},
        chassis_height_code = {row['chassis_height_code']},
        vehicle_following_code = {row['vehicle_following_code']},
        vehicle_tag_code = {row['vehicle_tag_code']},
        trailer_count = {row['trailer_count']},
        axle_count = {row['axle_count']},
        bumper_to_1st_axle_spacing = {row['bumper_to_1st_axle_spacing']},
        tyre_type = {row['tyre_type']},
        start_datetime'{row['start_datetime']}',
        direction'{row['direction']}',
        data_id'{row['data_id']}'
        where site_id = '{row['site_id']}' and physical_lane_number = {row['physical_lane_number']} and start_datetime'{row['start_datetime']}' 
    )
    """
    return qry

def header_update_type10(data):
    speed_limit_qry = f"select max_speed from trafc.countstation where tcname = '{data['site_id'][0]}' ;"
    speed_limit = pd.read_sql_query(speed_limit_qry,config.ENGINE)
    speed_limit = speed_limit['max_speed'][0]

    UPDATE_STRING = f"""update
        trafc.electronic_count_header
        set
        total_light_positive_direction = {data.loc[(data['vehicle_class_code_secondary_scheme']<=1)&(data['direction']=='N')].count()[0].round().astype(int)},
        total_light_negative_direction = {data.loc[(data['vehicle_class_code_secondary_scheme']<=1)&(data['direction']=='P')].count()[0].round().astype(int)},
        total_light_vehicles = {data.loc[data['vehicle_class_code_secondary_scheme']<=1].count()[0].round().astype(int)},
        total_heavy_positive_direction = {data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='N')].count()[0].round().astype(int)},
        total_heavy_negative_direction = {data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='P')].count()[0].round().astype(int)},
        total_heavy_vehicles = {data.loc[data['vehicle_class_code_secondary_scheme']>1].count()[0].round().astype(int)},
        total_short_heavy_positive_direction = {data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='N')].count()[0].round().astype(int)},
        total_short_heavy_negative_direction = {data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='P')].count()[0].round().astype(int)},
        total_short_heavy_vehicles = {data.loc[data['vehicle_class_code_secondary_scheme']==2].count()[0].round().astype(int)},
        total_medium_heavy_positive_direction = {data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='N')].count()[0].round().astype(int)},
        total_medium_heavy_negative_direction = {data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='P')].count()[0].round().astype(int)},
        total_medium_heavy_vehicles = {data.loc[data['vehicle_class_code_secondary_scheme']==3].count()[0].round().astype(int)},
        total_long_heavy_positive_direction = {data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='N')].count()[0].round().astype(int)},
        total_long_heavy_negative_direction = {data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='P')].count()[0].round().astype(int)},
        total_long_heavy_vehicles = {data.loc[data['vehicle_class_code_secondary_scheme']==4].count()[0].round().astype(int)},
        total_vehicles_positive_direction = {data.loc[data['direction']=='N'].count()[0].round().astype(int)},
        total_vehicles_negative_direction = {data.loc[data['direction']=='P'].count()[0].round().astype(int)},
        total_vehicles = {data.count()[0]},
        average_speed_positive_direction = {data.loc[data['direction']=='N']['vehicle_speed'].mean().round(2)},
        average_speed_negative_direction = {data.loc[data['direction']=='P']['vehicle_speed'].mean().round(2)},
        average_speed = {data['vehicle_speed'].mean().round(2)},
        average_speed_light_vehicles_positive_direction = {data['vehicle_speed'].loc[(data['vehicle_class_code_secondary_scheme']<=1)&(data['direction']=='N')].mean().round(2)},
        average_speed_light_vehicles_negative_direction = {data['vehicle_speed'].loc[(data['vehicle_class_code_secondary_scheme']<=1)&(data['direction']=='P')].mean().round(2)},
        average_speed_light_vehicles = {data['vehicle_speed'].loc[data['vehicle_class_code_secondary_scheme']<=1].mean().round(2)},
        average_speed_heavy_vehicles_positive_direction = {data['vehicle_speed'].loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='N')].mean().round(2)},
        average_speed_heavy_vehicles_negative_direction = {data['vehicle_speed'].loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='P')].mean().round(2)},
        average_speed_heavy_vehicles = {data['vehicle_speed'].loc[data['vehicle_class_code_secondary_scheme']>1].mean().round(2)},
        truck_split_positive_direction = '{str((((data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='N')].count()/data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='N')].count())[0])*100).round().astype(int)) +":"+ str((((data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='P')].count()/data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='P')].count())[0])*100).round().astype(int)) +":"+ str((((data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='N')].count()/data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='P')].count())[0])*100).round().astype(int))}',
        truck_split_negative_direction = '{str((((data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='P')].count()/data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='P')].count())[0])*100).round().astype(int)) +":"+ str((((data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='N')].count()/data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='N')].count())[0])*100).round().astype(int)) +":"+ str((((data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='P')].count()/data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='N')].count())[0])*100).round().astype(int))}',
        truck_split_total = '{str((((data.loc[data['vehicle_class_code_secondary_scheme']==2].count()/data.loc[data['vehicle_class_code_secondary_scheme']>1].count())[0])*100).round().astype(int)) +":"+ str((((data.loc[data['vehicle_class_code_secondary_scheme']==3].count()/data.loc[data['vehicle_class_code_secondary_scheme']>1].count())[0])*100).round().astype(int)) +":"+ str((((data.loc[data['vehicle_class_code_secondary_scheme']==4].count()/data.loc[data['vehicle_class_code_secondary_scheme']>1].count())[0])*100).round().astype(int))}',
        estimated_axles_per_truck_negative_direction = {((data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='N')].count()[0]*2+data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='N')].count()[0]*5+data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='N')].count()[0]*7)/(data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='N')].count()[0]+data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='N')].count()[0]+data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='N')].count()[0])).round(2)},
        estimated_axles_per_truck_positive_direction = {((data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='P')].count()[0]*2+data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='P')].count()[0]*5+data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='P')].count()[0]*7)/(data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='P')].count()[0]+data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='P')].count()[0]+data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='P')].count()[0])).round(2)},
        estimated_axles_per_truck_total = {((data.loc[data['vehicle_class_code_secondary_scheme']==2].count()[0]*2+data.loc[data['vehicle_class_code_secondary_scheme']==3].count()[0]*5+data.loc[data['vehicle_class_code_secondary_scheme']==4].count()[0]*7)/(data.loc[data['vehicle_class_code_secondary_scheme']==2].count()[0]+data.loc[data['vehicle_class_code_secondary_scheme']==3].count()[0]+data.loc[data['vehicle_class_code_secondary_scheme']==4].count()[0])).round(2)},
        percentage_speeding_positive_direction = {((data.loc[(data['vehicle_speed']>speed_limit)&(data['direction']=='P')].count()[0]/data.loc[data['direction'=='P']].count()[0])*100).round(2)},
        percentage_speeding_negative_direction = {((data.loc[(data['vehicle_speed']>speed_limit)&(data['direction']=='N')].count()[0]/data.loc[data['direction'=='N']].count()[0])*100).round(2)},
        percentage_speeding_total = {((data.loc[data['vehicle_speed']>speed_limit].count()[0]/data.count()[0])*100).round(2)},
        vehicles_with_rear_to_rear_headway_less_than_2sec_positive_dire = {data.loc[(data['vehicle_following_code']==2)&data['direction']=='N'].count()[0]},
        vehicles_with_rear_to_rear_headway_less_than_2sec_negative_dire = {data.loc[(data['vehicle_following_code']==2)&data['direction']=='P'].count()[0]},
        vehicles_with_rear_to_rear_headway_less_than_2sec_total = {data.loc[data['vehicle_following_code']==2].count()[0]},
        estimated_e80_positive_direction = {data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='N')].count()[0]*0.6+data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='N')].count()[0]*2.5+data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='N')].count()[0]*2.1},
        estimated_e80_negative_direction = {data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='P')].count()[0]*0.6+data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='P')].count()[0]*2.5+data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='P')].count()[0]*2.1},
        estimated_e80_on_road = {data.loc[data['vehicle_class_code_secondary_scheme']==2].count()[0]*0.6+data.loc[data['vehicle_class_code_secondary_scheme']==3].count()[0]*2.5+data.loc[data['vehicle_class_code_secondary_scheme']==4].count()[0]*2.1},
        adt_positive_direction = {data.loc[data['direction']=='N'].groupby(pd.Grouper(key='start_datetime',freq='D')).count().mean()[0].round().astype(int)},
        adt_negative_direction = {data.loc[data['direction']=='P'].groupby(pd.Grouper(key='start_datetime',freq='D')).count().mean()[0].round().astype(int)},
        adt_total = {data.groupby(pd.Grouper(key='start_datetime',freq='D')).count().mean()[0].round().astype(int)},
        adtt_positive_direction = {data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='N')].groupby(pd.Grouper(key='start_datetime',freq='D')).count().mean()[0].round().astype(int)},
        adtt_negative_direction = {data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='P')].groupby(pd.Grouper(key='start_datetime',freq='D')).count().mean()[0].round().astype(int)},
        adtt_total = {data.loc[data['vehicle_class_code_secondary_scheme']>1].groupby(pd.Grouper(key='start_datetime',freq='D')).count().mean()[0].round().astype(int)},
        highest_volume_per_hour_positive_direction = {data.loc[data['direction']=='N'].groupby(pd.Grouper(key='start_datetime',freq='H')).count().max()[0]},
        highest_volume_per_hour_negative_direction = {data.loc[data['direction']=='P'].groupby(pd.Grouper(key='start_datetime',freq='H')).count().max()[0]},
        highest_volume_per_hour_total = {data.groupby(pd.Grouper(key='start_datetime',freq='H')).count().max()[0]},
        "15th_highest_volume_per_hour_positive_direction" = {data.loc[data['direction']=='N'].groupby(pd.Grouper(key='start_datetime',freq='D')).count().quantile(0.15)[0].astype(int)},
        "15th_highest_volume_per_hour_negative_direction" = {data.loc[data['direction']=='P'].groupby(pd.Grouper(key='start_datetime',freq='D')).count().quantile(0.15)[0].astype(int)},
        "15th_highest_volume_per_hour_total" = {data.groupby(pd.Grouper(key='start_datetime',freq='D')).count().quantile(0.15)[0].astype(int)},
        "30th_highest_volume_per_hour_positive_direction" = {data.loc[data['direction']=='N'].groupby(pd.Grouper(key='start_datetime',freq='D')).count().quantile(0.30)[0].astype(int)},
        "30th_highest_volume_per_hour_negative_direction" = {data.loc[data['direction']=='P'].groupby(pd.Grouper(key='start_datetime',freq='D')).count().quantile(0.30)[0].astype(int)},
        "30th_highest_volume_per_hour_total" = {data.groupby(pd.Grouper(key='start_datetime',freq='D')).count().quantile(0.30)[0].astype(int)},
        "15th_percentile_speed_positive_direction" = {data.loc[data['direction']=='N']['vehicle_speed'].quantile(0.15).round(2)},
        "15th_percentile_speed_negative_direction" = {data.loc[data['direction']=='P']['vehicle_speed'].quantile(0.15).round(2)},
        "15th_percentile_speed_total" = {data['vehicle_speed'].quantile(0.15).round(2)},
        "85th_percentile_speed_positive_direction" = {data.loc[data['direction']=='N']['vehicle_speed'].quantile(0.85).round(2)},
        "85th_percentile_speed_negative_direction" = {data.loc[data['direction']=='P']['vehicle_speed'].quantile(0.85).round(2)},
        "85th_percentile_speed_total" = {data['vehicle_speed'].quantile(0.85).round(2)},
        avg_weekday_traffic = {data.groupby(pd.Grouper(key='start_datetime',freq='B')).count().mean()[0].round().astype(int)},
        number_of_days_counted = {data.groupby([data['start_datetime'].dt.to_period('D')]).count().count()[0]},
        duration_hours = {data.groupby([data['start_datetime'].dt.to_period('H')]).count().count()[0]}
        where
        site_id = '{data['site_id'][0]}'
        and start_datetime = '{data['start_datetime'][0]}'
        and end_datetime = '{data['end_datetime'][0]}';
        """
    return UPDATE_STRING

def header_insert(row):
    qry = f"""
    INSERT INTO trafc.electronic_count_header (
        header_id,
        site_id,
        station_name,
        x,
        y,
        start_datetime,
        end_datetime,
        number_of_lanes,
        type_21_count_interval_minutes,
        type_21_programmable_rear_to_rear_headway_bin,
        type_21_program_id,
        speedbin1,
        speedbin2,
        speedbin3,
        speedbin4,
        speedbin5,
        speedbin6,
        speedbin7,
        speedbin8,
        speedbin9,
        speedbin10,
        type_10_vehicle_classification_scheme_primary,
        type_10_vehicle_classification_scheme_secondary,
        type_10_maximum_gap_milliseconds,
        type_10_maximum_differential_speed,
        type_30_summary_interval_minutes,
        type_30_vehicle_classification_scheme,
        type_70_summary_interval_minutes,
        type_70_vehicle_classification_scheme,
        type_70_maximum_gap_milliseconds,
        type_70_maximum_differential_speed,
        type_70_error_bin_code,
        instrumentation_description,
        document_url,
        date_processed,
        growth_rate_use,
        total_light_positive_direction,
        total_light_negative_direction,
        total_light_vehicles,
        total_heavy_positive_direction,
        total_heavy_negative_direction,
        total_heavy_vehicles,
        total_short_heavy_positive_direction,
        total_short_heavy_negative_direction,
        total_short_heavy_vehicles,
        total_medium_heavy_positive_direction,
        total_medium_heavy_negative_direction,
        total_medium_heavy_vehicles,
        total_long_heavy_positive_direction,
        total_long_heavy_negative_direction,
        total_long_heavy_vehicles,
        total_vehicles_positive_direction,
        total_vehicles_negative_direction,
        total_vehicles,
        average_speed_positive_direction,
        average_speed_negative_direction,
        average_speed,
        average_speed_light_vehicles_positive_direction,
        average_speed_light_vehicles_negative_direction,
        average_speed_light_vehicles,
        average_speed_heavy_vehicles_positive_direction,
        average_speed_heavy_vehicles_negative_direction,
        average_speed_heavy_vehicles,
        truck_split_positive_direction,
        truck_split_negative_direction,
        truck_split_total,
        estimated_axles_per_truck_positive_direction,
        estimated_axles_per_truck_negative_direction,
        estimated_axles_per_truck_total,
        percentage_speeding_positive_direction,
        percentage_speeding_negative_direction,
        percentage_speeding_total,
        vehicles_with_rear_to_rear_headway_less_than_2sec_positive_dire,
        vehicles_with_rear_to_rear_headway_less_than_2sec_negative_dire,
        vehicles_with_rear_to_rear_headway_less_than_2sec_total,
        estimated_e80_positive_direction,
        estimated_e80_negative_direction,
        estimated_e80_on_road,
        adt_positive_direction,
        adt_negative_direction,
        adt_total,
        adtt_positive_direction,
        adtt_negative_direction,
        adtt_total,
        highest_volume_per_hour_positive_direction,
        highest_volume_per_hour_negative_direction,
        highest_volume_per_hour_total,
        "15th_highest_volume_per_hour_positive_direction",
        "15th_highest_volume_per_hour_negative_direction",
        "15th_highest_volume_per_hour_total",
        "30th_highest_volume_per_hour_positive_direction",
        "30th_highest_volume_per_hour_negative_direction",
        "30th_highest_volume_per_hour_total",
        "15th_percentile_speed_positive_direction",
        "15th_percentile_speed_negative_direction",
        "15th_percentile_speed_total",
        "85th_percentile_speed_positive_direction",
        "85th_percentile_speed_negative_direction",
        "85th_percentile_speed_total",
        "year",
        positive_direction,
        negative_direction,
        avg_weekday_traffic,
        number_of_days_counted,
        duration_hours
    )
    VALUES (
        '{row['header_id']}',
        '{row['site_id']}',
        '{row['station_name']}',
        '{row['x']}',
        '{row['y']}',
        '{row['start_datetime']}',
        '{row['end_datetime']}',
        {row['number_of_lanes']},
        {row['type_21_count_interval_minutes']},
        {row['type_21_programmable_rear_to_rear_headway_bin']},
        {row['type_21_program_id']},
        {row['speedbin1']},
        {row['speedbin2']},
        {row['speedbin3']},
        {row['speedbin4']},
        {row['speedbin5']},
        {row['speedbin6']},
        {row['speedbin7']},
        {row['speedbin8']},
        {row['speedbin9']},
        {row['speedbin10']},
        {row['type_10_vehicle_classification_scheme_primary']},
        {row['type_10_vehicle_classification_scheme_secondary']},
        {row['type_10_maximum_gap_milliseconds']},
        {row['type_10_maximum_differential_speed']},
        {row['type_30_summary_interval_minutes']},
        {row['type_30_vehicle_classification_scheme']},
        {row['type_70_summary_interval_minutes']},
        {row['type_70_vehicle_classification_scheme']},
        {row['type_70_maximum_gap_milliseconds']},
        {row['type_70_maximum_differential_speed']},
        {row['type_70_error_bin_code']},
        '{row['instrumentation_description']}',
        '{row['document_url']}',
        '{row['date_processed']}',
        '{row['growth_rate_use']}',
        {row['total_light_positive_direction']},
        {row['total_light_negative_direction']},
        {row['total_light_vehicles']},
        {row['total_heavy_positive_direction']},
        {row['total_heavy_negative_direction']},
        {row['total_heavy_vehicles']},
        {row['total_short_heavy_positive_direction']},
        {row['total_short_heavy_negative_direction']},
        {row['total_short_heavy_vehicles']},
        {row['total_medium_heavy_positive_direction']},
        {row['total_medium_heavy_negative_direction']},
        {row['total_medium_heavy_vehicles']},
        {row['total_long_heavy_positive_direction']},
        {row['total_long_heavy_negative_direction']},
        {row['total_long_heavy_vehicles']},
        {row['total_vehicles_positive_direction']},
        {row['total_vehicles_negative_direction']},
        {row['total_vehicles']},
        {row['average_speed_positive_direction']},
        {row['average_speed_negative_direction']},
        {row['average_speed']},
        {row['average_speed_light_vehicles_positive_direction']},
        {row['average_speed_light_vehicles_negative_direction']},
        {row['average_speed_light_vehicles']},
        {row['average_speed_heavy_vehicles_positive_direction']},
        {row['average_speed_heavy_vehicles_negative_direction']},
        {row['average_speed_heavy_vehicles']},
        '{row['truck_split_positive_direction']}',
        '{row['truck_split_negative_direction']}',
        '{row['truck_split_total']}',
        {row['estimated_axles_per_truck_positive_direction']},
        {row['estimated_axles_per_truck_negative_direction']},
        {row['estimated_axles_per_truck_total']},
        {row['percentage_speeding_positive_direction']},
        {row['percentage_speeding_negative_direction']},
        {row['percentage_speeding_total']},
        {row['vehicles_with_rear_to_rear_headway_less_than_2sec_positive_dire']},
        {row['vehicles_with_rear_to_rear_headway_less_than_2sec_negative_dire']},
        {row['vehicles_with_rear_to_rear_headway_less_than_2sec_total']},
        {row['estimated_e80_positive_direction']},
        {row['estimated_e80_negative_direction']},
        {row['estimated_e80_on_road']},
        {row['adt_positive_direction']},
        {row['adt_negative_direction']},
        {row['adt_total']},
        {row['adtt_positive_direction']},
        {row['adtt_negative_direction']},
        {row['adtt_total']},
        {row['highest_volume_per_hour_positive_direction']},
        {row['highest_volume_per_hour_negative_direction']},
        {row['highest_volume_per_hour_total']},
        {row['15th_highest_volume_per_hour_positive_direction']},
        {row['15th_highest_volume_per_hour_negative_direction']},
        {row['15th_highest_volume_per_hour_total']},
        {row['30th_highest_volume_per_hour_positive_direction']},
        {row['30th_highest_volume_per_hour_negative_direction']},
        {row['30th_highest_volume_per_hour_total']},
        {row['15th_percentile_speed_positive_direction']},
        {row['15th_percentile_speed_negative_direction']},
        {row['15th_percentile_speed_total']},
        {row['85th_percentile_speed_positive_direction']},
        {row['85th_percentile_speed_negative_direction']},
        {row['85th_percentile_speed_total']},
        {row['year']},
        '{row['positive_direction']}',
        '{row['negative_direction']}',
        {row['avg_weekday_traffic']},
        {row['number_of_days_counted']},
        {row['duration_hours']}
            )
        """
    return qry

def header_update(row):
    qry = f""" UPDATE trafc.electronic_count_header SET
        x = '{row['x']}',
        y = '{row['y']}',
        number_of_lanes = {row['number_of_lanes']},
        type_21_count_interval_minutes = {row['type_21_count_interval_minutes']},
        type_21_programmable_rear_to_rear_headway_bin = {row['type_21_programmable_rear_to_rear_headway_bin']},
        type_21_program_id = {row['type_21_program_id']},
        speedbin1 = {row['speedbin1']},
        speedbin2 = {row['speedbin2']},
        speedbin3 = {row['speedbin3']},
        speedbin4 = {row['speedbin4']},
        speedbin5 = {row['speedbin5']},
        speedbin6 = {row['speedbin6']},
        speedbin7 = {row['speedbin7']},
        speedbin8 = {row['speedbin8']},
        speedbin9 = {row['speedbin9']},
        speedbin10 = {row['speedbin10']},
        type_10_vehicle_classification_scheme_primary = {row['type_10_vehicle_classification_scheme_primary']},
        type_10_vehicle_classification_scheme_secondary = {row['type_10_vehicle_classification_scheme_secondary']},
        type_10_maximum_gap_milliseconds = {row['type_10_maximum_gap_milliseconds']},
        type_10_maximum_differential_speed = {row['type_10_maximum_differential_speed']},
        type_30_summary_interval_minutes = {row['type_30_summary_interval_minutes']},
        type_30_vehicle_classification_scheme = {row['type_30_vehicle_classification_scheme']},
        type_70_summary_interval_minutes = {row['type_70_summary_interval_minutes']},
        type_70_vehicle_classification_scheme = {row['type_70_vehicle_classification_scheme']},
        type_70_maximum_gap_milliseconds = {row['type_70_maximum_gap_milliseconds']},
        type_70_maximum_differential_speed = {row['type_70_maximum_differential_speed']},
        type_70_error_bin_code = {row['type_70_error_bin_code']},
        instrumentation_description = '{row['instrumentation_description']}',
        document_url = '{row['document_url']}',
        date_processed = '{row['date_processed']}',
        growth_rate_use = '{row['growth_rate_use']}',
        total_light_positive_direction = {row['total_light_positive_direction']},
        total_light_negative_direction = {row['total_light_negative_direction']},
        total_light_vehicles = {row['total_light_vehicles']},
        total_heavy_positive_direction = {row['total_heavy_positive_direction']},
        total_heavy_negative_direction = {row['total_heavy_negative_direction']},
        total_heavy_vehicles = {row['total_heavy_vehicles']},
        total_short_heavy_positive_direction = {row['total_short_heavy_positive_direction']},
        total_short_heavy_negative_direction = {row['total_short_heavy_negative_direction']},
        total_short_heavy_vehicles = {row['total_short_heavy_vehicles']},
        total_medium_heavy_positive_direction = {row['total_medium_heavy_positive_direction']},
        total_medium_heavy_negative_direction = {row['total_medium_heavy_negative_direction']},
        total_medium_heavy_vehicles = {row['total_medium_heavy_vehicles']},
        total_long_heavy_positive_direction = {row['total_long_heavy_positive_direction']},
        total_long_heavy_negative_direction = {row['total_long_heavy_negative_direction']},
        total_long_heavy_vehicles = {row['total_long_heavy_vehicles']},
        total_vehicles_positive_direction = {row['total_vehicles_positive_direction']},
        total_vehicles_negative_direction = {row['total_vehicles_negative_direction']},
        total_vehicles = {row['total_vehicles']},
        average_speed_positive_direction = {row['average_speed_positive_direction']},
        average_speed_negative_direction = {row['average_speed_negative_direction']},
        average_speed = {row['average_speed']},
        average_speed_light_vehicles_positive_direction = {row['average_speed_light_vehicles_positive_direction']},
        average_speed_light_vehicles_negative_direction = {row['average_speed_light_vehicles_negative_direction']},
        average_speed_light_vehicles = {row['average_speed_light_vehicles']},
        average_speed_heavy_vehicles_positive_direction = {row['average_speed_heavy_vehicles_positive_direction']},
        average_speed_heavy_vehicles_negative_direction = {row['average_speed_heavy_vehicles_negative_direction']},
        average_speed_heavy_vehicles = {row['average_speed_heavy_vehicles']},
        truck_split_positive_direction = '{row['truck_split_positive_direction']}',
        truck_split_negative_direction = '{row['truck_split_negative_direction']}',
        truck_split_total = '{row['truck_split_total']}',
        estimated_axles_per_truck_positive_direction = {row['estimated_axles_per_truck_positive_direction']},
        estimated_axles_per_truck_negative_direction = {row['estimated_axles_per_truck_negative_direction']},
        estimated_axles_per_truck_total = {row['estimated_axles_per_truck_total']},
        percentage_speeding_positive_direction = {row['percentage_speeding_positive_direction']},
        percentage_speeding_negative_direction = {row['percentage_speeding_negative_direction']},
        percentage_speeding_total = {row['percentage_speeding_total']},
        vehicles_with_rear_to_rear_headway_less_than_2sec_positive_dire = {row['vehicles_with_rear_to_rear_headway_less_than_2sec_positive_dire']},
        vehicles_with_rear_to_rear_headway_less_than_2sec_negative_dire = {row['vehicles_with_rear_to_rear_headway_less_than_2sec_negative_dire']},
        vehicles_with_rear_to_rear_headway_less_than_2sec_total = {row['vehicles_with_rear_to_rear_headway_less_than_2sec_total']},
        estimated_e80_positive_direction = {row['estimated_e80_positive_direction']},
        estimated_e80_negative_direction = {row['estimated_e80_negative_direction']},
        estimated_e80_on_road = {row['estimated_e80_on_road']},
        adt_positive_direction = {row['adt_positive_direction']},
        adt_negative_direction = {row['adt_negative_direction']},
        adt_total = {row['adt_total']},
        adtt_positive_direction = {row['adtt_positive_direction']},
        adtt_negative_direction = {row['adtt_negative_direction']},
        adtt_total = {row['adtt_total']},
        highest_volume_per_hour_positive_direction = {row['highest_volume_per_hour_positive_direction']},
        highest_volume_per_hour_negative_direction = {row['highest_volume_per_hour_negative_direction']},
        highest_volume_per_hour_total = {row['highest_volume_per_hour_total']},
        "15th_highest_volume_per_hour_positive_direction" = {row['15th_highest_volume_per_hour_positive_direction']},
        "15th_highest_volume_per_hour_negative_direction" = {row['15th_highest_volume_per_hour_negative_direction']},
        "15th_highest_volume_per_hour_total" = {row['15th_highest_volume_per_hour_total']},
        "30th_highest_volume_per_hour_positive_direction" = {row['30th_highest_volume_per_hour_positive_direction']},
        "30th_highest_volume_per_hour_negative_direction" = {row['30th_highest_volume_per_hour_negative_direction']},
        "30th_highest_volume_per_hour_total" = {row['30th_highest_volume_per_hour_total']},
        "15th_percentile_speed_positive_direction" = {row['15th_percentile_speed_positive_direction']},
        "15th_percentile_speed_negative_direction" = {row['15th_percentile_speed_negative_direction']},
        "15th_percentile_speed_total" = {row['15th_percentile_speed_total']},
        "85th_percentile_speed_positive_direction" = {row['85th_percentile_speed_positive_direction']},
        "85th_percentile_speed_negative_direction" = {row['85th_percentile_speed_negative_direction']},
        "85th_percentile_speed_total" = {row['85th_percentile_speed_total']},
        "year" = {row['year']},
        positive_direction = '{row['positive_direction']}',
        negative_direction = '{row['negative_direction']}',
        avg_weekday_traffic = {row['avg_weekday_traffic']},
        number_of_days_counted = {row['number_of_days_counted']},
        duration_hours = {row['duration_hours']}
        WHERE site_id='{row['site_id']}' AND start_datetime='{row['start_datetime']}' AND end_datetime='{row['end_datetime']}'
        on conflict on constraint electronic_count_header_un do update set
    id = coalesce(excluded.id,id),
    header_id = coalesce(excluded.header_id,header_id),
    station_name = coalesce(excluded.station_name,station_name),
    x = coalesce(excluded.x,x),
    y = coalesce(excluded.y,y),
    number_of_lanes = coalesce(excluded.number_of_lanes,number_of_lanes),
    type_21_count_interval_minutes = coalesce(excluded.type_21_count_interval_minutes,type_21_count_interval_minutes),
    type_21_programmable_rear_to_rear_headway_bin = coalesce(excluded.type_21_programmable_rear_to_rear_headway_bin,type_21_programmable_rear_to_rear_headway_bin),
    type_21_program_id = coalesce(excluded.type_21_program_id,type_21_program_id),
    speedbin1 = coalesce(excluded.speedbin1,speedbin1),
    speedbin2 = coalesce(excluded.speedbin2,speedbin2),
    speedbin3 = coalesce(excluded.speedbin3,speedbin3),
    speedbin4 = coalesce(excluded.speedbin4,speedbin4),
    speedbin5 = coalesce(excluded.speedbin5,speedbin5),
    speedbin6 = coalesce(excluded.speedbin6,speedbin6),
    speedbin7 = coalesce(excluded.speedbin7,speedbin7),
    speedbin8 = coalesce(excluded.speedbin8,speedbin8),
    speedbin9 = coalesce(excluded.speedbin9,speedbin9),
    speedbin10 = coalesce(excluded.speedbin10,speedbin10),
    type_10_vehicle_classification_scheme_primary = coalesce(excluded.type_10_vehicle_classification_scheme_primary,type_10_vehicle_classification_scheme_primary),
    type_10_vehicle_classification_scheme_secondary = coalesce(excluded.type_10_vehicle_classification_scheme_secondary,type_10_vehicle_classification_scheme_secondary),
    type_10_maximum_gap_milliseconds = coalesce(excluded.type_10_maximum_gap_milliseconds,type_10_maximum_gap_milliseconds),
    type_10_maximum_differential_speed = coalesce(excluded.type_10_maximum_differential_speed,type_10_maximum_differential_speed),
    type_30_summary_interval_minutes = coalesce(excluded.type_30_summary_interval_minutes,type_30_summary_interval_minutes),
    type_30_vehicle_classification_scheme = coalesce(excluded.type_30_vehicle_classification_scheme,type_30_vehicle_classification_scheme),
    type_70_summary_interval_minutes = coalesce(excluded.type_70_summary_interval_minutes,type_70_summary_interval_minutes),
    type_70_vehicle_classification_scheme = coalesce(excluded.type_70_vehicle_classification_scheme,type_70_vehicle_classification_scheme),
    type_70_maximum_gap_milliseconds = coalesce(excluded.type_70_maximum_gap_milliseconds,type_70_maximum_gap_milliseconds),
    type_70_maximum_differential_speed = coalesce(excluded.type_70_maximum_differential_speed,type_70_maximum_differential_speed),
    type_70_error_bin_code = coalesce(excluded.type_70_error_bin_code,type_70_error_bin_code),
    instrumentation_description = coalesce(excluded.instrumentation_description,instrumentation_description),
    document_url = coalesce(excluded.document_url,document_url),
    date_processed = coalesce(excluded.date_processed,date_processed),
    growth_rate_use = coalesce(excluded.growth_rate_use,growth_rate_use),
    total_light_positive_direction = coalesce(excluded.total_light_positive_direction,total_light_positive_direction),
    total_light_negative_direction = coalesce(excluded.total_light_negative_direction,total_light_negative_direction),
    total_light_vehicles = coalesce(excluded.total_light_vehicles,total_light_vehicles),
    total_heavy_positive_direction = coalesce(excluded.total_heavy_positive_direction,total_heavy_positive_direction),
    total_heavy_negative_direction = coalesce(excluded.total_heavy_negative_direction,total_heavy_negative_direction),
    total_heavy_vehicles = coalesce(excluded.total_heavy_vehicles,total_heavy_vehicles),
    total_short_heavy_positive_direction = coalesce(excluded.total_short_heavy_positive_direction,total_short_heavy_positive_direction),
    total_short_heavy_negative_direction = coalesce(excluded.total_short_heavy_negative_direction,total_short_heavy_negative_direction),
    total_short_heavy_vehicles = coalesce(excluded.total_short_heavy_vehicles,total_short_heavy_vehicles),
    total_medium_heavy_positive_direction = coalesce(excluded.total_medium_heavy_positive_direction,total_medium_heavy_positive_direction),
    total_medium_heavy_negative_direction = coalesce(excluded.total_medium_heavy_negative_direction,total_medium_heavy_negative_direction),
    total_medium_heavy_vehicles = coalesce(excluded.total_medium_heavy_vehicles,total_medium_heavy_vehicles),
    total_long_heavy_positive_direction = coalesce(excluded.total_long_heavy_positive_direction,total_long_heavy_positive_direction),
    total_long_heavy_negative_direction = coalesce(excluded.total_long_heavy_negative_direction,total_long_heavy_negative_direction),
    total_long_heavy_vehicles = coalesce(excluded.total_long_heavy_vehicles,total_long_heavy_vehicles),
    total_vehicles_positive_direction = coalesce(excluded.total_vehicles_positive_direction,total_vehicles_positive_direction),
    total_vehicles_negative_direction = coalesce(excluded.total_vehicles_negative_direction,total_vehicles_negative_direction),
    total_vehicles = coalesce(excluded.total_vehicles,total_vehicles),
    average_speed_positive_direction = coalesce(excluded.average_speed_positive_direction,average_speed_positive_direction),
    average_speed_negative_direction = coalesce(excluded.average_speed_negative_direction,average_speed_negative_direction),
    average_speed = coalesce(excluded.average_speed,average_speed),
    average_speed_light_vehicles_positive_direction = coalesce(excluded.average_speed_light_vehicles_positive_direction,average_speed_light_vehicles_positive_direction),
    average_speed_light_vehicles_negative_direction = coalesce(excluded.average_speed_light_vehicles_negative_direction,average_speed_light_vehicles_negative_direction),
    average_speed_light_vehicles = coalesce(excluded.average_speed_light_vehicles,average_speed_light_vehicles),
    average_speed_heavy_vehicles_positive_direction = coalesce(excluded.average_speed_heavy_vehicles_positive_direction,average_speed_heavy_vehicles_positive_direction),
    average_speed_heavy_vehicles_negative_direction = coalesce(excluded.average_speed_heavy_vehicles_negative_direction,average_speed_heavy_vehicles_negative_direction),
    average_speed_heavy_vehicles = coalesce(excluded.average_speed_heavy_vehicles,average_speed_heavy_vehicles),
    truck_split_positive_direction = coalesce(excluded.truck_split_positive_direction,truck_split_positive_direction),
    truck_split_negative_direction = coalesce(excluded.truck_split_negative_direction,truck_split_negative_direction),
    truck_split_total = coalesce(excluded.truck_split_total,truck_split_total),
    estimated_axles_per_truck_positive_direction = coalesce(excluded.estimated_axles_per_truck_positive_direction,estimated_axles_per_truck_positive_direction),
    estimated_axles_per_truck_negative_direction = coalesce(excluded.estimated_axles_per_truck_negative_direction,estimated_axles_per_truck_negative_direction),
    estimated_axles_per_truck_total = coalesce(excluded.estimated_axles_per_truck_total,estimated_axles_per_truck_total),
    percentage_speeding_positive_direction = coalesce(excluded.percentage_speeding_positive_direction,percentage_speeding_positive_direction),
    percentage_speeding_negative_direction = coalesce(excluded.percentage_speeding_negative_direction,percentage_speeding_negative_direction),
    percentage_speeding_total = coalesce(excluded.percentage_speeding_total,percentage_speeding_total),
    vehicles_with_rear_to_rear_headway_less_than_2sec_positive_dire = coalesce(excluded.vehicles_with_rear_to_rear_headway_less_than_2sec_positive_dire,vehicles_with_rear_to_rear_headway_less_than_2sec_positive_dire),
    vehicles_with_rear_to_rear_headway_less_than_2sec_negative_dire = coalesce(excluded.vehicles_with_rear_to_rear_headway_less_than_2sec_negative_dire,vehicles_with_rear_to_rear_headway_less_than_2sec_negative_dire),
    vehicles_with_rear_to_rear_headway_less_than_2sec_total = coalesce(excluded.vehicles_with_rear_to_rear_headway_less_than_2sec_total,vehicles_with_rear_to_rear_headway_less_than_2sec_total),
    estimated_e80_positive_direction = coalesce(excluded.estimated_e80_positive_direction,estimated_e80_positive_direction),
    estimated_e80_negative_direction = coalesce(excluded.estimated_e80_negative_direction,estimated_e80_negative_direction),
    estimated_e80_on_road = coalesce(excluded.estimated_e80_on_road,estimated_e80_on_road),
    adt_positive_direction = coalesce(excluded.adt_positive_direction,adt_positive_direction),
    adt_negative_direction = coalesce(excluded.adt_negative_direction,adt_negative_direction),
    adt_total = coalesce(excluded.adt_total,adt_total),
    adtt_positive_direction = coalesce(excluded.adtt_positive_direction,adtt_positive_direction),
    adtt_negative_direction = coalesce(excluded.adtt_negative_direction,adtt_negative_direction),
    adtt_total = coalesce(excluded.adtt_total,adtt_total),
    highest_volume_per_hour_positive_direction = coalesce(excluded.highest_volume_per_hour_positive_direction,highest_volume_per_hour_positive_direction),
    highest_volume_per_hour_negative_direction = coalesce(excluded.highest_volume_per_hour_negative_direction,highest_volume_per_hour_negative_direction),
    highest_volume_per_hour_total = coalesce(excluded.highest_volume_per_hour_total,highest_volume_per_hour_total),
    "15th_highest_volume_per_hour_positive_direction" = coalesce(excluded."15th_highest_volume_per_hour_positive_direction","15th_highest_volume_per_hour_positive_direction"),
    "15th_highest_volume_per_hour_negative_direction" = coalesce(excluded."15th_highest_volume_per_hour_negative_direction","15th_highest_volume_per_hour_negative_direction"),
    "15th_highest_volume_per_hour_total" = coalesce(excluded."15th_highest_volume_per_hour_total","15th_highest_volume_per_hour_total"),
    "30th_highest_volume_per_hour_positive_direction" = coalesce(excluded."30th_highest_volume_per_hour_positive_direction","30th_highest_volume_per_hour_positive_direction"),
    "30th_highest_volume_per_hour_negative_direction" = coalesce(excluded."30th_highest_volume_per_hour_negative_direction","30th_highest_volume_per_hour_negative_direction"),
    "30th_highest_volume_per_hour_total" = coalesce(excluded."30th_highest_volume_per_hour_total","30th_highest_volume_per_hour_total"),
    "15th_percentile_speed_positive_direction" = coalesce(excluded."15th_percentile_speed_positive_direction","15th_percentile_speed_positive_direction"),
    "15th_percentile_speed_negative_direction" = coalesce(excluded."15th_percentile_speed_negative_direction","15th_percentile_speed_negative_direction"),
    "15th_percentile_speed_total" = coalesce(excluded."15th_percentile_speed_total","15th_percentile_speed_total"),
    "85th_percentile_speed_positive_direction" = coalesce(excluded."85th_percentile_speed_positive_direction","85th_percentile_speed_positive_direction"),
    "85th_percentile_speed_negative_direction" = coalesce(excluded."85th_percentile_speed_negative_direction","85th_percentile_speed_negative_direction"),
    "85th_percentile_speed_total" = coalesce(excluded."85th_percentile_speed_total","85th_percentile_speed_total"),
    "year" = coalesce(excluded."year","year"),
    positive_direction = coalesce(excluded.positive_direction,positive_direction),
    negative_direction = coalesce(excluded.negative_direction,negative_direction),
    avg_weekday_traffic = coalesce(excluded.avg_weekday_traffic,avg_weekday_traffic),
    number_of_days_counted = coalesce(excluded.number_of_days_counted,number_of_days_counted),
    duration_hours = coalesce(excluded.duration_hours,duration_hours)
    ;
        """
    return qry

def header_update_2(header_out):
    qry = f"""update
        trafc.electronic_count_header
    set
        type_21_count_interval_minutes = {header_out['type_21_count_interval_minutes'][0]},
        type_21_programmable_rear_to_rear_headway_bin = {header_out['type_21_programmable_rear_to_rear_headway_bin'][0]},
        type_21_program_id = {header_out['type_21_program_id'][0]},
        speedbin1 = {header_out['speedbin1'][0]},
        speedbin2 = {header_out['speedbin2'][0]},
        speedbin3 = {header_out['speedbin3'][0]},
        speedbin4 = {header_out['speedbin4'][0]},
        speedbin5 = {header_out['speedbin5'][0]},
        speedbin6 = {header_out['speedbin6'][0]},
        speedbin7 = {header_out['speedbin7'][0]},
        speedbin8 = {header_out['speedbin8'][0]},
        speedbin9 = {header_out['speedbin9'][0]},
        total_light_positive_direction = {header_out['total_light_positive_direction'][0]},
        total_light_negative_direction = {header_out['total_light_negative_direction'][0]},
        total_light_vehicles = {header_out['total_light_vehicles'][0]},
        total_heavy_positive_direction = {header_out['total_heavy_positive_direction'][0]},
        total_heavy_negative_direction = {header_out['total_heavy_negative_direction'][0]},
        total_heavy_vehicles = {header_out['total_heavy_vehicles'][0]},
        total_short_heavy_positive_direction = {header_out['total_short_heavy_positive_direction'][0]},
        total_short_heavy_negative_direction = {header_out['total_short_heavy_negative_direction'][0]},
        total_short_heavy_vehicles = {header_out['total_short_heavy_vehicles'][0]},
        total_medium_heavy_positive_direction = {header_out['total_medium_heavy_positive_direction'][0]},
        total_medium_heavy_negative_direction = {header_out['total_medium_heavy_negative_direction'][0]},
        total_medium_heavy_vehicles = {header_out['total_medium_heavy_vehicles'][0]},
        total_long_heavy_positive_direction = {header_out['total_long_heavy_positive_direction'][0]},
        total_long_heavy_negative_direction = {header_out['total_long_heavy_negative_direction'][0]},
        total_long_heavy_vehicles = {header_out['total_long_heavy_vehicles'][0]},
        total_vehicles_positive_direction = {header_out['total_vehicles_positive_direction'][0]},
        total_vehicles_negative_direction = {header_out['total_vehicles_negative_direction'][0]},
        total_vehicles = {header_out['total_vehicles'][0]},
        average_speed_positive_direction = {header_out['average_speed_positive_direction'][0]},
        average_speed_negative_direction = {header_out['average_speed_negative_direction'][0]},
        average_speed = {header_out['average_speed'][0]},
        average_speed_light_vehicles_positive_direction = {header_out['average_speed_light_vehicles_positive_direction'][0]},
        average_speed_light_vehicles_negative_direction = {header_out['average_speed_light_vehicles_negative_direction'][0]},
        average_speed_light_vehicles = {header_out['average_speed_light_vehicles'][0]},
        average_speed_heavy_vehicles_positive_direction = {header_out['average_speed_heavy_vehicles_positive_direction'][0]},
        average_speed_heavy_vehicles_negative_direction = {header_out['average_speed_heavy_vehicles_negative_direction'][0]},
        average_speed_heavy_vehicles = {header_out['average_speed_heavy_vehicles'][0]},
        truck_split_positive_direction = '{header_out['truck_split_positive_direction'][0]}',
        truck_split_negative_direction = '{header_out['truck_split_negative_direction'][0]}',
        truck_split_total = '{header_out['truck_split_total'][0]}',
        estimated_axles_per_truck_positive_direction = {header_out['estimated_axles_per_truck_positive_direction'][0]},
        estimated_axles_per_truck_negative_direction = {header_out['estimated_axles_per_truck_negative_direction'][0]},
        estimated_axles_per_truck_total = {header_out['estimated_axles_per_truck_total'][0]},
        percentage_speeding_positive_direction = {header_out['percentage_speeding_positive_direction'][0]},
        percentage_speeding_negative_direction = {header_out['percentage_speeding_negative_direction'][0]},
        percentage_speeding_total = {header_out['percentage_speeding_total'][0]},
        vehicles_with_rear_to_rear_headway_less_than_2sec_positive_dire = {header_out['vehicles_with_rear_to_rear_headway_less_than_2sec_positive_dire'][0]},
        vehicles_with_rear_to_rear_headway_less_than_2sec_negative_dire = {header_out['vehicles_with_rear_to_rear_headway_less_than_2sec_negative_dire'][0]},
        vehicles_with_rear_to_rear_headway_less_than_2sec_total = {header_out['vehicles_with_rear_to_rear_headway_less_than_2sec_total'][0]},
        estimated_e80_positive_direction = {header_out['estimated_e80_positive_direction'][0]},
        estimated_e80_negative_direction = {header_out['estimated_e80_negative_direction'][0]},
        estimated_e80_on_road = {header_out['estimated_e80_on_road'][0]},
        adt_positive_direction = {header_out['adt_positive_direction'][0]},
        adt_negative_direction = {header_out['adt_negative_direction'][0]},
        adt_total = {header_out['adt_total'][0]},
        adtt_positive_direction = {header_out['adtt_positive_direction'][0]},
        adtt_negative_direction = {header_out['adtt_negative_direction'][0]},
        adtt_total = {header_out['adtt_total'][0]},
        highest_volume_per_hour_positive_direction = {header_out['highest_volume_per_hour_positive_direction'][0]},
        highest_volume_per_hour_negative_direction = {header_out['highest_volume_per_hour_negative_direction'][0]},
        highest_volume_per_hour_total = {header_out['highest_volume_per_hour_total'][0]},
        "15th_highest_volume_per_hour_positive_direction" = {header_out['15th_highest_volume_per_hour_positive_direction'][0]},
        "15th_highest_volume_per_hour_negative_direction" = {header_out['15th_highest_volume_per_hour_negative_direction'][0]},
        "15th_highest_volume_per_hour_total" = {header_out['15th_highest_volume_per_hour_total'][0]},
        "30th_highest_volume_per_hour_positive_direction" = {header_out['30th_highest_volume_per_hour_positive_direction'][0]},
        "30th_highest_volume_per_hour_negative_direction" = {header_out['30th_highest_volume_per_hour_negative_direction'][0]},
        "30th_highest_volume_per_hour_total" = {header_out['30th_highest_volume_per_hour_total'][0]},
        "15th_percentile_speed_positive_direction" = {header_out['15th_percentile_speed_positive_direction'][0]},
        "15th_percentile_speed_negative_direction" = {header_out['15th_percentile_speed_negative_direction'][0]},
        "15th_percentile_speed_total" = {header_out['15th_percentile_speed_total'][0]},
        "85th_percentile_speed_positive_direction" = {header_out['85th_percentile_speed_positive_direction'][0]},
        "85th_percentile_speed_negative_direction" = {header_out['85th_percentile_speed_negative_direction'][0]},
        "85th_percentile_speed_total" = {header_out['85th_percentile_speed_total'][0]},
        "year" = {header_out['year'][0]},
        avg_weekday_traffic = {header_out['avg_weekday_traffic'][0]},
        number_of_days_counted = {header_out['number_of_days_counted'][0]},
        duration_hours = {header_out['duration_hours'][0]}
    where
        site_id = '{header_out['site_id'][0]}'
        and start_datetime = '{header_out['start_datetime'][0]}'
        and end_datetime = '{header_out['end_datetime'][0]}'
    on conflict on constraint electronic_count_header_un do update set
    id = coalesce(excluded.id,id),
    header_id = coalesce(excluded.header_id,header_id),
    station_name = coalesce(excluded.station_name,station_name),
    x = coalesce(excluded.x,x),
    y = coalesce(excluded.y,y),
    number_of_lanes = coalesce(excluded.number_of_lanes,number_of_lanes),
    type_21_count_interval_minutes = coalesce(excluded.type_21_count_interval_minutes,type_21_count_interval_minutes),
    type_21_programmable_rear_to_rear_headway_bin = coalesce(excluded.type_21_programmable_rear_to_rear_headway_bin,type_21_programmable_rear_to_rear_headway_bin),
    type_21_program_id = coalesce(excluded.type_21_program_id,type_21_program_id),
    speedbin1 = coalesce(excluded.speedbin1,speedbin1),
    speedbin2 = coalesce(excluded.speedbin2,speedbin2),
    speedbin3 = coalesce(excluded.speedbin3,speedbin3),
    speedbin4 = coalesce(excluded.speedbin4,speedbin4),
    speedbin5 = coalesce(excluded.speedbin5,speedbin5),
    speedbin6 = coalesce(excluded.speedbin6,speedbin6),
    speedbin7 = coalesce(excluded.speedbin7,speedbin7),
    speedbin8 = coalesce(excluded.speedbin8,speedbin8),
    speedbin9 = coalesce(excluded.speedbin9,speedbin9),
    speedbin10 = coalesce(excluded.speedbin10,speedbin10),
    type_10_vehicle_classification_scheme_primary = coalesce(excluded.type_10_vehicle_classification_scheme_primary,type_10_vehicle_classification_scheme_primary),
    type_10_vehicle_classification_scheme_secondary = coalesce(excluded.type_10_vehicle_classification_scheme_secondary,type_10_vehicle_classification_scheme_secondary),
    type_10_maximum_gap_milliseconds = coalesce(excluded.type_10_maximum_gap_milliseconds,type_10_maximum_gap_milliseconds),
    type_10_maximum_differential_speed = coalesce(excluded.type_10_maximum_differential_speed,type_10_maximum_differential_speed),
    type_30_summary_interval_minutes = coalesce(excluded.type_30_summary_interval_minutes,type_30_summary_interval_minutes),
    type_30_vehicle_classification_scheme = coalesce(excluded.type_30_vehicle_classification_scheme,type_30_vehicle_classification_scheme),
    type_70_summary_interval_minutes = coalesce(excluded.type_70_summary_interval_minutes,type_70_summary_interval_minutes),
    type_70_vehicle_classification_scheme = coalesce(excluded.type_70_vehicle_classification_scheme,type_70_vehicle_classification_scheme),
    type_70_maximum_gap_milliseconds = coalesce(excluded.type_70_maximum_gap_milliseconds,type_70_maximum_gap_milliseconds),
    type_70_maximum_differential_speed = coalesce(excluded.type_70_maximum_differential_speed,type_70_maximum_differential_speed),
    type_70_error_bin_code = coalesce(excluded.type_70_error_bin_code,type_70_error_bin_code),
    instrumentation_description = coalesce(excluded.instrumentation_description,instrumentation_description),
    document_url = coalesce(excluded.document_url,document_url),
    date_processed = coalesce(excluded.date_processed,date_processed),
    growth_rate_use = coalesce(excluded.growth_rate_use,growth_rate_use),
    total_light_positive_direction = coalesce(excluded.total_light_positive_direction,total_light_positive_direction),
    total_light_negative_direction = coalesce(excluded.total_light_negative_direction,total_light_negative_direction),
    total_light_vehicles = coalesce(excluded.total_light_vehicles,total_light_vehicles),
    total_heavy_positive_direction = coalesce(excluded.total_heavy_positive_direction,total_heavy_positive_direction),
    total_heavy_negative_direction = coalesce(excluded.total_heavy_negative_direction,total_heavy_negative_direction),
    total_heavy_vehicles = coalesce(excluded.total_heavy_vehicles,total_heavy_vehicles),
    total_short_heavy_positive_direction = coalesce(excluded.total_short_heavy_positive_direction,total_short_heavy_positive_direction),
    total_short_heavy_negative_direction = coalesce(excluded.total_short_heavy_negative_direction,total_short_heavy_negative_direction),
    total_short_heavy_vehicles = coalesce(excluded.total_short_heavy_vehicles,total_short_heavy_vehicles),
    total_medium_heavy_positive_direction = coalesce(excluded.total_medium_heavy_positive_direction,total_medium_heavy_positive_direction),
    total_medium_heavy_negative_direction = coalesce(excluded.total_medium_heavy_negative_direction,total_medium_heavy_negative_direction),
    total_medium_heavy_vehicles = coalesce(excluded.total_medium_heavy_vehicles,total_medium_heavy_vehicles),
    total_long_heavy_positive_direction = coalesce(excluded.total_long_heavy_positive_direction,total_long_heavy_positive_direction),
    total_long_heavy_negative_direction = coalesce(excluded.total_long_heavy_negative_direction,total_long_heavy_negative_direction),
    total_long_heavy_vehicles = coalesce(excluded.total_long_heavy_vehicles,total_long_heavy_vehicles),
    total_vehicles_positive_direction = coalesce(excluded.total_vehicles_positive_direction,total_vehicles_positive_direction),
    total_vehicles_negative_direction = coalesce(excluded.total_vehicles_negative_direction,total_vehicles_negative_direction),
    total_vehicles = coalesce(excluded.total_vehicles,total_vehicles),
    average_speed_positive_direction = coalesce(excluded.average_speed_positive_direction,average_speed_positive_direction),
    average_speed_negative_direction = coalesce(excluded.average_speed_negative_direction,average_speed_negative_direction),
    average_speed = coalesce(excluded.average_speed,average_speed),
    average_speed_light_vehicles_positive_direction = coalesce(excluded.average_speed_light_vehicles_positive_direction,average_speed_light_vehicles_positive_direction),
    average_speed_light_vehicles_negative_direction = coalesce(excluded.average_speed_light_vehicles_negative_direction,average_speed_light_vehicles_negative_direction),
    average_speed_light_vehicles = coalesce(excluded.average_speed_light_vehicles,average_speed_light_vehicles),
    average_speed_heavy_vehicles_positive_direction = coalesce(excluded.average_speed_heavy_vehicles_positive_direction,average_speed_heavy_vehicles_positive_direction),
    average_speed_heavy_vehicles_negative_direction = coalesce(excluded.average_speed_heavy_vehicles_negative_direction,average_speed_heavy_vehicles_negative_direction),
    average_speed_heavy_vehicles = coalesce(excluded.average_speed_heavy_vehicles,average_speed_heavy_vehicles),
    truck_split_positive_direction = coalesce(excluded.truck_split_positive_direction,truck_split_positive_direction),
    truck_split_negative_direction = coalesce(excluded.truck_split_negative_direction,truck_split_negative_direction),
    truck_split_total = coalesce(excluded.truck_split_total,truck_split_total),
    estimated_axles_per_truck_positive_direction = coalesce(excluded.estimated_axles_per_truck_positive_direction,estimated_axles_per_truck_positive_direction),
    estimated_axles_per_truck_negative_direction = coalesce(excluded.estimated_axles_per_truck_negative_direction,estimated_axles_per_truck_negative_direction),
    estimated_axles_per_truck_total = coalesce(excluded.estimated_axles_per_truck_total,estimated_axles_per_truck_total),
    percentage_speeding_positive_direction = coalesce(excluded.percentage_speeding_positive_direction,percentage_speeding_positive_direction),
    percentage_speeding_negative_direction = coalesce(excluded.percentage_speeding_negative_direction,percentage_speeding_negative_direction),
    percentage_speeding_total = coalesce(excluded.percentage_speeding_total,percentage_speeding_total),
    vehicles_with_rear_to_rear_headway_less_than_2sec_positive_dire = coalesce(excluded.vehicles_with_rear_to_rear_headway_less_than_2sec_positive_dire,vehicles_with_rear_to_rear_headway_less_than_2sec_positive_dire),
    vehicles_with_rear_to_rear_headway_less_than_2sec_negative_dire = coalesce(excluded.vehicles_with_rear_to_rear_headway_less_than_2sec_negative_dire,vehicles_with_rear_to_rear_headway_less_than_2sec_negative_dire),
    vehicles_with_rear_to_rear_headway_less_than_2sec_total = coalesce(excluded.vehicles_with_rear_to_rear_headway_less_than_2sec_total,vehicles_with_rear_to_rear_headway_less_than_2sec_total),
    estimated_e80_positive_direction = coalesce(excluded.estimated_e80_positive_direction,estimated_e80_positive_direction),
    estimated_e80_negative_direction = coalesce(excluded.estimated_e80_negative_direction,estimated_e80_negative_direction),
    estimated_e80_on_road = coalesce(excluded.estimated_e80_on_road,estimated_e80_on_road),
    adt_positive_direction = coalesce(excluded.adt_positive_direction,adt_positive_direction),
    adt_negative_direction = coalesce(excluded.adt_negative_direction,adt_negative_direction),
    adt_total = coalesce(excluded.adt_total,adt_total),
    adtt_positive_direction = coalesce(excluded.adtt_positive_direction,adtt_positive_direction),
    adtt_negative_direction = coalesce(excluded.adtt_negative_direction,adtt_negative_direction),
    adtt_total = coalesce(excluded.adtt_total,adtt_total),
    highest_volume_per_hour_positive_direction = coalesce(excluded.highest_volume_per_hour_positive_direction,highest_volume_per_hour_positive_direction),
    highest_volume_per_hour_negative_direction = coalesce(excluded.highest_volume_per_hour_negative_direction,highest_volume_per_hour_negative_direction),
    highest_volume_per_hour_total = coalesce(excluded.highest_volume_per_hour_total,highest_volume_per_hour_total),
    "15th_highest_volume_per_hour_positive_direction" = coalesce(excluded."15th_highest_volume_per_hour_positive_direction","15th_highest_volume_per_hour_positive_direction"),
    "15th_highest_volume_per_hour_negative_direction" = coalesce(excluded."15th_highest_volume_per_hour_negative_direction","15th_highest_volume_per_hour_negative_direction"),
    "15th_highest_volume_per_hour_total" = coalesce(excluded."15th_highest_volume_per_hour_total","15th_highest_volume_per_hour_total"),
    "30th_highest_volume_per_hour_positive_direction" = coalesce(excluded."30th_highest_volume_per_hour_positive_direction","30th_highest_volume_per_hour_positive_direction"),
    "30th_highest_volume_per_hour_negative_direction" = coalesce(excluded."30th_highest_volume_per_hour_negative_direction","30th_highest_volume_per_hour_negative_direction"),
    "30th_highest_volume_per_hour_total" = coalesce(excluded."30th_highest_volume_per_hour_total","30th_highest_volume_per_hour_total"),
    "15th_percentile_speed_positive_direction" = coalesce(excluded."15th_percentile_speed_positive_direction","15th_percentile_speed_positive_direction"),
    "15th_percentile_speed_negative_direction" = coalesce(excluded."15th_percentile_speed_negative_direction","15th_percentile_speed_negative_direction"),
    "15th_percentile_speed_total" = coalesce(excluded."15th_percentile_speed_total","15th_percentile_speed_total"),
    "85th_percentile_speed_positive_direction" = coalesce(excluded."85th_percentile_speed_positive_direction","85th_percentile_speed_positive_direction"),
    "85th_percentile_speed_negative_direction" = coalesce(excluded."85th_percentile_speed_negative_direction","85th_percentile_speed_negative_direction"),
    "85th_percentile_speed_total" = coalesce(excluded."85th_percentile_speed_total","85th_percentile_speed_total"),
    "year" = coalesce(excluded."year","year"),
    positive_direction = coalesce(excluded.positive_direction,positive_direction),
    negative_direction = coalesce(excluded.negative_direction,negative_direction),
    avg_weekday_traffic = coalesce(excluded.avg_weekday_traffic,avg_weekday_traffic),
    number_of_days_counted = coalesce(excluded.number_of_days_counted,number_of_days_counted),
    duration_hours = coalesce(excluded.duration_hours,duration_hours)
    ;
    """
    return qry

def wim_stations_header_update(header_id):
    SELECT_TYPE10_QRY = f"""SELECT 
        *
        FROM trafc.electronic_count_data_type_10 t10
        left join traf_lu.vehicle_classes_scheme_08 c on c.id = t10.vehicle_class_code_primary_scheme
        where t10.header_id = '{header_id}'
        """
    AXLE_SPACING_SELECT_QRY = f"""SELECT 
        t10.id,
        t10.header_id, 
        t10.start_datetime,
        t10.edit_code,
        t10.vehicle_class_code_primary_scheme, 
        t10.vehicle_class_code_secondary_scheme,
        t10.direction,
        t10.axle_count,
        axs.axle_spacing_number,
        axs.axle_spacing_cm,
        FROM trafc.electronic_count_data_type_10 t10
        left join trafc.traffic_e_type10_axle_spacing axs ON axs.type10_id = t10.data_id
        where t10.header_id = '{header_id}'
        """
    WHEEL_MASS_SELECT_QRY = f"""SELECT 
        t10.id,
        t10.header_id, 
        t10.start_datetime,
        t10.edit_code,
        t10.vehicle_class_code_primary_scheme, 
        t10.vehicle_class_code_secondary_scheme,
        t10.direction,
        t10.axle_count,
        wm.wheel_mass_number,
        wm.wheel_mass,
        vm.kg as vehicle_mass_limit_kg,
        sum(wm.wheel_mass)*2 over(partition by t10.id) as gross_mass
        FROM trafc.electronic_count_data_type_10 t10
        left join trafc.traffic_e_type10_wheel_mass wm ON wm.type10_id = t10.data_id
        left join traf_lu.gross_vehicle_mass_limits vm on vm.number_of_axles = t10.axle_count
        where t10.header_id = '{header_id}'
        """
    df = pd.read_sql_query(AXLE_SPACING_SELECT_QRY,config.ENGINE)
    df3 = pd.read_sql_query(WHEEL_MASS_SELECT_QRY,config.ENGINE)

    # axle_load_limits = pd.read_sql_query("SELECT * FROM traf_lu.axle_load_limits",config.ENGINE)
    # gross_vehicle_mass_limits = pd.read_sql_query("SELECT * FROM traf_lu.gross_vehicle_mass_limits",config.ENGINE)

    # UPDATE_STRING = f"""
    # update
    #     trafc.electronic_count_header set
    #     egrl_percent {},
    #     egrw_percent {},
    #     mean_equivalent_axle_mass = {(df3.groupby(pd.Grouper(key='id')).mean()*2).mean().round(2)},
    #     mean_equivalent_axle_mass_positive_direction = {(df3.loc[df3['direction']=='P'].groupby(pd.Grouper(key='id')).mean()*2).mean().round(2)},
    #     mean_equivalent_axle_mass_negative_direction = {(df3.loc[df3['direction']=='N'].groupby(pd.Grouper(key='id')).mean()*2).mean().round(2)},
    #     mean_axle_spacing = {(df3.groupby(pd.Grouper(key='id')).mean()).mean()['axle_spacing_number'].round()},
    #     mean_axle_spacing_positive_direction = {(df3.loc[df3['direction']=='P'].groupby(pd.Grouper(key='id')).mean()).mean()['axle_spacing_number'].round()},
    #     mean_axle_spacing_negative_direction = {(df3.loc[df3['direction']=='N'].groupby(pd.Grouper(key='id')).mean()).mean()['axle_spacing_number'].round()},
    #     e80_per_axle = {((df3['wheel_mass']*2/8200)**4.2).sum().round()},
    #     e80_per_axle_positive_direction = {((df3.loc[df3['direction']=='P']['wheel_mass']*2/8200)**4.2).sum().round()},
    #     e80_per_axle_negative_direction = {((df3.loc[df3['direction']=='N']['wheel_mass']*2/8200)**4.2).sum().round()},
    #     olhv = {len(df3.loc[df3['gross_mass']>df3['vehicle_mass_limit_kg']]['id'].unique())},
    #     olhv_positive_direction = {len(df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])&(df3['direction']=='P')]['id'].unique())},
    #     olhv_negative_direction = {len(df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])&(df3['direction']=='N')]['id'].unique())},
    #     olhv_percent = {round(((len(df3.loc[df3['gross_mass']>df3['vehicle_mass_limit_kg']]['id'].unique())/len(df3['id'].unique()))*100),2)},
    #     olhv_percent_positive_direction = {round(((len(df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])&(df3['direction']=='P')]['id'].unique())/len(df3.loc[df3['direction']=='P']['id'].unique()))*100),2)},
    #     olhv_percent_negative_direction = {round(((len(df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])&(df3['direction']=='N')]['id'].unique())/len(df3.loc[df3['direction']=='N']['id'].unique()))*100),2)},
    #     tonnage_generated = {((df3['wheel_mass']*2).sum()/1000).round().astype(int)},
    #     tonnage_generated_positive_direction = {((df3.loc[df3['direction']=='P']['wheel_mass']*2).sum()/1000).round().astype(int)},
    #     tonnage_generated_negative_direction = {((df3.loc[df3['direction']=='N']['wheel_mass']*2).sum()/1000).round().astype(int)},
    #     olton = {(df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])]['wheel_mass']*2).sum().astype(int)},
    #     olton_positive_direction = {(df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])&(df3['direction']=='P')]['wheel_mass']*2).sum().astype(int)},
    #     olton_negative_direction = {(df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])&(df3['direction']=='N')]['wheel_mass']*2).sum().astype(int)},
    #     olton_percent = {round(((df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])]['wheel_mass']*2).sum()/(df3['wheel_mass']*2).sum())*100,2)},
    #     olton_percent_positive_direction = {round(((df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])&(df3['direction']=='P')]['wheel_mass']*2).sum()/(df3.loc[df3['direction']=='P']['wheel_mass']*2).sum())*100,2)},
    #     olton_percent_negative_direction = {round(((df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])&(df3['direction']=='N')]['wheel_mass']*2).sum()/(df3.loc[df3['direction']=='N']['wheel_mass']*2).sum())*100,2)},
    #     ole80 = {((df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])]['wheel_mass']*2/8200)**4.2).sum().round()},
    #     ole80_positive_direction = {((df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])&(df3['direction']=='P')]['wheel_mass']*2/8200)**4.2).sum().round()},
    #     ole80_negative_direction = {((df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])&(df3['direction']=='N')]['wheel_mass']*2/8200)**4.2).sum().round()},
    #     ole80_percent = {((((df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])]['wheel_mass']*2/8200)**4.2).sum().round()/((df3['wheel_mass']*2/8200)**4.2).sum().round())*100).round(2)},
    #     ole80_percent_positive_direction = {((((df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])&(df3['direction']=='P')]['wheel_mass']*2/8200)**4.2).sum().round()/((df3.loc[df3['direction']=='P']['wheel_mass']*2/8200)**4.2).sum().round())*100).round(2)},
    #     ole80_percent_negative_direction = {((((df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])&(df3['direction']=='N')]['wheel_mass']*2/8200)**4.2).sum().round()/((df3.loc[df3['direction']=='N']['wheel_mass']*2/8200)**4.2).sum().round())*100).round(2)},
    #     xe80 = {},
    #     xe80_positive_direction = {},
    #     xe80_negative_direction = {},
    #     xe80_percent = {},
    #     xe80_percent_positive_direction = {},
    #     xe80_percent_negative_direction = {},
    #     e80_per_day = {((((df3['wheel_mass']*2/8200)**4.2).sum().round()/df3.groupby(pd.Grouper(key='start_datetime',freq='D')).count().count()[0])*100).round(2)},
    #     e80_per_day_positive_direction = {((((df3.loc[df3['direction']=='P']['wheel_mass']*2/8200)**4.2).sum().round()/df3.loc[df3['direction']=='P'].groupby(pd.Grouper(key='start_datetime',freq='D')).count().count()[0])*100).round(2)},
    #     e80_per_day_negative_direction = {((((df3.loc[df3['direction']=='N']['wheel_mass']*2/8200)**4.2).sum().round()/df3.loc[df3['direction']=='N'].groupby(pd.Grouper(key='start_datetime',freq='D')).count().count()[0])*100).round(2)},
    #     e80_per_heavy_vehicle = {((((df3.loc[df3['vehicle_class_code_primary_scheme']>3]['wheel_mass']*2/8200)**4.2).sum().round()/df3.loc[df3['vehicle_class_code_primary_scheme']>3].count()[0])*100).round(2)},
    #     e80_per_heavy_vehicle_positive_direction = {((((df3.loc[(df3['vehicle_class_code_primary_scheme']>3)&(df3['direction']=='P')]['wheel_mass']*2/8200)**4.2).sum().round()/df3.loc[(df3['vehicle_class_code_primary_scheme']>3)&(df3['direction']=='P')].count()[0])*100).round(2)},
    #     e80_per_heavy_vehicle_negative_direction = {((((df3.loc[(df3['vehicle_class_code_primary_scheme']>3)&(df3['direction']=='N')]['wheel_mass']*2/8200)**4.2).sum().round()/df3.loc[(df3['vehicle_class_code_primary_scheme']>3)&(df3['direction']=='N')].count()[0])*100).round(2)},
    #     worst_steering_single_axle_cnt = {},
    #     worst_steering_single_axle_olhv_perc = {},
    #     worst_steering_single_axle_tonperhv = {},
    #     worst_steering_double_axle_cnt = {},
    #     worst_steering_double_axle_olhv_perc = {},
    #     worst_steering_double_axle_tonperhv = {},
    #     worst_non_steering_single_axle_cnt = {},
    #     worst_non_steering_single_axle_olhv_perc = {},
    #     worst_non_steering_single_axle_tonperhv = {},
    #     worst_non_steering_double_axle_cnt = {},
    #     worst_non_steering_double_axle_olhv_perc = {},
    #     worst_non_steering_double_axle_tonperhv = {},
    #     worst_triple_axle_cnt = {},
    #     worst_triple_axle_olhv_perc = {},
    #     worst_triple_axle_tonperhv = {},
    #     bridge_formula_cnt = {(18000 + 2.1 * (df2.loc[df2['axle_spacing_number']>1].groupby('id')['axle_spacing_cm'].sum().mean().round(2))).round(2)},
    #     bridge_formula_olhv_perc = {},
    #     bridge_formula_tonperhv = {},
    #     gross_formula_cnt = {},
    #     gross_formula_olhv_perc = {},
    #     gross_formula_tonperhv = {},
    #     total_avg_cnt = {},
    #     total_avg_olhv_perc = {},
    #     total_avg_tonperhv = {},
    #     worst_steering_single_axle_cnt_positive_direciton = {},
    #     worst_steering_single_axle_olhv_perc_positive_direciton = {},
    #     worst_steering_single_axle_tonperhv_positive_direciton = {},
    #     worst_steering_double_axle_cnt_positive_direciton = {},
    #     worst_steering_double_axle_olhv_perc_positive_direciton = {},
    #     worst_steering_double_axle_tonperhv_positive_direciton = {},
    #     worst_non_steering_single_axle_cnt_positive_direciton = {},
    #     worst_non_steering_single_axle_olhv_perc_positive_direciton = {},
    #     worst_non_steering_single_axle_tonperhv_positive_direciton = {},
    #     worst_non_steering_double_axle_cnt_positive_direciton = {},
    #     worst_non_steering_double_axle_olhv_perc_positive_direciton = {},
    #     worst_non_steering_double_axle_tonperhv_positive_direciton = {},
    #     worst_triple_axle_cnt_positive_direciton = {},
    #     worst_triple_axle_olhv_perc_positive_direciton = {},
    #     worst_triple_axle_tonperhv_positive_direciton = {},
    #     bridge_formula_cnt_positive_direciton = {},
    #     bridge_formula_olhv_perc_positive_direciton = {},
    #     bridge_formula_tonperhv_positive_direciton = {},
    #     gross_formula_cnt_positive_direciton = {},
    #     gross_formula_olhv_perc_positive_direciton = {},
    #     gross_formula_tonperhv_positive_direciton = {},
    #     total_avg_cnt_positive_direciton = {},
    #     total_avg_olhv_perc_positive_direciton = {},
    #     total_avg_tonperhv_positive_direciton = {},
    #     worst_steering_single_axle_cnt_negative_direciton = {},
    #     worst_steering_single_axle_olhv_perc_negative_direciton = {},
    #     worst_steering_single_axle_tonperhv_negative_direciton = {},
    #     worst_steering_double_axle_cnt_negative_direciton = {},
    #     worst_steering_double_axle_olhv_perc_negative_direciton = {},
    #     worst_steering_double_axle_tonperhv_negative_direciton = {},
    #     worst_non_steering_single_axle_cnt_negative_direciton = {},
    #     worst_non_steering_single_axle_olhv_perc_negative_direciton = {},
    #     worst_non_steering_single_axle_tonperhv_negative_direciton = {},
    #     worst_non_steering_double_axle_cnt_negative_direciton = {},
    #     worst_non_steering_double_axle_olhv_perc_negative_direciton = {},
    #     worst_non_steering_double_axle_tonperhv_negative_direciton = {},
    #     worst_triple_axle_cnt_negative_direciton = {},
    #     worst_triple_axle_olhv_perc_negative_direciton = {},
    #     worst_triple_axle_tonperhv_negative_direciton = {},
    #     bridge_formula_cnt_negative_direciton = {},
    #     bridge_formula_olhv_perc_negative_direciton = {},
    #     bridge_formula_tonperhv_negative_direciton = {},
    #     gross_formula_cnt_negative_direciton = {},
    #     gross_formula_olhv_perc_negative_direciton = {},
    #     gross_formula_tonperhv_negative_direciton = {},
    #     total_avg_cnt_negative_direciton = {},
    #     total_avg_olhv_perc_negative_direciton = {},
    #     total_avg_tonperhv_negative_direciton = {}
    # where
    #     header_id = '{header_id}';
    # """

    INSERT_STRING = f"""
    insert into trafc.electronic_count_header_hswim (
        header_id,
        egrl_percent,
        egrl_percent_positive_direction,
        egrl_percent_negative_direction,
        egrw_percent ,
        egrw_percent_positive_direction ,
        egrw_percent_negative_direction ,
        num_weighed,
        num_weighed_positive_direction,
        num_weighed_negative_direction,
        mean_equivalent_axle_mass,
        mean_equivalent_axle_mass_positive_direction,
        mean_equivalent_axle_mass_negative_direction,
        mean_axle_spacing,
        mean_axle_spacing_positive_direction,
        mean_axle_spacing_negative_direction,
        e80_per_axle,
        e80_per_axle_positive_direction,
        e80_per_axle_negative_direction,
        olhv,
        olhv_positive_direction,
        olhv_negative_direction,
        olhv_percent,
        olhv_percent_positive_direction,
        olhv_percent_negative_direction,
        tonnage_generated,
        tonnage_generated_positive_direction,
        tonnage_generated_negative_direction,
        olton,
        olton_positive_direction,
        olton_negative_direction,
        olton_percent,
        olton_percent_positive_direction,
        olton_percent_negative_direction,
        ole80,
        ole80_positive_direction,
        ole80_negative_direction,
        ole80_percent,
        ole80_percent_positive_direction,
        ole80_percent_negative_direction,
        xe80,
        xe80_positive_direction,
        xe80_negative_direction,
        xe80_percent,
        xe80_percent_positive_direction,
        xe80_percent_negative_direction,
        e80_per_day,
        e80_per_day_positive_direction,
        e80_per_day_negative_direction,
        e80_per_heavy_vehicle,
        e80_per_heavy_vehicle_positive_direction,
        e80_per_heavy_vehicle_negative_direction,
        worst_steering_single_axle_cnt,
        worst_steering_single_axle_olhv_perc,
        worst_steering_single_axle_tonperhv,
        worst_steering_double_axle_cnt,
        worst_steering_double_axle_olhv_perc,
        worst_steering_double_axle_tonperhv,
        worst_non_steering_single_axle_cnt,
        worst_non_steering_single_axle_olhv_perc,
        worst_non_steering_single_axle_tonperhv,
        worst_non_steering_double_axle_cnt,
        worst_non_steering_double_axle_olhv_perc,
        worst_non_steering_double_axle_tonperhv,
        worst_triple_axle_cnt,
        worst_triple_axle_olhv_perc,
        worst_triple_axle_tonperhv,
        bridge_formula_cnt,
        bridge_formula_olhv_perc,
        bridge_formula_tonperhv,
        gross_formula_cnt,
        gross_formula_olhv_perc,
        gross_formula_tonperhv,
        total_avg_cnt,
        total_avg_olhv_perc,
        total_avg_tonperhv,
        worst_steering_single_axle_cnt_positive_direciton,
        worst_steering_single_axle_olhv_perc_positive_direciton,
        worst_steering_single_axle_tonperhv_positive_direciton,
        worst_steering_double_axle_cnt_positive_direciton,
        worst_steering_double_axle_olhv_perc_positive_direciton,
        worst_steering_double_axle_tonperhv_positive_direciton,
        worst_non_steering_single_axle_cnt_positive_direciton,
        worst_non_steering_single_axle_olhv_perc_positive_direciton,
        worst_non_steering_single_axle_tonperhv_positive_direciton,
        worst_non_steering_double_axle_cnt_positive_direciton,
        worst_non_steering_double_axle_olhv_perc_positive_direciton,
        worst_non_steering_double_axle_tonperhv_positive_direciton,
        worst_triple_axle_cnt_positive_direciton,
        worst_triple_axle_olhv_perc_positive_direciton,
        worst_triple_axle_tonperhv_positive_direciton,
        bridge_formula_cnt_positive_direciton,
        bridge_formula_olhv_perc_positive_direciton,
        bridge_formula_tonperhv_positive_direciton,
        gross_formula_cnt_positive_direciton,
        gross_formula_olhv_perc_positive_direciton,
        gross_formula_tonperhv_positive_direciton,
        total_avg_cnt_positive_direciton,
        total_avg_olhv_perc_positive_direciton,
        total_avg_tonperhv_positive_direciton,
        worst_steering_single_axle_cnt_negative_direciton,
        worst_steering_single_axle_olhv_perc_negative_direciton,
        worst_steering_single_axle_tonperhv_negative_direciton,
        worst_steering_double_axle_cnt_negative_direciton,
        worst_steering_double_axle_olhv_perc_negative_direciton,
        worst_steering_double_axle_tonperhv_negative_direciton,
        worst_non_steering_single_axle_cnt_negative_direciton,
        worst_non_steering_single_axle_olhv_perc_negative_direciton,
        worst_non_steering_single_axle_tonperhv_negative_direciton,
        worst_non_steering_double_axle_cnt_negative_direciton,
        worst_non_steering_double_axle_olhv_perc_negative_direciton,
        worst_non_steering_double_axle_tonperhv_negative_direciton,
        worst_triple_axle_cnt_negative_direciton,
        worst_triple_axle_olhv_perc_negative_direciton,
        worst_triple_axle_tonperhv_negative_direciton,
        bridge_formula_cnt_negative_direciton,
        bridge_formula_olhv_perc_negative_direciton,
        bridge_formula_tonperhv_negative_direciton,
        gross_formula_cnt_negative_direciton,
        gross_formula_olhv_perc_negative_direciton,
        gross_formula_tonperhv_negative_direciton,
        total_avg_cnt_negative_direciton,
        total_avg_olhv_perc_negative_direciton,
        total_avg_tonperhv_negative_direciton
        )
    values(
        '{header_id}',
        , --  egrl_percent,
        , --  egrl_percent_positive_direction,
        , --  egrl_percent_negative_direction,
        , --  egrw_percent ,
        , --  egrw_percent_positive_direction ,
        , --  egrw_percent_negative_direction ,
        {df3.groupby(pd.Grouper(key='id')).count().count()[0]}, --  num_weighed,
        {df3.loc[df3['direction']=='P'].groupby(pd.Grouper(key='id')).count().count()[0]}, --  num_weighed_positive_direction,
        {df3.loc[df3['direction']=='N'].groupby(pd.Grouper(key='id')).count().count()[0]}, --  num_weighed_negative_direction,
        {(df.groupby(pd.Grouper(key='id')).mean()*2).mean().round(2)}, --mean_equivalent_axle_mass
        {(df.loc[df['direction']=='P'].groupby(pd.Grouper(key='id')).mean()*2).mean().round(2)}, --mean_equivalent_axle_mass_positive_direction
        {(df.loc[df['direction']=='N'].groupby(pd.Grouper(key='id')).mean()*2).mean().round(2)}, --mean_equivalent_axle_mass_negative_direction
        {(df.groupby(pd.Grouper(key='id')).mean()).mean()['axle_spacing_number'].round()}, --mean_axle_spacing
        {(df.loc[df['direction']=='P'].groupby(pd.Grouper(key='id')).mean()).mean()['axle_spacing_number'].round()}, --mean_axle_spacing_positive_direction
        {(df.loc[df['direction']=='N'].groupby(pd.Grouper(key='id')).mean()).mean()['axle_spacing_number'].round()}, --mean_axle_spacing_negative_direction
        {(df3['wheel_mass']/8200)**4.2}, --e80_per_axle
        {(df3.loc[df3['direction']=='P']['wheel_mass']/8200)**4.2}, --e80_per_axle_positive_direction
        {(df3.loc[df3['direction']=='N']['wheel_mass']/8200)**4.2}, --e80_per_axle_negative_direction
        {len(df3.loc[df3['gross_mass']>df3['vehicle_mass_limit_kg']]['id'].unique())},    -- olhv
        {len(df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])&(df3['direction']=='P')]['id'].unique())},    -- olhv_positive_direction
        {len(df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])&(df3['direction']=='N')]['id'].unique())},    -- olhv_negative_direction
        {round(((len(df3.loc[df3['gross_mass']>df3['vehicle_mass_limit_kg']]['id'].unique())/len(df3['id'].unique()))*100),2)},    -- olhv_percent
        {round(((len(df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])&(df3['direction']=='P')]['id'].unique())/len(df3.loc[df3['direction']=='P']['id'].unique()))*100),2)},    -- olhv_percent_positive_direction
        {round(((len(df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])&(df3['direction']=='N')]['id'].unique())/len(df3.loc[df3['direction']=='N']['id'].unique()))*100),2)},    -- olhv_percent_negative_direction
        {((df3['wheel_mass']*2).sum()/1000).round().astype(int)},    -- tonnage_generated
        {((df3.loc[df3['direction']=='P']['wheel_mass']*2).sum()/1000).round().astype(int)},    -- tonnage_generated_positive_direction
        {((df3.loc[df3['direction']=='N']['wheel_mass']*2).sum()/1000).round().astype(int)},    -- tonnage_generated_negative_direction
        {(df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])]['wheel_mass']*2).sum().astype(int)},    -- olton
        {(df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])&(df3['direction']=='P')]['wheel_mass']*2).sum().astype(int)},    -- olton_positive_direction
        {(df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])&(df3['direction']=='N')]['wheel_mass']*2).sum().astype(int)},    -- olton_negative_direction
        {round(((df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])]['wheel_mass']*2).sum()/(df3['wheel_mass']*2).sum())*100,2)},    -- olton_percent
        {round(((df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])&(df3['direction']=='P')]['wheel_mass']*2).sum()/(df3.loc[df3['direction']=='P']['wheel_mass']*2).sum())*100,2)},    -- olton_percent_positive_direction
        {round(((df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])&(df3['direction']=='N')]['wheel_mass']*2).sum()/(df3.loc[df3['direction']=='N']['wheel_mass']*2).sum())*100,2)},    -- olton_percent_negative_direction
        {((df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])]['wheel_mass']*2/8200)**4.2).sum().round()},    -- ole80
        {((df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])&(df3['direction']=='P')]['wheel_mass']*2/8200)**4.2).sum().round()},    -- ole80_positive_direction
        {((df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])&(df3['direction']=='N')]['wheel_mass']*2/8200)**4.2).sum().round()},    -- ole80_negative_direction
        {((((df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])]['wheel_mass']*2/8200)**4.2).sum().round()/((df3['wheel_mass']*2/8200)**4.2).sum().round())*100).round(2)},    -- ole80_percent
        {((((df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])&(df3['direction']=='P')]['wheel_mass']*2/8200)**4.2).sum().round()/((df3.loc[df3['direction']=='P']['wheel_mass']*2/8200)**4.2).sum().round())*100).round(2)},    -- ole80_percent_positive_direction
        {((((df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])&(df3['direction']=='N')]['wheel_mass']*2/8200)**4.2).sum().round()/((df3.loc[df3['direction']=='N']['wheel_mass']*2/8200)**4.2).sum().round())*100).round(2)},    -- ole80_percent_negative_direction
        ,    -- xe80
        ,    -- xe80_positive_direction
        ,    -- xe80_negative_direction
        ,    -- xe80_percent
        ,    -- xe80_percent_positive_direction
        ,    -- xe80_percent_negative_direction
        {((((df3['wheel_mass']*2/8200)**4.2).sum().round()/df3.groupby(pd.Grouper(key='start_datetime',freq='D')).count().count()[0])*100).round(2)},    -- e80_per_day
        {((((df3.loc[df3['direction']=='P']['wheel_mass']*2/8200)**4.2).sum().round()/df3.loc[df3['direction']=='P'].groupby(pd.Grouper(key='start_datetime',freq='D')).count().count()[0])*100).round(2)},    -- e80_per_day_positive_direction
        {((((df3.loc[df3['direction']=='N']['wheel_mass']*2/8200)**4.2).sum().round()/df3.loc[df3['direction']=='N'].groupby(pd.Grouper(key='start_datetime',freq='D')).count().count()[0])*100).round(2)},    -- e80_per_day_negative_direction
        {((((df3.loc[df3['vehicle_class_code_primary_scheme']>3]['wheel_mass']*2/8200)**4.2).sum().round()/df3.loc[df3['vehicle_class_code_primary_scheme']>3].count()[0])*100).round(2)},    -- e80_per_heavy_vehicle
        {((((df3.loc[(df3['vehicle_class_code_primary_scheme']>3)&(df3['direction']=='P')]['wheel_mass']*2/8200)**4.2).sum().round()/df3.loc[(df3['vehicle_class_code_primary_scheme']>3)&(df3['direction']=='P')].count()[0])*100).round(2)},    -- e80_per_heavy_vehicle_positive_direction
        {((((df3.loc[(df3['vehicle_class_code_primary_scheme']>3)&(df3['direction']=='N')]['wheel_mass']*2/8200)**4.2).sum().round()/df3.loc[(df3['vehicle_class_code_primary_scheme']>3)&(df3['direction']=='N')].count()[0])*100).round(2)},    -- e80_per_heavy_vehicle_negative_direction
        ,    -- worst_steering_single_axle_cnt
        ,    -- worst_steering_single_axle_olhv_perc
        ,    -- worst_steering_single_axle_tonperhv
        ,    -- worst_steering_double_axle_cnt
        ,    -- worst_steering_double_axle_olhv_perc
        ,    -- worst_steering_double_axle_tonperhv
        ,    -- worst_non_steering_single_axle_cnt
        ,    -- worst_non_steering_single_axle_olhv_perc
        ,    -- worst_non_steering_single_axle_tonperhv
        ,    -- worst_non_steering_double_axle_cnt
        ,    -- worst_non_steering_double_axle_olhv_perc
        ,    -- worst_non_steering_double_axle_tonperhv
        ,    -- worst_triple_axle_cnt
        ,    -- worst_triple_axle_olhv_perc
        ,    -- worst_triple_axle_tonperhv
        {(18000 + 2.1 * (df.loc[df['axle_spacing_number']>1].groupby('id')['axle_spacing_cm'].sum().mean().round(2))).round(2)},    -- bridge_formula_cnt
        ,    -- bridge_formula_olhv_perc
        ,    -- bridge_formula_tonperhv
        ,    -- gross_formula_cnt
        ,    -- gross_formula_olhv_perc
        ,    -- gross_formula_tonperhv
        ,    -- total_avg_cnt
        ,    -- total_avg_olhv_perc
        ,    -- total_avg_tonperhv
        ,    -- worst_steering_single_axle_cnt_positive_direciton
        ,    -- worst_steering_single_axle_olhv_perc_positive_direciton
        ,    -- worst_steering_single_axle_tonperhv_positive_direciton
        ,    -- worst_steering_double_axle_cnt_positive_direciton
        ,    -- worst_steering_double_axle_olhv_perc_positive_direciton
        ,    -- worst_steering_double_axle_tonperhv_positive_direciton
        ,    -- worst_non_steering_single_axle_cnt_positive_direciton
        ,    -- worst_non_steering_single_axle_olhv_perc_positive_direciton
        ,    -- worst_non_steering_single_axle_tonperhv_positive_direciton
        ,    -- worst_non_steering_double_axle_cnt_positive_direciton
        ,    -- worst_non_steering_double_axle_olhv_perc_positive_direciton
        ,    -- worst_non_steering_double_axle_tonperhv_positive_direciton
        ,    -- worst_triple_axle_cnt_positive_direciton
        ,    -- worst_triple_axle_olhv_perc_positive_direciton
        ,    -- worst_triple_axle_tonperhv_positive_direciton
        ,    -- bridge_formula_cnt_positive_direciton
        ,    -- bridge_formula_olhv_perc_positive_direciton
        ,    -- bridge_formula_tonperhv_positive_direciton
        ,    -- gross_formula_cnt_positive_direciton
        ,    -- gross_formula_olhv_perc_positive_direciton
        ,    -- gross_formula_tonperhv_positive_direciton
        ,    -- total_avg_cnt_positive_direciton
        ,    -- total_avg_olhv_perc_positive_direciton
        ,    -- total_avg_tonperhv_positive_direciton
        ,    -- worst_steering_single_axle_cnt_negative_direciton
        ,    -- worst_steering_single_axle_olhv_perc_negative_direciton
        ,    -- worst_steering_single_axle_tonperhv_negative_direciton
        ,    -- worst_steering_double_axle_cnt_negative_direciton
        ,    -- worst_steering_double_axle_olhv_perc_negative_direciton
        ,    -- worst_steering_double_axle_tonperhv_negative_direciton
        ,    -- worst_non_steering_single_axle_cnt_negative_direciton
        ,    -- worst_non_steering_single_axle_olhv_perc_negative_direciton
        ,    -- worst_non_steering_single_axle_tonperhv_negative_direciton
        ,    -- worst_non_steering_double_axle_cnt_negative_direciton
        ,    -- worst_non_steering_double_axle_olhv_perc_negative_direciton
        ,    -- worst_non_steering_double_axle_tonperhv_negative_direciton
        ,    -- worst_triple_axle_cnt_negative_direciton
        ,    -- worst_triple_axle_olhv_perc_negative_direciton
        ,    -- worst_triple_axle_tonperhv_negative_direciton
        ,    -- bridge_formula_cnt_negative_direciton
        ,    -- bridge_formula_olhv_perc_negative_direciton
        ,    -- bridge_formula_tonperhv_negative_direciton
        ,    -- gross_formula_cnt_negative_direciton
        ,    -- gross_formula_olhv_perc_negative_direciton
        ,    -- gross_formula_tonperhv_negative_direciton
        ,    -- total_avg_cnt_negative_direciton
        ,    -- total_avg_olhv_perc_negative_direciton
            -- total_avg_tonperhv_negative_direciton
            ON CONFLICT ON CONSTRAINT electronic_count_header_hswim_pkey DO UPDATE SET
            egrl_percent = coalesce(excluded.egrl_percent,egrl_percent),
            egrl_percent_positive_direction = coalesce(excluded.egrl_percent_positive_direction,egrl_percent_positive_direction),
            egrl_percent_negative_direction = coalesce(excluded.egrl_percent_negative_direction,egrl_percent_negative_direction),
            egrw_percent = coalesce(excluded.egrw_percent,egrw_percent),
            egrw_percent_positive_direction = coalesce(excluded.egrw_percent_positive_direction,egrw_percent_positive_direction),
            egrw_percent_negative_direction = coalesce(excluded.egrw_percent_negative_direction,egrw_percent_negative_direction),
            num_weighed = coalesce(excluded.num_weighed,num_weighed),
            num_weighed_positive_direction = coalesce(excluded.num_weighed_positive_direction,num_weighed_positive_direction),
            num_weighed_negative_direction = coalesce(excluded.num_weighed_negative_direction,num_weighed_negative_direction),
            mean_equivalent_axle_mass = coalesce(excluded.mean_equivalent_axle_mass,mean_equivalent_axle_mass),
            mean_equivalent_axle_mass_positive_direction = coalesce(excluded.mean_equivalent_axle_mass_positive_direction,mean_equivalent_axle_mass_positive_direction),
            mean_equivalent_axle_mass_negative_direction = coalesce(excluded.mean_equivalent_axle_mass_negative_direction,mean_equivalent_axle_mass_negative_direction),
            mean_axle_spacing = coalesce(excluded.mean_axle_spacing,mean_axle_spacing),
            mean_axle_spacing_positive_direction = coalesce(excluded.mean_axle_spacing_positive_direction,mean_axle_spacing_positive_direction),
            mean_axle_spacing_negative_direction = coalesce(excluded.mean_axle_spacing_negative_direction,mean_axle_spacing_negative_direction),
            e80_per_axle = coalesce(excluded.e80_per_axle,e80_per_axle),
            e80_per_axle_positive_direction = coalesce(excluded.e80_per_axle_positive_direction,e80_per_axle_positive_direction),
            e80_per_axle_negative_direction = coalesce(excluded.e80_per_axle_negative_direction,e80_per_axle_negative_direction),
            olhv = coalesce(excluded.olhv,olhv),
            olhv_positive_direction = coalesce(excluded.olhv_positive_direction,olhv_positive_direction),
            olhv_negative_direction = coalesce(excluded.olhv_negative_direction,olhv_negative_direction),
            olhv_percent = coalesce(excluded.olhv_percent,olhv_percent),
            olhv_percent_positive_direction = coalesce(excluded.olhv_percent_positive_direction,olhv_percent_positive_direction),
            olhv_percent_negative_direction = coalesce(excluded.olhv_percent_negative_direction,olhv_percent_negative_direction),
            tonnage_generated = coalesce(excluded.tonnage_generated,tonnage_generated),
            tonnage_generated_positive_direction = coalesce(excluded.tonnage_generated_positive_direction,tonnage_generated_positive_direction),
            tonnage_generated_negative_direction = coalesce(excluded.tonnage_generated_negative_direction,tonnage_generated_negative_direction),
            olton = coalesce(excluded.olton,olton),
            olton_positive_direction = coalesce(excluded.olton_positive_direction,olton_positive_direction),
            olton_negative_direction = coalesce(excluded.olton_negative_direction,olton_negative_direction),
            olton_percent = coalesce(excluded.olton_percent,olton_percent),
            olton_percent_positive_direction = coalesce(excluded.olton_percent_positive_direction,olton_percent_positive_direction),
            olton_percent_negative_direction = coalesce(excluded.olton_percent_negative_direction,olton_percent_negative_direction),
            ole80 = coalesce(excluded.ole80,ole80),
            ole80_positive_direction = coalesce(excluded.ole80_positive_direction,ole80_positive_direction),
            ole80_negative_direction = coalesce(excluded.ole80_negative_direction,ole80_negative_direction),
            ole80_percent = coalesce(excluded.ole80_percent,ole80_percent),
            ole80_percent_positive_direction = coalesce(excluded.ole80_percent_positive_direction,ole80_percent_positive_direction),
            ole80_percent_negative_direction = coalesce(excluded.ole80_percent_negative_direction,ole80_percent_negative_direction),
            xe80 = coalesce(excluded.xe80,xe80),
            xe80_positive_direction = coalesce(excluded.xe80_positive_direction,xe80_positive_direction),
            xe80_negative_direction = coalesce(excluded.xe80_negative_direction,xe80_negative_direction),
            xe80_percent = coalesce(excluded.xe80_percent,xe80_percent),
            xe80_percent_positive_direction = coalesce(excluded.xe80_percent_positive_direction,xe80_percent_positive_direction),
            xe80_percent_negative_direction = coalesce(excluded.xe80_percent_negative_direction,xe80_percent_negative_direction),
            e80_per_day = coalesce(excluded.e80_per_day,e80_per_day),
            e80_per_day_positive_direction = coalesce(excluded.e80_per_day_positive_direction,e80_per_day_positive_direction),
            e80_per_day_negative_direction = coalesce(excluded.e80_per_day_negative_direction,e80_per_day_negative_direction),
            e80_per_heavy_vehicle = coalesce(excluded.e80_per_heavy_vehicle,e80_per_heavy_vehicle),
            e80_per_heavy_vehicle_positive_direction = coalesce(excluded.e80_per_heavy_vehicle_positive_direction,e80_per_heavy_vehicle_positive_direction),
            e80_per_heavy_vehicle_negative_direction = coalesce(excluded.e80_per_heavy_vehicle_negative_direction,e80_per_heavy_vehicle_negative_direction),
            worst_steering_single_axle_cnt = coalesce(excluded.worst_steering_single_axle_cnt,worst_steering_single_axle_cnt),
            worst_steering_single_axle_olhv_perc = coalesce(excluded.worst_steering_single_axle_olhv_perc,worst_steering_single_axle_olhv_perc),
            worst_steering_single_axle_tonperhv = coalesce(excluded.worst_steering_single_axle_tonperhv,worst_steering_single_axle_tonperhv),
            worst_steering_double_axle_cnt = coalesce(excluded.worst_steering_double_axle_cnt,worst_steering_double_axle_cnt),
            worst_steering_double_axle_olhv_perc = coalesce(excluded.worst_steering_double_axle_olhv_perc,worst_steering_double_axle_olhv_perc),
            worst_steering_double_axle_tonperhv = coalesce(excluded.worst_steering_double_axle_tonperhv,worst_steering_double_axle_tonperhv),
            worst_non_steering_single_axle_cnt = coalesce(excluded.worst_non_steering_single_axle_cnt,worst_non_steering_single_axle_cnt),
            worst_non_steering_single_axle_olhv_perc = coalesce(excluded.worst_non_steering_single_axle_olhv_perc,worst_non_steering_single_axle_olhv_perc),
            worst_non_steering_single_axle_tonperhv = coalesce(excluded.worst_non_steering_single_axle_tonperhv,worst_non_steering_single_axle_tonperhv),
            worst_non_steering_double_axle_cnt = coalesce(excluded.worst_non_steering_double_axle_cnt,worst_non_steering_double_axle_cnt),
            worst_non_steering_double_axle_olhv_perc = coalesce(excluded.worst_non_steering_double_axle_olhv_perc,worst_non_steering_double_axle_olhv_perc),
            worst_non_steering_double_axle_tonperhv = coalesce(excluded.worst_non_steering_double_axle_tonperhv,worst_non_steering_double_axle_tonperhv),
            worst_triple_axle_cnt = coalesce(excluded.worst_triple_axle_cnt,worst_triple_axle_cnt),
            worst_triple_axle_olhv_perc = coalesce(excluded.worst_triple_axle_olhv_perc,worst_triple_axle_olhv_perc),
            worst_triple_axle_tonperhv = coalesce(excluded.worst_triple_axle_tonperhv,worst_triple_axle_tonperhv),
            bridge_formula_cnt = coalesce(excluded.bridge_formula_cnt,bridge_formula_cnt),
            bridge_formula_olhv_perc = coalesce(excluded.bridge_formula_olhv_perc,bridge_formula_olhv_perc),
            bridge_formula_tonperhv = coalesce(excluded.bridge_formula_tonperhv,bridge_formula_tonperhv),
            gross_formula_cnt = coalesce(excluded.gross_formula_cnt,gross_formula_cnt),
            gross_formula_olhv_perc = coalesce(excluded.gross_formula_olhv_perc,gross_formula_olhv_perc),
            gross_formula_tonperhv = coalesce(excluded.gross_formula_tonperhv,gross_formula_tonperhv),
            total_avg_cnt = coalesce(excluded.total_avg_cnt,total_avg_cnt),
            total_avg_olhv_perc = coalesce(excluded.total_avg_olhv_perc,total_avg_olhv_perc),
            total_avg_tonperhv = coalesce(excluded.total_avg_tonperhv,total_avg_tonperhv),
            worst_steering_single_axle_cnt_positive_direciton = coalesce(excluded.worst_steering_single_axle_cnt_positive_direciton,worst_steering_single_axle_cnt_positive_direciton),
            worst_steering_single_axle_olhv_perc_positive_direciton = coalesce(excluded.worst_steering_single_axle_olhv_perc_positive_direciton,worst_steering_single_axle_olhv_perc_positive_direciton),
            worst_steering_single_axle_tonperhv_positive_direciton = coalesce(excluded.worst_steering_single_axle_tonperhv_positive_direciton,worst_steering_single_axle_tonperhv_positive_direciton),
            worst_steering_double_axle_cnt_positive_direciton = coalesce(excluded.worst_steering_double_axle_cnt_positive_direciton,worst_steering_double_axle_cnt_positive_direciton),
            worst_steering_double_axle_olhv_perc_positive_direciton = coalesce(excluded.worst_steering_double_axle_olhv_perc_positive_direciton,worst_steering_double_axle_olhv_perc_positive_direciton),
            worst_steering_double_axle_tonperhv_positive_direciton = coalesce(excluded.worst_steering_double_axle_tonperhv_positive_direciton,worst_steering_double_axle_tonperhv_positive_direciton),
            worst_non_steering_single_axle_cnt_positive_direciton = coalesce(excluded.worst_non_steering_single_axle_cnt_positive_direciton,worst_non_steering_single_axle_cnt_positive_direciton),
            worst_non_steering_single_axle_olhv_perc_positive_direciton = coalesce(excluded.worst_non_steering_single_axle_olhv_perc_positive_direciton,worst_non_steering_single_axle_olhv_perc_positive_direciton),
            worst_non_steering_single_axle_tonperhv_positive_direciton = coalesce(excluded.worst_non_steering_single_axle_tonperhv_positive_direciton,worst_non_steering_single_axle_tonperhv_positive_direciton),
            worst_non_steering_double_axle_cnt_positive_direciton = coalesce(excluded.worst_non_steering_double_axle_cnt_positive_direciton,worst_non_steering_double_axle_cnt_positive_direciton),
            worst_non_steering_double_axle_olhv_perc_positive_direciton = coalesce(excluded.worst_non_steering_double_axle_olhv_perc_positive_direciton,worst_non_steering_double_axle_olhv_perc_positive_direciton),
            worst_non_steering_double_axle_tonperhv_positive_direciton = coalesce(excluded.worst_non_steering_double_axle_tonperhv_positive_direciton,worst_non_steering_double_axle_tonperhv_positive_direciton),
            worst_triple_axle_cnt_positive_direciton = coalesce(excluded.worst_triple_axle_cnt_positive_direciton,worst_triple_axle_cnt_positive_direciton),
            worst_triple_axle_olhv_perc_positive_direciton = coalesce(excluded.worst_triple_axle_olhv_perc_positive_direciton,worst_triple_axle_olhv_perc_positive_direciton),
            worst_triple_axle_tonperhv_positive_direciton = coalesce(excluded.worst_triple_axle_tonperhv_positive_direciton,worst_triple_axle_tonperhv_positive_direciton),
            bridge_formula_cnt_positive_direciton = coalesce(excluded.bridge_formula_cnt_positive_direciton,bridge_formula_cnt_positive_direciton),
            bridge_formula_olhv_perc_positive_direciton = coalesce(excluded.bridge_formula_olhv_perc_positive_direciton,bridge_formula_olhv_perc_positive_direciton),
            bridge_formula_tonperhv_positive_direciton = coalesce(excluded.bridge_formula_tonperhv_positive_direciton,bridge_formula_tonperhv_positive_direciton),
            gross_formula_cnt_positive_direciton = coalesce(excluded.gross_formula_cnt_positive_direciton,gross_formula_cnt_positive_direciton),
            gross_formula_olhv_perc_positive_direciton = coalesce(excluded.gross_formula_olhv_perc_positive_direciton,gross_formula_olhv_perc_positive_direciton),
            gross_formula_tonperhv_positive_direciton = coalesce(excluded.gross_formula_tonperhv_positive_direciton,gross_formula_tonperhv_positive_direciton),
            total_avg_cnt_positive_direciton = coalesce(excluded.total_avg_cnt_positive_direciton,total_avg_cnt_positive_direciton),
            total_avg_olhv_perc_positive_direciton = coalesce(excluded.total_avg_olhv_perc_positive_direciton,total_avg_olhv_perc_positive_direciton),
            total_avg_tonperhv_positive_direciton = coalesce(excluded.total_avg_tonperhv_positive_direciton,total_avg_tonperhv_positive_direciton),
            worst_steering_single_axle_cnt_negative_direciton = coalesce(excluded.worst_steering_single_axle_cnt_negative_direciton,worst_steering_single_axle_cnt_negative_direciton),
            worst_steering_single_axle_olhv_perc_negative_direciton = coalesce(excluded.worst_steering_single_axle_olhv_perc_negative_direciton,worst_steering_single_axle_olhv_perc_negative_direciton),
            worst_steering_single_axle_tonperhv_negative_direciton = coalesce(excluded.worst_steering_single_axle_tonperhv_negative_direciton,worst_steering_single_axle_tonperhv_negative_direciton),
            worst_steering_double_axle_cnt_negative_direciton = coalesce(excluded.worst_steering_double_axle_cnt_negative_direciton,worst_steering_double_axle_cnt_negative_direciton),
            worst_steering_double_axle_olhv_perc_negative_direciton = coalesce(excluded.worst_steering_double_axle_olhv_perc_negative_direciton,worst_steering_double_axle_olhv_perc_negative_direciton),
            worst_steering_double_axle_tonperhv_negative_direciton = coalesce(excluded.worst_steering_double_axle_tonperhv_negative_direciton,worst_steering_double_axle_tonperhv_negative_direciton),
            worst_non_steering_single_axle_cnt_negative_direciton = coalesce(excluded.worst_non_steering_single_axle_cnt_negative_direciton,worst_non_steering_single_axle_cnt_negative_direciton),
            worst_non_steering_single_axle_olhv_perc_negative_direciton = coalesce(excluded.worst_non_steering_single_axle_olhv_perc_negative_direciton,worst_non_steering_single_axle_olhv_perc_negative_direciton),
            worst_non_steering_single_axle_tonperhv_negative_direciton = coalesce(excluded.worst_non_steering_single_axle_tonperhv_negative_direciton,worst_non_steering_single_axle_tonperhv_negative_direciton),
            worst_non_steering_double_axle_cnt_negative_direciton = coalesce(excluded.worst_non_steering_double_axle_cnt_negative_direciton,worst_non_steering_double_axle_cnt_negative_direciton),
            worst_non_steering_double_axle_olhv_perc_negative_direciton = coalesce(excluded.worst_non_steering_double_axle_olhv_perc_negative_direciton,worst_non_steering_double_axle_olhv_perc_negative_direciton),
            worst_non_steering_double_axle_tonperhv_negative_direciton = coalesce(excluded.worst_non_steering_double_axle_tonperhv_negative_direciton,worst_non_steering_double_axle_tonperhv_negative_direciton),
            worst_triple_axle_cnt_negative_direciton = coalesce(excluded.worst_triple_axle_cnt_negative_direciton,worst_triple_axle_cnt_negative_direciton),
            worst_triple_axle_olhv_perc_negative_direciton = coalesce(excluded.worst_triple_axle_olhv_perc_negative_direciton,worst_triple_axle_olhv_perc_negative_direciton),
            worst_triple_axle_tonperhv_negative_direciton = coalesce(excluded.worst_triple_axle_tonperhv_negative_direciton,worst_triple_axle_tonperhv_negative_direciton),
            bridge_formula_cnt_negative_direciton = coalesce(excluded.bridge_formula_cnt_negative_direciton,bridge_formula_cnt_negative_direciton),
            bridge_formula_olhv_perc_negative_direciton = coalesce(excluded.bridge_formula_olhv_perc_negative_direciton,bridge_formula_olhv_perc_negative_direciton),
            bridge_formula_tonperhv_negative_direciton = coalesce(excluded.bridge_formula_tonperhv_negative_direciton,bridge_formula_tonperhv_negative_direciton),
            gross_formula_cnt_negative_direciton = coalesce(excluded.gross_formula_cnt_negative_direciton,gross_formula_cnt_negative_direciton),
            gross_formula_olhv_perc_negative_direciton = coalesce(excluded.gross_formula_olhv_perc_negative_direciton,gross_formula_olhv_perc_negative_direciton),
            gross_formula_tonperhv_negative_direciton = coalesce(excluded.gross_formula_tonperhv_negative_direciton,gross_formula_tonperhv_negative_direciton),
            total_avg_cnt_negative_direciton = coalesce(excluded.total_avg_cnt_negative_direciton,total_avg_cnt_negative_direciton),
            total_avg_olhv_perc_negative_direciton = coalesce(excluded.total_avg_olhv_perc_negative_direciton,total_avg_olhv_perc_negative_direciton),
            total_avg_tonperhv_negative_direciton = coalesce(excluded.total_avg_tonperhv_negative_direciton,total_avg_tonperhv_negative_direciton)
        )
    """

    return INSERT_STRING
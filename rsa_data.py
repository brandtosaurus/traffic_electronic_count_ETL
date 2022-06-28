import pandas as pd
from pandasql import sqldf
import numpy as np
import config
import rsa_headers as rh
from sqlalchemy.dialects.postgresql import insert


from datetime import timedelta
import uuid

#################################################################################################################################################################################################################################
##########################################################################				  DATA RECORDS BELOW									 #############################################################################
#################################################################################################################################################################################################################################
class Data(object):
    def __init__(self, df):
        self.get_head = Data.get_head(df)
        self.header = Data.headers(self.get_head)
        self.lanes = Data.lanes(self.get_head)
        self.dtype21 = Data.join_header_id(Data.dtype21(df))
        self.dtype30 = Data.join_header_id(Data.dtype30(df))
        self.dtype70 = Data.join_header_id(Data.dtype70(df))
        self.dtype10, self.dtype10_sub_data = Data.dtype10(df)
        self.dtype10 = Data.join_header_id(self.dtype10)
        self.dtype60 = Data.join_header_id(Data.dtype60(df))
        self.type10_separate_table = Data.type10_separate_table(df)
        pass

#### DATA TOOLS ####

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

            headers["primary_direction"] = dfh.loc[dfh[0] == 'L1', 2].astype(int)
            headers["secondary_direction"] = dfh.loc[dfh[0] == 'L'+ headers["number_of_lanes"].iloc[0].astype(str), 2].astype(int)            
                

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
                                                else pd.to_datetime(
                                                    x["end_datetime"] + x["end_time"]
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
                                            else pd.to_datetime(
                                                x["start_datetime"] + x["start_time"]
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
        data = df.loc[(df[0] == "10") & (df[3].isin(["1", "0"]))].dropna(
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

    def dtype10_separate_table(df: pd.DataFrame) -> pd.DataFrame:
        data = df.loc[(df[0] == "10") & (df[1].isin(["15", "17", "19"]))].dropna(
            axis=1, how="all"
        )
        dfh2 = pd.DataFrame(df.loc[(df[0].isin(["S0", "L1"]))]).dropna(
            axis=1, how="all"
        )
        if data.empty:
            print("data empty")
            print(data)
        else:
            ddf = data.iloc[:, 4:]
            ddf = pd.DataFrame(ddf).dropna(axis=1, how="all")
            if (data[1].isin(['15','17']).all()
                and len(ddf.columns) == 11
            ):
                ddf.columns = [
                    "departure_date",
                    "departure_time",
                    "assigned_lane_number",
                    "physical_lane_number",
                    "forward_reverse_code",
                    "vehicle_category",
                    "vehicle_class_code_primary_scheme",
                    "vehicle_class_code_secondary_scheme",
                    "vehicle_speed",
                    "vehicle_length",
                    "site_occupancy_time_in_milliseconds",
                    "chassis_height_code",
                    "vehicle_following_code",
                ]
                ddf = pd.concat(
                    [
                        ddf,
                        pd.DataFrame(
                            columns=[
                                "vehicle_tag_code",
                                "trailer_count",
                                "axle_count",
                                "bumper_to_1st_axle_spacing",
                                "sub_data_type_code_sx",
                                "number_of_axles_spacings_counted",
                            ]
                        ),
                    ]
                )
            elif (data[1].isin(['15','17']).all()
                and len(ddf.columns) == 13
            ):
                ddf.columns = [
                    "departure_date",
                    "departure_time",
                    "assigned_lane_number",
                    "physical_lane_number",
                    "forward_reverse_code",
                    "vehicle_category",
                    "vehicle_class_code_primary_scheme",
                    "vehicle_class_code_secondary_scheme",
                    "vehicle_speed",
                    "vehicle_length",
                    "site_occupancy_time_in_milliseconds",
                    "chassis_height_code",
                    "vehicle_following_code",
                ]
                ddf = pd.concat(
                    [
                        ddf,
                        pd.DataFrame(
                            columns=[
                                "vehicle_tag_code",
                                "trailer_count",
                                "axle_count",
                                "bumper_to_1st_axle_spacing",
                                "sub_data_type_code_sx",
                                "number_of_axles_spacings_counted",
                            ]
                        ),
                    ]
                )
            elif (data[1].isin(['15','17']).all()
                and len(ddf.columns) == 15
            ):
                ddf.columns = [
                    "departure_date",
                    "departure_time",
                    "assigned_lane_number",
                    "physical_lane_number",
                    "forward_reverse_code",
                    "vehicle_category",
                    "vehicle_class_code_primary_scheme",
                    "vehicle_class_code_secondary_scheme",
                    "vehicle_speed",
                    "vehicle_length",
                    "site_occupancy_time_in_milliseconds",
                    "chassis_height_code",
                    "vehicle_following_code",
                    "vehicle_tag_code",
                    "trailer_count",
                ]
                ddf = pd.concat(
                    [
                        ddf,
                        pd.DataFrame(
                            columns=[
                                "axle_count",
                                "bumper_to_1st_axle_spacing",
                                "sub_data_type_code_sx",
                                "number_of_axles_spacings_counted",
                            ]
                        ),
                    ]
                )
            elif data[1].isin(['19']).all():
                ddf = data.iloc[:, 4:22]
                ddf = pd.DataFrame(ddf).dropna(axis=1, how="all")
                ddf.columns = [
                    "departure_date",
                    "departure_time",
                    "assigned_lane_number",
                    "physical_lane_number",
                    "forward_reverse_code",
                    "vehicle_category",
                    "vehicle_class_code_primary_scheme",
                    "vehicle_class_code_secondary_scheme",
                    "vehicle_speed",
                    "vehicle_length",
                    "site_occupancy_time_in_milliseconds",
                    "chassis_height_code",
                    "vehicle_following_code",
                    "vehicle_tag_code",
                    "trailer_count", 
                    "axle_count",
                    "bumper_to_1st_axle_spacing",
                    "sub_data_type_code_sx",
                    "number_of_axles_spacings_counted",
                ]
                ddf["number_of_axles_spacings_counted"] = ddf[
                    "number_of_axles_spacings_counted"
                ].astype(int)
                for i in range(ddf["number_of_axles_spacings_counted"].max()()):
                    i = i + 1
                    newcolumn = (
                        "axle_spacing_" + str(i) + "_between_individual_axles_cm"
                    )
                    ddf[newcolumn] = data[22 + i]

            ddf = ddf.fillna(0)
            ddf["assigned_lane_number"] = ddf["assigned_lane_number"].astype(int)
            max_lanes = ddf["assigned_lane_number"].max()
            try:
                ddf["direction"] = ddf.apply(
                lambda x: "P" if x["assigned_lane_number"] <= (int(max_lanes) / 2) else "N",
                axis=1,
            )
                direction = dfh2.loc[dfh2[0] == "L1", 1:3]
                direction = direction.drop_duplicates()
            except:
                pass
            try:
                ddf["forward_direction_code"] = ddf.apply(
                    lambda x: get_direction(x["assigned_lane_number"], direction), axis=1
                )
                # FIXME: ddf['lane_position_code']=ddf.apply(lambda x: Data.get_lane_position(x['lane_number'],direction),axis=1)
            except Exception:
                ddf["forward_direction_code"] = None
                # ddf['lane_position_code']=None

            if ddf["departure_date"].map(len).isin([8]).all():
                ddf["start_datetime"] = pd.to_datetime(
                    ddf["departure_date"] + ddf["departure_time"],
                    format="%Y%m%d%H%M%S%f",
                )
            elif ddf["departure_date"].map(len).isin([6]).all():
                ddf["start_datetime"] = pd.to_datetime(
                    ddf["departure_date"] + ddf["departure_time"],
                    format="%y%m%d%H%M%S%f",
                )
            ddf['year'] = ddf['start_datetime'].dt.year
            t1 = dfh2.loc[dfh2[0] == "S0", 1].unique()
            ddf["site_id"] = str(t1[0])
            ddf["site_id"] = ddf["site_id"].astype(str)
            ddf['departure_time'] = pd.to_datetime(ddf['departure_time'], format='%H%M%S%f')

            ddf = ddf.drop_duplicates()
            ddf["start_datetime"] = ddf["start_datetime"].astype("datetime64[ns]")

            ddf = ddf.replace(r'^\s*$', np.NaN, regex=True)

            scols = ddf.select_dtypes('object').columns
        
            ddf[scols] = ddf[scols].apply(pd.to_numeric, axis=1, errors='ignore')
        
            return ddf

    def header_calcs(header, data, dtype):
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
        
            return header

        elif dtype == 60:
            
            return header

        else:
            return header

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

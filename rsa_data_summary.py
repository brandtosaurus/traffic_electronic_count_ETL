import pandas as pd
from pandasql import sqldf
import config
from sqlalchemy.dialects.postgresql import insert
from datetime import timedelta, date
import queries as q

#################################################################################################################################################################################################################################
##########################################################################				  DATA RECORDS BELOW									 #############################################################################
#################################################################################################################################################################################################################################
class Data(object):
    def __init__(self, df: pd.DataFrame, header: pd.DataFrame):
        self.dtype21 = Data.join(header, Data.dtype21(df))
        self.dtype30 = Data.join(header, Data.dtype30(df))
        self.dtype70 = Data.join(header, Data.dtype70(df))
        # self.dtype60 = Data.join(header, Data.dtype60(df))
        self.electronic_count_data_type_30 = Data.electronic_count_data_type_30(df)
        

#### DATA TOOLS ####

    def get_direction(lane_number: int, df: pd.DataFrame) -> pd.DataFrame:
        filt = df[1] == lane_number
        df = df.where(filt)
        df = df[2].dropna()
        df = int(df)
        return df

    def get_lane_position(lane_number: int, df: pd.DataFrame) -> pd.DataFrame:
        filt = df[1] == lane_number
        df = df.where(filt)
        df = df[3].dropna()
        df = str(df)
        return df

    def join_header_id(header: pd.DataFrame, df: pd.DataFrame) -> pd.DataFrame:
        if df is None:
            pass
        elif df.empty:
            pass
        else:
            data = Data.join(df, header)
            data.drop("station_name", axis=1, inplace=True)
            data["start_datetime"] = data["start_datetime"].astype("datetime64[ns]")
            df["start_datetime"] = df["start_datetime"].astype("datetime64[ns]")
            data = data.merge(
                df, how="outer", on=["site_id", "start_datetime", "lane_number"]
            )
            return data

    def join(header: pd.DataFrame, data: pd.DataFrame) -> pd.DataFrame:
        if data is None:
            pass
        elif data.empty:
            pass
        else:
            q = """
            SELECT header.header_id, data.*
            FROM header
            LEFT JOIN data ON data.start_datetime WHERE 
            data.start_datetime >= header.start_datetime AND data.end_datetime <= header.end_datetime
            OR
            date(data.start_datetime) >= date(header.start_datetime) AND date(data.end_datetime) <= date(header.end_datetime);
            """
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

    def dtype21(df: pd.DataFrame) -> pd.DataFrame:
        data = df.loc[(df[0] == "21") & (df[1].isin(["0", "1", "2", "3", "4"]))].dropna(
            axis=1, how="all"
        ).reset_index(drop=True)
        dfh = pd.DataFrame(df.loc[(df[0].isin(["S0", "L1", "L2", "L3", "L4", "L5", "L6", "L7", "L8"]))]).dropna(
            axis=1, how="all"
        ).reset_index(drop=True)

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
            
            ddf.reset_index(inplace=True)
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

            direction = dfh.loc[dfh[0] == "L1", 1:3].astype(int)
            direction = direction.drop_duplicates()

            ddf['end_date'] = ddf['end_datetime'].apply(lambda x: pd.to_datetime(x, format="%y%m%d").date() if len(x)==6 else pd.to_datetime(x, format="%Y%m%d").date())

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
            
            ddf["start_datetime"] = pd.to_datetime(
                ddf["end_datetime"]
            ) - pd.to_timedelta(ddf["duration_min"].astype(int), unit="m")
            ddf['year'] = ddf['start_datetime'].dt.year
            ddf["site_id"] = str(dfh[dfh[0]=="S0"].reset_index(drop=True).iloc[0,1]).zfill(4)

            ddf = ddf.drop_duplicates()
            ddf["start_datetime"] = ddf["start_datetime"].astype("datetime64[ns]")

            ddf['end_time'] = ddf['end_time'].apply(lambda x: '0000' if (x=='2400' and len(x)==4) 
                else '000000' if ((x=='240000' and len(x)==6))
                else x)
            ddf['end_time'] = ddf['end_time'].apply(lambda x: pd.to_datetime(x, format="%H%M").time()
                if len(x)==4 
                else pd.to_datetime(x, format="%H%M%S").time())

            return ddf

    def dtype30(df: pd.DataFrame) -> pd.DataFrame:
        data = df.loc[(df[0] == "30") & (df[1].isin(["0", "1", "2", "3", "4"]))].dropna(
            axis=1, how="all"
        ).reset_index(drop=True)
        dfh = pd.DataFrame(df.loc[(df[0].isin(["S0", "L1", "L2", "L3", "L4", "L5", "L6", "L7", "L8"]))]).dropna(
            axis=1, how="all"
        ).reset_index(drop=True)
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
                    ddf["heavy_vehicle"] = 0

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
            ddf.reset_index(inplace=True)
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
            direction = dfh.loc[dfh[0] == "L1", 1:3].astype(int)
            direction = direction.drop_duplicates()

            ddf['end_date'] = ddf['end_datetime'].apply(lambda x: pd.to_datetime(x, format="%y%m%d").date() if len(x)==6 else pd.to_datetime(x, format="%Y%m%d").date())

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

            

            ddf["start_datetime"] = pd.to_datetime(
                ddf["end_datetime"]
            ) - pd.to_timedelta(ddf["duration_min"].astype(int), unit="m")
            ddf['year'] = ddf['start_datetime'].dt.year
            # t1 = dfh.loc[dfh[0] == "S0", 1].unique()
            ddf["site_id"] = str(dfh[dfh[0]=="S0"].reset_index(drop=True).iloc[0,1]).zfill(4)

            ddf = ddf.drop_duplicates()
            ddf["start_datetime"] = ddf["start_datetime"].astype("datetime64[ns]")

            ddf['end_time'] = ddf['end_time'].apply(lambda x: '0000' if (x=='2400' and len(x)==4) 
            else '000000' if ((x=='240000' and len(x)==6))
            else x)
            ddf['end_time'] = ddf['end_time'].apply(lambda x: pd.to_datetime(x, format="%H%M").time()
            if len(x)==4 
            else pd.to_datetime(x, format="%H%M%S").time())

            return ddf

    def electronic_count_data_type_30(df: pd.DataFrame) -> pd.DataFrame:
        data = df.loc[(df[0] == "30") & (df[1].isin(["0", "1", "2", "3", "4"]))].dropna(
                    axis=1, how="all"
                ).reset_index(drop=True)
        dfh = pd.DataFrame(df.loc[(df[0].isin(["S0", "L1", "L2", "L3", "L4", "L5", "L6", "L7", "L8"]))]).dropna(
            axis=1, how="all"
        ).reset_index(drop=True)
        header = df.loc[(df[0] == "30") & (~df[1].isin(["0", "1", "2", "3", "4"]))].dropna(
                    axis=1, how="all"
                ).reset_index(drop=True)

        if data.empty:
            pass
        else:
            try:
                dir_1 = dfh.iloc[1:,2].astype(int).min()
                dir_2 = dfh.iloc[1:,2].astype(int).max()
            except (TypeError,ValueError):
                dir_1 = 0
                dir_2 = 4

            if header.shape[1] > 3:
                summary_interval = header.iloc[0,2]
                classification_scheme = header.iloc[0,3]
                number_of_data_records = header.iloc[0,4]
            else:
                summary_interval = header.iloc[0,1]
                classification_scheme = header.iloc[0,2]
                number_of_data_records = header.iloc[0,3]

            vc_df = select_classification_scheme(classification_scheme)

            if data[1].isin(["0", "2"]).any():
                ddf = data.iloc[:, 1:].reset_index(drop=True)
                ddf = pd.DataFrame(ddf).dropna(axis=1, how="all").reset_index(drop=True)

                try:
                    site_id = str(dfh[dfh[0]=="S0"].reset_index(drop=True).iloc[0,1]).zfill(4)
                except IndexError:
                    site_id = dfh.loc[df[0]=="S0",1]
   
                time_length = len(ddf[2][0])
                date_length = len(ddf[2][0])
                duration_min = int(ddf[4][0])
                ddf[4] = ddf[4].astype(int)
                ddf[5] = ddf[5].astype(int)
                max_lanes = ddf[5].max()

                ddf = process_dates_and_times(ddf, date_length, time_length, duration_min)

                ddf = ddf.apply(pd.to_numeric, errors='ignore')
                    
                ddf["direction"] = ddf.apply(
                    lambda x: "P" if int(x[5]) <= (int(max_lanes) / 2) else "N",
                    axis=1,
                )
                ddf["compas_heading"] = ddf.apply(
                    lambda x: dir_1 if int(x[5]) <= (int(max_lanes) / 2) else dir_2,
                    axis=1,
                )

                ddf['vehicle_classification_scheme'] = int(classification_scheme)

                ddf['start_datetime'] = pd.to_datetime(ddf[2].astype(str)+ddf[3].astype(str), 
                    format='%Y-%m-%d%H:%M:%S') - timedelta(minutes=duration_min)
                
                ddf.columns = ddf.columns.astype(str)

                df3 = pd.DataFrame(columns=['edit_code', 'start_datetime', 'end_date', 'end_time', 
                    'duration_of_summary', 'lane_number', 'number_of_vehicles', 'class_number', 'direction', 'compas_heading'])
                for lane_no in range(max_lanes):
                    for i in range(1,int(number_of_data_records)):
                        i=i+5
                        join_to_df3 = ddf.loc[ddf['5'] == lane_no, ['1', 'start_datetime','2', '3', '4', '5',str(i), 'direction', 'compas_heading']]
                        join_to_df3['class_number'] = i-5
                        join_to_df3.rename(columns={'1':"edit_code",'2':"end_date",'3':"end_time",'4':"duration_of_summary",'5':'lane_number', str(i): 'number_of_vehicles'}, inplace=True)
                        df3 = pd.concat([df3,join_to_df3],keys=['start_datetime','lane_number','number_of_vehicles','class_number'],ignore_index=True, axis=0)
                df3['classification_scheme'] = int(classification_scheme)
                df3['site_id'] = site_id
                df3['year'] = int(df3['start_datetime'][0].year)
            else:
                pass
            return df3

    def dtype70(df: pd.DataFrame) -> pd.DataFrame:
        data = df.loc[(df[0] == "70") & (df[1].isin(["0", "1", "2", "3", "4"]))].dropna(
            axis=1, how="all"
        ).reset_index(drop=True)
        dfh = pd.DataFrame(df.loc[(df[0].isin(["S0", "L1", "L2", "L3", "L4", "L5", "L6", "L7", "L8"]))]).dropna(
            axis=1, how="all"
        ).reset_index(drop=True)
        if data.empty:
            pass
        else:
            if data[1].all() == "1":
                ddf = data.iloc[:, 3:]
                ddf = pd.DataFrame(ddf).dropna(axis=1, how="all").reset_index(drop=True)

                # time_length = len(ddf[2][0])
                # date_length = len(ddf[2][0])
                # duration_min = int(ddf[4][0])
                # ddf[4] = ddf[4].astype(int)
                # ddf[5] = ddf[5].astype(int)
                # max_lanes = ddf[5].max()

                # ddf = process_dates_and_times(ddf, date_length, time_length, duration_min)

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
                ddf = pd.DataFrame(ddf).dropna(axis=1, how="all").reset_index(drop=True)
                ddf.columns = [
                    "end_datetime",
                    "end_time",
                    "duration_min",
                    "lane_number",
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
            
            ddf.reset_index(drop=True,inplace=True)
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
            direction = dfh.loc[dfh[0] == "L1", 1:3].astype(int)
            direction = direction.drop_duplicates()

            ddf['end_date'] = ddf['end_datetime'].apply(lambda x: pd.to_datetime(x, format="%y%m%d").date() if len(x)==6 else pd.to_datetime(x, format="%Y%m%d").date())
            
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


            ddf["start_datetime"] = pd.to_datetime(
                ddf["end_datetime"]
            ) - pd.to_timedelta(ddf["duration_min"].astype(int), unit="m")
            ddf['year'] = ddf['start_datetime'].dt.year
            # t1 = dfh.loc[dfh[0] == "S0", 1].unique()
            ddf["site_id"] = str(dfh[dfh[0]=="S0"].reset_index(drop=True).iloc[0,1]).zfill(4)
            ddf["site_id"] = ddf["site_id"].astype(str)

            ddf = ddf.drop_duplicates()
            ddf["start_datetime"] = ddf["start_datetime"].astype("datetime64[ns]")

            ddf['end_time'] = ddf['end_time'].apply(lambda x: '0000' if (x=='2400' and len(x)==4) 
            else '000000' if ((x=='240000' and len(x)==6))
            else x)
            ddf['end_time'] = ddf['end_time'].apply(lambda x: pd.to_datetime(x, format="%H%M").time()
            if len(x)==4 
            else pd.to_datetime(x, format="%H%M%S").time())

            return ddf

    def dtype60(df: pd.DataFrame) -> pd.DataFrame:
        data = df.loc[(df[0] == "60") & (df[1].isin(["0", "1", "2", "3", "4"]))].dropna(
            axis=1, how="all"
        ).reset_index(drop=True)
        dfh = pd.DataFrame(
            df.loc[
                (df[0].isin(["60"]))
                & (~df[1].isin(["0", "1", "2", "3", "4"]))
            ]
        ).dropna(axis=1, how="all")
        dfh = pd.DataFrame(df.loc[(df[0].isin(["S0", "L1"]))]).dropna(
            axis=1, how="all"
        ).reset_index(drop=True)

        if data.empty:
            pass
        else:
            lengthBinCode = dfh[2]
            numberOfBins = dfh[3].max()
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

            ddf.reset_index(inplace=True)

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
            direction = dfh.loc[dfh[0] == "L1", 1:3].astype(int)
            direction = direction.drop_duplicates()

            ddf['end_date'] = ddf['end_datetime'].apply(lambda x: pd.to_datetime(x, format="%y%m%d").date() if len(x)==6 else pd.to_datetime(x, format="%Y%m%d").date())

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

            # t1 = dfh.loc[dfh[0] == "S0", 1].unique()
            ddf["site_id"] = str(dfh[dfh[0]=="S0"].reset_index(drop=True).iloc[0,1]).zfill(4)
            ddf["site_id"] = ddf["site_id"].astype(str)
            ddf["start_datetime"] = pd.to_datetime(
                ddf["end_datetime"]
            ) - pd.to_timedelta(ddf["duration_min"].astype(int), unit="m")
            ddf['year'] = ddf['start_datetime'].dt.year
            ddf = ddf.drop_duplicates()
            ddf["start_datetime"] = ddf["start_datetime"].astype("datetime64[ns]")

            ddf['end_time'] = ddf['end_time'].apply(lambda x: '0000' if (x=='2400' and len(x)==4) 
            else '000000' if ((x=='240000' and len(x)==6))
            else x)
            ddf['end_time'] = ddf['end_time'].apply(lambda x: pd.to_datetime(x, format="%H%M").time()
            if len(x)==4 
            else pd.to_datetime(x, format="%H%M%S").time())

            return ddf

    def header_calcs(header: pd.DataFrame, data: pd.DataFrame, dtype: int):
        if (header is None) or (data is None):
            pass
        else:
            try:
                speed_limit_qry = f"select max_speed from trafc.countstation where tcname = '{data['site_id'].iloc[0]}' ;"
                speed_limit = pd.read_sql_query(speed_limit_qry,config.ENGINE).reset_index(drop=True)
                try:
                    speed_limit = speed_limit['max_speed'].iloc[0]
                except IndexError:
                    speed_limit = 60
                data['start_datetime'] = pd.to_datetime(data['start_datetime'])
                if dtype == 21:
                    try:
                        header['adt_total'] = data['total_vehicles_type21'].groupby([data['start_datetime'].dt.to_period('D'), data['header_id']]).sum().mean().round().astype(int)
                        header['adt_positive_direction'] = data['total_vehicles_type21'].groupby([data['start_datetime'].dt.to_period('D'), data['direction'].loc[data['direction'] == 'P'], data['header_id']]).sum().mean().round().astype(int)
                        header['adt_negative_direction'] = data['total_vehicles_type21'].groupby([data['start_datetime'].dt.to_period('D'), data['direction'].loc[data['direction'] == 'N'], data['header_id']]).sum().mean().round().astype(int)

                        header['adtt_total'] = data['total_heavy_vehicles_type21'].groupby([data['start_datetime'].dt.to_period('D'), data['header_id']]).sum().mean().round().astype(int)
                        header['adtt_positive_direction'] = data['total_vehicles_type21'].groupby([data['start_datetime'].dt.to_period('D'), data['direction'].loc[data['direction'] == 'P'], data['header_id']]).sum().mean().round().astype(int)
                        header['adtt_negative_direction'] = data['total_vehicles_type21'].groupby([data['start_datetime'].dt.to_period('D'), data['direction'].loc[data['direction'] == 'N'], data['header_id']]).sum().mean().round().astype(int)

                        header['total_vehicles'] = data['total_vehicles_type21'].groupby(data['header_id']).sum()[0]
                        header['total_vehicles_positive_direction'] = data['total_vehicles_type21'].groupby(data['direction'].loc[data['direction'] == 'P']).sum()[0]
                        header['total_vehicles_negative_direction'] = data['total_vehicles_type21'].groupby(data['direction'].loc[data['direction'] == 'N']).sum()[0]

                        header['total_heavy_vehicles'] = data['total_heavy_vehicles_type21'].groupby(data['header_id']).sum()[0]
                        header['total_heavy_negative_direction'] = data['total_heavy_vehicles_type21'].groupby([data['direction'].loc[data['direction'] == 'N'], data['header_id']]).sum()[0]
                        header['total_heavy_positive_direction'] = data['total_heavy_vehicles_type21'].groupby([data['direction'].loc[data['direction'] == 'P'], data['header_id']]).sum()[0]
                        header['truck_split_negative_direction'] = data['total_heavy_vehicles_type21'].groupby([data['direction'].loc[data['direction'] == 'N'], data['header_id']]).sum()[0] / data['total_heavy_vehicles_type21'].groupby(data['header_id']).sum()[0]
                        header['truck_split_positive_direction'] = data['total_heavy_vehicles_type21'].groupby([data['direction'].loc[data['direction'] == 'P'], data['header_id']]).sum()[0] / data['total_heavy_vehicles_type21'].groupby(data['header_id']).sum()[0]

                        header['total_light_vehicles'] = data['total_light_vehicles_type21'].groupby(data['header_id']).sum()[0]
                        header['total_light_positive_direction'] = data['total_light_vehicles_type21'].groupby([data['direction'].loc[data['direction'] == 'P'], data['header_id']]).sum()[0]
                        header['total_light_negative_direction'] = data['total_light_vehicles_type21'].groupby([data['direction'].loc[data['direction'] == 'N'], data['header_id']]).sum()[0]

                        header['short_heavy_vehicles'] = data['short_heavy_vehicles'].groupby(data['header_id']).sum()[0]
                        header['short_heavy_positive_direction'] = data['short_heavy_vehicles'].groupby([data['direction'].loc[data['direction'] == 'P'], data['header_id']]).sum()[0]
                        header['short_heavy_negative_direction'] = data['short_heavy_vehicles'].groupby([data['direction'].loc[data['direction'] == 'N'], data['header_id']]).sum()[0]

                        header['Medium_heavy_vehicles'] = data['medium_heavy_vehicles'].groupby(data['header_id']).sum()[0]
                        header['Medium_heavy_negative_direction'] = data['medium_heavy_vehicles'].groupby([data['direction'].loc[data['direction'] == 'N'], data['header_id']]).sum()[0]
                        header['Medium_heavy_positive_direction'] = data['medium_heavy_vehicles'].groupby([data['direction'].loc[data['direction'] == 'P'], data['header_id']]).sum()[0]

                        header['long_heavy_vehicles'] = data['long_heavy_vehicles'].groupby(data['header_id']).sum()[0]
                        header['long_heavy_positive_direction'] = data['long_heavy_vehicles'].groupby([data['direction'].loc[data['direction'] == 'P'], data['header_id']]).sum()[0]
                        header['long_heavy_negative_direction'] = data['long_heavy_vehicles'].groupby([data['direction'].loc[data['direction'] == 'N'], data['header_id']]).sum()[0]

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
                        try:
                            header['average_speed'] = (int((
                            (header['speedbin1'] * data['speedbin1'].groupby(data['header_id']).sum()[0]) +
                            (header['speedbin2'] * data['speedbin2'].groupby(data['header_id']).sum()[0]) +
                            (header['speedbin3'] * data['speedbin3'].groupby(data['header_id']).sum()[0]) +
                            (header['speedbin4'] * data['speedbin4'].groupby(data['header_id']).sum()[0]) +
                            (header['speedbin5'] * data['speedbin5'].groupby(data['header_id']).sum()[0]) +
                            (header['speedbin6'] * data['speedbin6'].groupby(data['header_id']).sum()[0]) +
                            (header['speedbin7'] * data['speedbin7'].groupby(data['header_id']).sum()[0]) +
                            (header['speedbin8'] * data['speedbin8'].groupby(data['header_id']).sum()[0]) +
                            (header['speedbin9'] * data['speedbin9'].groupby(data['header_id']).sum()[0]) 
                            ))
                            / data['sum_of_heavy_vehicle_speeds'].astype(int).groupby(data['header_id']).sum()[0]
                            )
                        except TypeError:
                            header['average_speed'] = (((
                            (header['speedbin1'] * data['speedbin1'].astype(int).groupby(data['header_id']).sum()[0]) +
                            (header['speedbin2'] * data['speedbin2'].astype(int).groupby(data['header_id']).sum()[0]) +
                            (header['speedbin3'] * data['speedbin3'].astype(int).groupby(data['header_id']).sum()[0]) +
                            (header['speedbin4'] * data['speedbin4'].astype(int).groupby(data['header_id']).sum()[0]) +
                            (header['speedbin5'] * data['speedbin5'].astype(int).groupby(data['header_id']).sum()[0]) +
                            (header['speedbin6'] * data['speedbin6'].astype(int).groupby(data['header_id']).sum()[0]) +
                            (header['speedbin7'] * data['speedbin7'].astype(int).groupby(data['header_id']).sum()[0]) +
                            (header['speedbin8'] * data['speedbin8'].astype(int).groupby(data['header_id']).sum()[0]) +
                            (header['speedbin9'] * data['speedbin9'].astype(int).groupby(data['header_id']).sum()[0]) 
                            ))
                            / data['sum_of_heavy_vehicle_speeds'].astype(int).groupby(data['header_id']).sum()[0]
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
                    except KeyError:
                        pass
                    try:
                        header["type_21_count_interval_minutes"] = header["type_21_count_interval_minutes"].round().astype(int)
                    except (KeyError, pd.errors.IntCastingNaNError):
                        pass

                    return header
                
                elif dtype == 30:
                    try:
                        if header['adt_total'].isnull().all():
                            header['adt_total'] = data['total_vehicles_type30'].groupby([data['start_datetime'].dt.to_period('D'), data['header_id']]).sum().mean()
                            header['adt_positive_direction'] = data['total_vehicles_type30'].groupby([data['start_datetime'].dt.to_period('D'), data['direction'].loc[data['direction'] == 'P'], data['header_id']]).sum().mean()
                            header['adt_negative_direction'] = data['total_vehicles_type30'].groupby([data['start_datetime'].dt.to_period('D'), data['direction'].loc[data['direction'] == 'N'], data['header_id']]).sum().mean()
                        else:
                            pass

                        if header['adtt_total'].isnull().all():
                            header['adtt_total'] = data['total_heavy_vehicles_type30'].groupby([data['start_datetime'].dt.to_period('D'), data['header_id']]).sum().mean()
                            header['adtt_positive_direction'] = data['total_vehicles_type30'].groupby([data['start_datetime'].dt.to_period('D'), data['direction'].loc[data['direction'] == 'P'], data['header_id']]).sum().mean()
                            header['adtt_negative_direction'] = data['total_vehicles_type30'].groupby([data['start_datetime'].dt.to_period('D'), data['direction'].loc[data['direction'] == 'N'], data['header_id']]).sum().mean()
                        else:
                            pass

                        if header['total_vehicles'].isnull().all():
                            header['total_vehicles'] = data['total_vehicles_type30'].groupby(data['header_id']).sum()[0]
                            header['total_vehicles_positive_direction'] = data['total_vehicles_type30'].groupby(data['direction'].loc[data['direction'] == 'P']).sum()[0]
                            header['total_vehicles_negative_direction'] = data['total_vehicles_type30'].groupby(data['direction'].loc[data['direction'] == 'N']).sum()[0]
                        else:
                            pass
                        
                        if header['total_heavy_vehicles'].isnull().all():
                            header['total_heavy_vehicles'] = data['total_heavy_vehicles_type21'].groupby(data['header_id']).sum()[0]
                            header['total_heavy_negative_direction'] = data['total_heavy_vehicles_type21'].groupby([data['direction'].loc[data['direction'] == 'N'], data['header_id']]).sum()[0]
                            header['total_heavy_positive_direction'] = data['total_heavy_vehicles_type21'].groupby([data['direction'].loc[data['direction'] == 'P'], data['header_id']]).sum()[0]
                            header['truck_split_negative_direction'] = data['total_heavy_vehicles_type21'].groupby([data['direction'].loc[data['direction'] == 'N'], data['header_id']]).sum()[0] / data['total_heavy_vehicles_type21'].groupby(data['header_id']).sum()[0]
                            header['truck_split_positive_direction'] = data['total_heavy_vehicles_type21'].groupby([data['direction'].loc[data['direction'] == 'P'], data['header_id']]).sum()[0] / data['total_heavy_vehicles_type21'].groupby(data['header_id']).sum()[0]
                        else:
                            pass

                        if header['total_light_vehicles'].isnull().all():
                            header['total_light_vehicles'] = data['total_light_vehicles_type30'].groupby(data['header_id']).sum()[0]
                            header['total_light_positive_direction'] = data['total_light_vehicles_type30'].groupby([data['direction'].loc[data['direction'] == 'P'], data['header_id']]).sum()[0]
                            header['total_light_negative_direction'] = data['total_light_vehicles_type30'].groupby([data['direction'].loc[data['direction'] == 'N'], data['header_id']]).sum()[0]
                        else:
                            pass
                    except KeyError:
                        pass
                        
                    try:
                        header['type_30_vehicle_classification_scheme'] = header['type_30_vehicle_classification_scheme'].round().astype(int)
                    except (KeyError, pd.errors.IntCastingNaNError):
                        pass

                    return header

                elif dtype == 70:

                    try:
                        header['type_70_maximum_gap_milliseconds'] = header['type_70_maximum_gap_milliseconds'].round().astype(int)
                    except (KeyError, pd.errors.IntCastingNaNError):
                        pass
                    
                    return header
                
                elif dtype == 10:
                    header['total_light_negative_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme']<=1)&(data['direction']=='N')].count()[0].round().astype(int)
                    header['total_light_positive_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme']<=1)&(data['direction']=='P')].count()[0].round().astype(int)
                    header['total_light_vehicles'] = data.loc[data['vehicle_class_code_secondary_scheme']<=1].count()[0].round().astype(int)
                    header['total_heavy_negative_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='N')].count()[0].round().astype(int)
                    header['total_heavy_positive_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='P')].count()[0].round().astype(int)
                    header['total_heavy_vehicles'] = data.loc[data['vehicle_class_code_secondary_scheme']>1].count()[0].round().astype(int)
                    header['total_short_heavy_negative_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='N')].count()[0].round().astype(int)
                    header['total_short_heavy_positive_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='P')].count()[0].round().astype(int)
                    header['total_short_heavy_vehicles'] = data.loc[data['vehicle_class_code_secondary_scheme']==2].count()[0].round().astype(int)
                    header['total_medium_heavy_negative_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='N')].count()[0].round().astype(int)
                    header['total_medium_heavy_positive_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='P')].count()[0].round().astype(int)
                    header['total_medium_heavy_vehicles'] = data.loc[data['vehicle_class_code_secondary_scheme']==3].count()[0].round().astype(int)
                    header['total_long_heavy_negative_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='N')].count()[0].round().astype(int)
                    header['total_long_heavy_positive_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='P')].count()[0].round().astype(int)
                    header['total_long_heavy_vehicles'] = data.loc[data['vehicle_class_code_secondary_scheme']==4].count()[0].round().astype(int)
                    header['total_vehicles_negative_direction'] = data.loc[data['direction']=='N'].count()[0].round().astype(int)
                    header['total_vehicles_positive_direction'] = data.loc[data['direction']=='P'].count()[0].round().astype(int)
                    header['total_vehicles'] = data.count()[0].round().astype(int)
                    header['average_speed_negative_direction'] = data.loc[data['direction']=='N']['vehicle_speed'].mean().round(2)
                    header['average_speed_positive_direction'] = data.loc[data['direction']=='P']['vehicle_speed'].mean().round(2)
                    header['average_speed'] = data['vehicle_speed'].mean().round(2)
                    header['average_speed_light_vehicles_negative_direction'] = data['vehicle_speed'].loc[(data['vehicle_class_code_secondary_scheme']<=1)&(data['direction']=='N')].mean().round(2)
                    header['average_speed_light_vehicles_positive_direction'] = data['vehicle_speed'].loc[(data['vehicle_class_code_secondary_scheme']<=1)&(data['direction']=='P')].mean().round(2)
                    header['average_speed_light_vehicles'] = data['vehicle_speed'].loc[data['vehicle_class_code_secondary_scheme']<=1].mean().round(2)
                    header['average_speed_heavy_vehicles_negative_direction'] = data['vehicle_speed'].loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='N')].mean().round(2)
                    header['average_speed_heavy_vehicles_positive_direction'] = data['vehicle_speed'].loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='P')].mean().round(2)
                    header['average_speed_heavy_vehicles'] = data['vehicle_speed'].loc[data['vehicle_class_code_secondary_scheme']>1].mean().round(2)
                    header['truck_split_negative_direction'] = {str((((data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='N')].count()/data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='N')].count())[0])*100).round().astype(int)) +":"+ str((((data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='N')].count()/data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='N')].count())[0])*100).round().astype(int)) +":"+ str((((data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='N')].count()/data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='N')].count())[0])*100).round().astype(int))}
                    header['truck_split_positive_direction'] = {str((((data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='P')].count()/data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='P')].count())[0])*100).round().astype(int)) +":"+ str((((data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='P')].count()/data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='P')].count())[0])*100).round().astype(int)) +":"+ str((((data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='P')].count()/data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='P')].count())[0])*100).round().astype(int))}
                    header['truck_split_total'] = {str((((data.loc[data['vehicle_class_code_secondary_scheme']==2].count()/data.loc[data['vehicle_class_code_secondary_scheme']>1].count())[0])*100).round().astype(int)) +":"+ str((((data.loc[data['vehicle_class_code_secondary_scheme']==3].count()/data.loc[data['vehicle_class_code_secondary_scheme']>1].count())[0])*100).round().astype(int)) +":"+ str((((data.loc[data['vehicle_class_code_secondary_scheme']==4].count()/data.loc[data['vehicle_class_code_secondary_scheme']>1].count())[0])*100).round().astype(int))}
                    header['estimated_axles_per_truck_negative_direction'] = ((data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='N')].count()[0]*2+data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='N')].count()[0]*5+data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='N')].count()[0]*7)/(data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='N')].count()[0]+data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='N')].count()[0]+data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='N')].count()[0])).round(2)
                    header['estimated_axles_per_truck_positive_direction'] = ((data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='P')].count()[0]*2+data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='P')].count()[0]*5+data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='P')].count()[0]*7)/(data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='P')].count()[0]+data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='P')].count()[0]+data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='P')].count()[0])).round(2)
                    header['estimated_axles_per_truck_total'] = ((data.loc[data['vehicle_class_code_secondary_scheme']==2].count()[0]*2+data.loc[data['vehicle_class_code_secondary_scheme']==3].count()[0]*5+data.loc[data['vehicle_class_code_secondary_scheme']==4].count()[0]*7)/(data.loc[data['vehicle_class_code_secondary_scheme']==2].count()[0]+data.loc[data['vehicle_class_code_secondary_scheme']==3].count()[0]+data.loc[data['vehicle_class_code_secondary_scheme']==4].count()[0])).round(2)
                    header['percentage_speeding_positive_direction'] = ((data.loc[(data['vehicle_speed']>speed_limit)&(data['direction']=='P')].count()[0]/data.loc[data['direction'=='P']].count()[0])*100).round(2)
                    header['percentage_speeding_negative_direction'] = ((data.loc[(data['vehicle_speed']>speed_limit)&(data['direction']=='N')].count()[0]/data.loc[data['direction'=='N']].count()[0])*100).round(2)
                    header['percentage_speeding_total'] = ((data.loc[data['vehicle_speed']>speed_limit].count()[0]/data.count()[0])*100).round(2)
                    header['vehicles_with_rear_to_rear_headway_less_than_2sec_negative_dire'] = data.loc[(data['vehicle_following_code']==2)&data['direction']=='N'].count()[0]
                    header['vehicles_with_rear_to_rear_headway_less_than_2sec_positive_dire'] = data.loc[(data['vehicle_following_code']==2)&data['direction']=='P'].count()[0]
                    header['vehicles_with_rear_to_rear_headway_less_than_2sec_total'] = data.loc[data['vehicle_following_code']==2].count()[0]
                    header['estimated_e80_negative_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='N')].count()[0]*0.6+data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='N')].count()[0]*2.5+data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='N')].count()[0]*2.1
                    header['estimated_e80_positive_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='P')].count()[0]*0.6+data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='P')].count()[0]*2.5+data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='P')].count()[0]*2.1
                    header['estimated_e80_on_road'] = data.loc[data['vehicle_class_code_secondary_scheme']==2].count()[0]*0.6+data.loc[data['vehicle_class_code_secondary_scheme']==3].count()[0]*2.5+data.loc[data['vehicle_class_code_secondary_scheme']==4].count()[0]*2.1
                    header['adt_negative_direction'] = data.loc[data['direction']=='N'].groupby(pd.Grouper(key='start_datetime',freq='D')).count().mean()[0].round().astype(int)
                    header['adt_positive_direction'] = data.loc[data['direction']=='P'].groupby(pd.Grouper(key='start_datetime',freq='D')).count().mean()[0].round().astype(int)
                    header['adt_total'] = data.groupby(pd.Grouper(key='start_datetime',freq='D')).count().mean()[0].round().astype(int)
                    header['adtt_negative_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='N')].groupby(pd.Grouper(key='start_datetime',freq='D')).count().mean()[0].round().astype(int)
                    header['adtt_positive_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='P')].groupby(pd.Grouper(key='start_datetime',freq='D')).count().mean()[0].round().astype(int)
                    header['adtt_total'] = data.loc[data['vehicle_class_code_secondary_scheme']>1].groupby(pd.Grouper(key='start_datetime',freq='D')).count().mean()[0].round().astype(int)
                    header['highest_volume_per_hour_negative_direction'] = data.loc[data['direction']=='N'].groupby(pd.Grouper(key='start_datetime',freq='H')).count().max()[0]
                    header['highest_volume_per_hour_positive_direction'] = data.loc[data['direction']=='P'].groupby(pd.Grouper(key='start_datetime',freq='H')).count().max()[0]
                    header['highest_volume_per_hour_total'] = data.groupby(pd.Grouper(key='start_datetime',freq='H')).count().max()[0]
                    header["15th_highest_volume_per_hour_negative_direction"] = data.loc[data['direction']=='N'].groupby(pd.Grouper(key='start_datetime',freq='D')).count().quantile(0.15)[0].round().astype(int)
                    header["15th_highest_volume_per_hour_positive_direction"] = data.loc[data['direction']=='P'].groupby(pd.Grouper(key='start_datetime',freq='D')).count().quantile(0.15)[0].round().astype(int)
                    header["15th_highest_volume_per_hour_total"] = data.groupby(pd.Grouper(key='start_datetime',freq='D')).count().quantile(0.15)[0].round().astype(int)
                    header["30th_highest_volume_per_hour_negative_direction"] = data.loc[data['direction']=='N'].groupby(pd.Grouper(key='start_datetime',freq='D')).count().quantile(0.30)[0].round().astype(int)
                    header["30th_highest_volume_per_hour_positive_direction"] = data.loc[data['direction']=='P'].groupby(pd.Grouper(key='start_datetime',freq='D')).count().quantile(0.30)[0].round().astype(int)
                    header["30th_highest_volume_per_hour_total"] = data.groupby(pd.Grouper(key='start_datetime',freq='D')).count().quantile(0.30)[0].round().astype(int)
                    header["15th_percentile_speed_negative_direction"] = data.loc[data['direction']=='N']['vehicle_speed'].quantile(0.15).round(2)
                    header["15th_percentile_speed_positive_direction"] = data.loc[data['direction']=='P']['vehicle_speed'].quantile(0.15).round(2)
                    header["15th_percentile_speed_total"] = data['vehicle_speed'].quantile(0.15).round(2)
                    header["85th_percentile_speed_negative_direction"] = data.loc[data['direction']=='N']['vehicle_speed'].quantile(0.85).round(2)
                    header["85th_percentile_speed_positive_direction"] = data.loc[data['direction']=='P']['vehicle_speed'].quantile(0.85).round(2)
                    header["85th_percentile_speed_total"] = data['vehicle_speed'].quantile(0.85).round(2)
                    header['avg_weekday_traffic'] = data.groupby(pd.Grouper(key='start_datetime',freq='B')).count().mean()[0].round().astype(int)
                    header['number_of_days_counted'] = data.groupby([data['start_datetime'].dt.to_period('D')]).count().count()[0]
                    header['duration_hours'] = data.groupby([data['start_datetime'].dt.to_period('H')]).count().count()[0]

                    return header

                elif dtype == 60:
                    
                    return header
                else:
                    return header
            except IndexError:
                return header

def process_dates_and_times(df: pd.DataFrame, date_length: int, time_length: int, duration_min: int):
    df[3] = df[3].astype(str)
    df[3].loc[df[3].str[:2] == '24'] = ('0').zfill(7)

    if date_length == 6:
        df[2] = str(date.today())[:2] + df[2]
        df[2] = df[2].apply(lambda x: pd.to_datetime(x, format="%Y%m%d").date() + timedelta(days=1)
            if x[3] == '2400000' else pd.to_datetime(x, format="%Y%m%d").date())
    elif date_length == 8:
        df[2] = df[2].apply(lambda x: pd.to_datetime(x, format="%Y%m%d").date() + timedelta(days=1)
            if x[3] == '2400000' else pd.to_datetime(x, format="%Y%m%d").date())
    else:
        raise Exception("DATA Date length abnormal")

    if time_length < 7:
        df[3] = df[3].str.pad(width=7,side='right',fillchar="0")
        df[3] = df[3].apply(lambda x: pd.to_datetime(x, format="%H%M%S%f").time())
    else:
        df[3] = df[3].apply(lambda x: pd.to_datetime(x, format="%H%M%S%f").time())

    if (df[2].astype(str)+df[3].astype(str)).map(len).isin([20]).all():
        df['end_datetime'] = pd.to_datetime(df[2].astype(str)+df[3].astype(str), 
            format='%Y-%m-%d%H:%M:%S.%f')
    else:
        df['end_datetime'] = pd.to_datetime((df[2].astype(str)+df[3].astype(str))[:18], 
            format='%Y-%m-%d%H:%M:%S')

    if (df[2].astype(str)+df[3].astype(str)).map(len).isin([20]).all():
        df['start_datetime'] = pd.to_datetime(df[2].astype(str)+df[3].astype(str), 
            format='%Y-%m-%d%H:%M:%S.%f') - timedelta(minutes=duration_min)
    else:
        df['start_datetime'] = pd.to_datetime((df[2].astype(str)+df[3].astype(str))[:18], 
            format='%Y-%m-%d%H:%M:%S') - timedelta(minutes=duration_min)

    return df

def merge_summary_dataframes(join_this_df: pd.DataFrame, onto_this_df: pd.DataFrame) -> pd.DataFrame:
    onto_this_df = pd.concat([onto_this_df, join_this_df], keys=["site_id", "start_datetime", "lane_number"], ignore_index=False, axis=1)
    onto_this_df = onto_this_df.droplevel(0, axis=1)
    onto_this_df = onto_this_df.T.drop_duplicates().T
    onto_this_df = onto_this_df.loc[:,~onto_this_df.T.duplicated(keep='first')]
    return onto_this_df

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

def select_classification_scheme(classification_scheme):
    if int(classification_scheme) == 8:
        vc_df = pd.read_sql_query(q.SELECT_CLASSIFICAITON_SCHEME_8, config.ENGINE)
    elif int(classification_scheme) == 1:
        vc_df = pd.read_sql_query(q.SELECT_CLASSIFICAITON_SCHEME_1, config.ENGINE)
    elif int(classification_scheme) == 5:
        vc_df = pd.read_sql_query(q.SELECT_CLASSIFICAITON_SCHEME_5, config.ENGINE)
    elif int(classification_scheme) == 9:
        vc_df = pd.read_sql_query(q.SELECT_CLASSIFICAITON_SCHEME_9, config.ENGINE)
    else:
        vc_df = None
    return vc_df

def convert_columns_to_int(df):
    # df.columns = df.columns.astype(str)
    # for col_name, col_value in df.iteritems():
    #     df = 
    return df
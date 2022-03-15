import pandas as pd
from datetime import timedelta

#################################################################################################################################################################################################################################
#################################################################################################################################################################################################################################
##########################################################################				  DATA RECORDSS BELOW									 #############################################################################
#################################################################################################################################################################################################################################
#################################################################################################################################################################################################################################
class Data(object):
    def __init__(self, df):
        self.dtype21 = Data.dtype21(df)
        self.dtype30 = Data.dtype30(df)
        self.dtype70 = Data.dtype70(df)
        self.dtype10 = Data.dtype10(df)
        self.dtype60 = Data.dtype60(df)

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

    #### CREATE DATA FOR TYPE 21 TRAFFIC COUNTS
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
        data = df.loc[(df[0] == "10") & (df[1].isin(["15", "17", "19"]))].dropna(
            axis=1, how="all"
        )
        dfh2 = pd.DataFrame(df.loc[(df[0].isin(["S0", "L1"]))]).dropna(
            axis=1, how="all"
        )
        if data.empty:
            pass
        else:
            ddf = data.iloc[:, 4:]
            ddf = pd.DataFrame(ddf).dropna(axis=1, how="all")
            if (
                data[1].all() == "15"
                or data[1].all() == "17"
                and len(ddf.columns) == 11
            ):
                ddf.columns = [
                    "start_datetime",
                    "departure_time",
                    "lane_number",
                    "physical_lane_number",
                    "forward_1_or_reverse_code_2",
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
                                "vehicle_Tag_Code",
                                "Trailer_count",
                                "axle_count",
                                "bumper_to_1st_axle_spacing",
                                "sub_data_type_code_axle_spacing",
                                "number_of_axles_spacings_counted",
                            ]
                        ),
                    ]
                )
            elif (
                data[1].all() == "15"
                or data[1].all() == "17"
                and len(ddf.columns) == 13
            ):
                ddf.columns = [
                    "start_datetime",
                    "departure_time",
                    "lane_number",
                    "physical_lane_number",
                    "forward_1_or_reverse_code_2",
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
                                "vehicle_Tag_Code",
                                "Trailer_count",
                                "axle_count",
                                "bumper_to_1st_axle_spacing",
                                "sub_data_type_code_axle_spacing",
                                "number_of_axles_spacings_counted",
                            ]
                        ),
                    ]
                )
            elif (
                data[1].all() == "15"
                or data[1].all() == "17"
                and len(ddf.columns) == 15
            ):
                ddf.columns = [
                    "start_datetime",
                    "departure_time",
                    "lane_number",
                    "physical_lane_number",
                    "forward_1_or_reverse_code_2",
                    "vehicle_category",
                    "vehicle_class_code_primary_scheme",
                    "vehicle_class_code_secondary_scheme",
                    "vehicle_speed",
                    "vehicle_length",
                    "site_occupancy_time_in_milliseconds",
                    "chassis_height_code",
                    "vehicle_following_code",
                    "vehicle_Tag_Code",
                    "Trailer_count",
                ]
                ddf = pd.concat(
                    [
                        ddf,
                        pd.DataFrame(
                            columns=[
                                "axle_count",
                                "bumper_to_1st_axle_spacing",
                                "sub_data_type_code_axle_spacing",
                                "number_of_axles_spacings_counted",
                            ]
                        ),
                    ]
                )
            elif data[1].all() == "19":
                ddf = data.iloc[:, 4:22]
                ddf = pd.DataFrame(ddf).dropna(axis=1, how="all")
                ddf.columns = [
                    "start_datetime",
                    "departure_time",
                    "lane_number",
                    "physical_lane_number",
                    "forward_1_or_reverse_code_2",
                    "vehicle_category",
                    "vehicle_class_code_primary_scheme",
                    "vehicle_class_code_secondary_scheme",
                    "vehicle_speed",
                    "vehicle_length",
                    "site_occupancy_time_in_milliseconds",
                    "chassis_height_code",
                    "vehicle_following_code",
                    "vehicle_Tag_Code",
                    "Trailer_count" "axle_count",
                    "bumper_to_1st_axle_spacing",
                    "sub_data_type_code_axle_spacing",
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

            if ddf["start_datetime"].map(len).all() == 8:
                ddf["start_datetime"] = pd.to_datetime(
                    ddf["start_datetime"] + ddf["departure_time"],
                    format="%Y%m%d%H%M%S%f",
                )
            elif ddf["start_datetime"].map(len).all() == 6:
                ddf["start_datetime"] = pd.to_datetime(
                    ddf["start_datetime"] + ddf["departure_time"],
                    format="%y%m%d%H%M%S%f",
                )
            # ddf['year'] = ddf['start_datetime'].dt.year
            t1 = dfh2.loc[dfh2[0] == "S0", 1].unique()
            ddf["site_id"] = str(t1[0])
            ddf["site_id"] = ddf["site_id"].astype(str)

            ddf.iloc[:, 2:17] = ddf.iloc[:, 2:17].apply(to_numeric)
            ddf[21] = ddf[21].astype(str)
            ddf.iloc[:, 2:17] = ddf.iloc[:, 2:17].apply(to_numeric)
            ddf = ddf.drop(["departure_time"], axis=1)

            ddf = ddf.drop_duplicates()
            ddf["start_datetime"] = ddf["start_datetime"].astype("datetime64[ns]")
            return ddf

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

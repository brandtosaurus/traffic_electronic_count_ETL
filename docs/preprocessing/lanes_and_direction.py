import pandas as pd
import traceback
import gc
import os
import csv
import config


def get_lanes(df: pd.DataFrame, site_id: str) -> pd.DataFrame:
    """
    It takes a dataframe, checks if it's empty, if it's not empty, it checks if the first column is
    equal to "L1", if it is, it drops all columns that have null values, drops duplicates, resets
    the index, and copies the dataframe. If the dataframe is empty, it checks if the first column is
    equal to "L0", if it is, it drops all columns that have null values, drops duplicates, resets
    the index, and copies the dataframe. It then sets the max_lanes variable to the first value in
    the second column of the dataframe. It then renames the columns. If the dataframe has 5 columns,
    it renames the columns to "lane_number", "direction_code", "lane_type_code", and
    "traffic_stream_number". If the dataframe has 11 columns, it renames the columns to
    "lane_number", "direction

    :param df: pd.DataFrame
    :type df: pd.DataFrame
    :return: A dataframe with the columns:
    """
    if df.empty:
        pass
    else:
        lanes = (
            df.loc[df[0] == "L1"]
            .dropna(axis=1)
            .drop_duplicates()
            .reset_index(drop=True)
            .copy()
        )
        if lanes.empty:
            lanes = (
                df.loc[df[0] == "L0"]
                .dropna(axis=1)
                .drop_duplicates()
                .reset_index(drop=True)
                .copy()
            )
            max_lanes = lanes[1].astype(int).at[0]
            lanes.rename(
                columns={1: "num_assigned_lanes", 2: "num_physical_lanes"},
                inplace=True,
            )
        else:
            lanes = lanes.drop(
                [17, 18, 19, 20, 21, 22, 23, 24, 25], axis=1, errors="ignore"
            )
            if lanes.shape[1] == 5:
                lanes.rename(
                    columns={
                        1: "lane_number",
                        2: "direction_code",
                        3: "lane_type_code",
                        4: "traffic_stream_number",
                    },
                    inplace=True,
                )
            elif lanes.shape[1] == 11:
                lanes.rename(
                    columns={
                        1: "lane_number",
                        2: "direction_code",
                        3: "lane_type_code",
                        4: "traffic_stream_number",
                        5: "traffic_stream_lane_position",
                        6: "reverse_direction_lane_number",
                        7: "vehicle_code",
                        8: "time_code",
                        9: "length_code",
                        10: "speed_code",
                    },
                    inplace=True,
                )
            elif lanes.shape[1] == 17:
                lanes.rename(
                    columns={
                        1: "lane_number",
                        2: "direction_code",
                        3: "lane_type_code",
                        4: "traffic_stream_number",
                        5: "traffic_stream_lane_position",
                        6: "reverse_direction_lane_number",
                        7: "vehicle_code",
                        8: "time_code",
                        9: "length_code",
                        10: "speed_code",
                        11: "occupancy_time_code",
                        12: "vehicle_following_code",
                        13: "trailer_code",
                        14: "axle_code",
                        15: "mass_code",
                        16: "tyre_type_code",
                    },
                    inplace=True,
                )
            else:
                lanes.rename(
                    columns={
                        1: "lane_number",
                        2: "direction_code",
                        3: "lane_type_code",
                        4: "traffic_stream_number",
                    },
                    inplace=True,
                )
            lanes[lanes.select_dtypes(include=["object"]).columns] = lanes[
                lanes.select_dtypes(include=["object"]).columns
            ].apply(pd.to_numeric, axis=1, errors="ignore")
            lanes["site_name"] = site_id
            lanes["site_id"] = site_id
            try:
                max_lanes = int(lanes["lane_number"].astype(int).max())
            except ValueError:
                max_lanes = int(lanes["lane_number"].drop_duplicates().count())
            return lanes, max_lanes


def get_direction(body_df: pd.DataFrame, type: str, lanes_df: pd.DataFrame, max_lanes):
    """
    It takes a dataframe and returns a dataframe with the columns 'direction', 'direction_code', and
    'compass_heading' added

    :param body_df: the dataframe that contains the data
    :return: A dataframe with the following columns:
    """
    try:
        if type == "indv":
            lane_col = 6
        else:
            lane_col = 5
        try:
            dir_1 = lanes_df["direction_code"].astype(int).min()
            dir_2 = lanes_df["direction_code"].astype(int).max()
        except (TypeError, ValueError):
            dir_1 = 0
            dir_2 = 4

        direction_1 = "P"
        direction_2 = "N"

        if dir_1 == dir_2:
            dir_2 = dir_1
            direction_2 = direction_1
        else:
            pass

        body_df["direction"] = body_df[lane_col].astype(int)
        body_df["compass_heading"] = body_df[lane_col].astype(int)
        body_df["direction_code"] = body_df[lane_col].astype(int)
        body_df["direction"].loc[
            body_df[lane_col]
            .astype(int)
            .isin(
                list(
                    lanes_df["lane_number"]
                    .astype(int)
                    .loc[lanes_df["direction_code"].astype(int) == dir_1]
                )
            )
        ] = direction_1
        body_df["direction"].loc[
            body_df[lane_col]
            .astype(int)
            .isin(
                list(
                    lanes_df["lane_number"]
                    .astype(int)
                    .loc[lanes_df["direction_code"].astype(int) == dir_2]
                )
            )
        ] = direction_2
        body_df["compass_heading"].loc[
            body_df[lane_col]
            .astype(int)
            .isin(
                list(
                    lanes_df["lane_number"]
                    .astype(int)
                    .loc[lanes_df["direction_code"].astype(int) == dir_1]
                )
            )
        ] = str(dir_1)
        body_df["compass_heading"].loc[
            body_df[lane_col]
            .astype(int)
            .isin(
                list(
                    lanes_df["lane_number"]
                    .astype(int)
                    .loc[lanes_df["direction_code"].astype(int) == dir_2]
                )
            )
        ] = str(dir_2)
        body_df["direction_code"].loc[
            body_df[lane_col]
            .astype(int)
            .isin(
                list(
                    lanes_df["lane_number"]
                    .astype(int)
                    .loc[lanes_df["direction_code"].astype(int) == dir_2]
                )
            )
        ] = str(dir_2)
        body_df["direction_code"].loc[
            body_df[lane_col]
            .astype(int)
            .isin(
                list(
                    lanes_df["lane_number"]
                    .astype(int)
                    .loc[lanes_df["direction_code"].astype(int) == dir_1]
                )
            )
        ] = str(dir_1)

        return body_df
    except (KeyError, TypeError):
        body_df["direction"] = body_df.apply(
            lambda x: "P" if int(x[lane_col]) <= int(max_lanes) / 2 else "N",
            axis=1,
        )
        body_df["direction_code"] = body_df.apply(
            lambda x: 0 if int(x[lane_col]) <= int(max_lanes) / 2 else 4,
            axis=1,
        )
        body_df["compass_heading"] = body_df.apply(
            lambda x: 0 if int(x[lane_col]) <= int(max_lanes) / 2 else 4,
            axis=1,
        )
        return body_df

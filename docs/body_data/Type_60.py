import pandas as pd
import csv
import os
import gc

import config
from uuid import UUID
from typing import List
from helpers import push_to_db


def type_60(
    body_df: pd.DataFrame,
    head_df: pd.DataFrame,
    site_id: str,
    header_id: UUID,
    max_lanes: int,
    t60_cols: List,
    file: str,
) -> pd.DataFrame:
    """
    It takes a dataframe, and returns a dataframe
    :return: A dataframe
    """
    if body_df is None:
        pass
    else:
        data = (
            body_df.loc[(body_df[0] == "60")]
            .dropna(axis=1, how="all")
            .reset_index(drop=True)
            .copy()
        )
        dfh = (
            head_df.loc[(head_df[0] == "60")]
            .dropna(axis=1, how="all")
            .drop_duplicates()
            .reset_index(drop=True)
            .copy()
        )
        if data.empty:
            pass
        else:
            dfh["error_bin"] = 0
            number_of_data_records = dfh.iloc[0, 3]

            if data[1].isin(["0", "1", "2", "3", "4"]).any():
                ddf = data.iloc[:, 1:].reset_index(drop=True)
                ddf = pd.DataFrame(ddf).dropna(axis=1, how="all").reset_index(drop=True)

                ddf[ddf.select_dtypes(include=["object"]).columns] = ddf[
                    ddf.select_dtypes(include=["object"]).columns
                ].apply(pd.to_numeric, axis=1, errors="ignore")
                ddf.columns = ddf.columns.astype(str)

                df3 = pd.DataFrame(
                    columns=[
                        "edit_code",
                        "start_datetime",
                        "end_date",
                        "end_time",
                        "duration_of_summary",
                        "lane_number",
                        "bin_number",
                        "number_of_vehicles",
                        "bin_boundary_length_cm",
                        "direction",
                        "compass_heading",
                    ]
                )

                for i in range(6, int(number_of_data_records) + 6):
                    for lane_no in range(1, int(max_lanes) + 1):
                        join_to_df3 = ddf.loc[
                            ddf["5"].astype(int) == lane_no,
                            [
                                "1",  # edit_code
                                "start_datetime",
                                "end_date",  # end_date
                                "end_time",  # end_time
                                "4",  # duration_of_summary
                                "5",  # lane_number
                                str(i),  # "number_of_vehicles"
                                "direction",
                                "compass_heading",
                            ],
                        ]
                        if str(int(i) - 6) == "0":
                            bin_bound_col = "error_bin"
                        else:
                            bin_bound_col = int(i) - 3
                        join_to_df3["bin_number"] = str(i - 6)
                        join_to_df3["bin_boundary_length_cm"] = int(
                            dfh[bin_bound_col][0]
                        )
                        join_to_df3.rename(
                            columns={
                                "1": "edit_code",
                                "4": "duration_of_summary",
                                "5": "lane_number",
                                str(i): "number_of_vehicles",
                            },
                            inplace=True,
                        )
                        df3 = pd.concat([df3, join_to_df3], axis=0, ignore_index=True)
                df3 = df3.apply(pd.to_numeric, axis=1, errors="ignore")
                df3["site_id"] = site_id
                df3["header_id"] = header_id
                try:
                    if df3 is None:
                        pass
                    else:
                        df3 = df3[df3.columns.intersection(t60_cols)]
                        push_to_db(df3, config.TYPE_60_TBL_NAME)
                except Exception as exc:
                    traceback.print_exc()
                    with open(
                        os.path.expanduser(config.FILES_FAILED),
                        "a",
                        newline="",
                    ) as f:
                        write = csv.writer(f)
                        write.writerows([[file]])
                    gc.collect()
            else:
                pass
            return df3

    if body_df is None:
        pass
    else:
        data = (
            body_df.loc[(body_df[0] == "21")]
            .dropna(axis=1, how="all")
            .reset_index(drop=True)
            .copy()
        )
        if data.empty:
            pass
        else:
            if (data[1] == "0").any():
                ddf = data
                ddf.rename(
                    columns={
                        4: "duration_min",
                        5: "lane_number",
                        6: "speedbin1",
                        7: "speedbin2",
                        8: "speedbin3",
                        9: "speedbin4",
                        10: "speedbin5",
                        11: "speedbin6",
                        12: "speedbin7",
                        13: "speedbin8",
                        14: "speedbin9",
                        15: "speedbin10",
                        16: "sum_of_heavy_vehicle_speeds",
                        17: "short_heavy_vehicles",
                        18: "medium_heavy_vehicles",
                        19: "long_heavy_vehicles",
                        20: "rear_to_rear_headway_shorter_than_2_seconds",
                        21: "rear_to_rear_headways_shorter_than_programmed_time",
                    },
                    inplace=True,
                )
                ddf["speedbin0"] = 0

            elif (data[1] == "1").any():
                ddf = data
                ddf.rename(
                    columns={
                        4: "duration_min",
                        5: "lane_number",
                        6: "speedbin0",
                        7: "speedbin1",
                        8: "speedbin2",
                        9: "speedbin3",
                        10: "speedbin4",
                        11: "speedbin5",
                        12: "speedbin6",
                        13: "speedbin7",
                        14: "speedbin8",
                        15: "speedbin9",
                        16: "speedbin10",
                        17: "sum_of_heavy_vehicle_speeds",
                        18: "short_heavy_vehicles",
                        19: "medium_heavy_vehicles",
                        20: "long_heavy_vehicles",
                        21: "rear_to_rear_headway_shorter_than_2_seconds",
                        22: "rear_to_rear_headways_shorter_than_programmed_time",
                    },
                    inplace=True,
                )

            ddf = ddf.fillna(0)

            ddf[
                [
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
            ] = ddf[
                [
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
            ].astype(
                int
            )

            ddf["total_heavy_vehicles"] = (
                ddf[
                    [
                        "short_heavy_vehicles",
                        "medium_heavy_vehicles",
                        "long_heavy_vehicles",
                    ]
                ]
                .astype(int)
                .sum(axis=1)
            )

            ddf["total_light_vehicles"] = (
                ddf[
                    [
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
                    ]
                ]
                .astype(int)
                .sum()
                - ddf[
                    [
                        "short_heavy_vehicles",
                        "medium_heavy_vehicles",
                        "long_heavy_vehicles",
                    ]
                ]
                .astype(int)
                .sum()
            )

            ddf["total_vehicles"] = (
                ddf[
                    [
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
                    ]
                ]
                .astype(int)
                .sum()
            )

            try:
                ddf["year"] = ddf["start_datetime"].dt.year.astype(int)
            except AttributeError:
                ddf["year"] = int(ddf["start_datetime"].str[:4][0])

            ddf["site_id"] = site_id
            ddf["header_id"] = header_id

            return ddf

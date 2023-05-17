import pandas as pd
from uuid import UUID
from typing import List
import config
import numpy as np


def type_10(
    body_df: pd.DataFrame,
    data: pd.DataFrame,
    site_id: str,
    header_id: UUID,
    t10_cols: List,
) -> pd.DataFrame:
    """
    It takes a dataframe, checks if it's empty, if not, it checks if it's empty, if not, it checks
    if it's empty, if not, it does some stuff, then it checks if it's empty, if not, it does some
    stuff, then it checks if it's empty, if not, it does some stuff, then it checks if it's empty,
    if not, it does some stuff, then it checks if it's empty, if not, it does some stuff, then it
    checks if it's empty, if not, it does some stuff, then it checks if it's empty, if not, it does
    some stuff, then it checks if it's empty, if not, it does some stuff, then it checks if it's
    empty, if not, it does some stuff, then it checks if it's empty, if not, it does some stuff,
    then it checks if it's empty, if not, it does
    :return: A tuple of two dataframes.
    """
    if body_df is None:
        pass
    else:
        if body_df.empty:
            return None
        elif data is None:
            return None
        else:
            data = (
                body_df.loc[(body_df[0] == "10")]
                .dropna(axis=1, how="all")
                .reset_index(drop=True)
                .copy()
            )

            num_of_fields = int(data.iloc[:, 1].unique()[0])
            ddf = data.iloc[:, : 2 + num_of_fields]
            ddf.rename(
                columns=config.RENAME_TYPE10_DATA_COLUMNS, inplace=True, errors="ignore"
            )
            ddf["data_id"] = ddf.apply(lambda x: uuid.uuid4(), axis=1)

            if data.shape[1] > ddf.shape[1]:
                sub_body_df = pd.DataFrame(
                    columns=[
                        "sub_data_type_code",
                        "offset_sensor_detection_code",
                        "mass_measurement_resolution_kg",
                        "number",
                        "value",
                    ]
                )
                for index, row in data.iterrows():
                    col = int(row[1]) + 2
                    while col < len(row) - 5 and row[col] is not None:
                        sub_data_type = row[col]
                        col += 1
                        no_of_type = int(row[col])
                        col += 1
                        if sub_data_type[0].lower() in ["w", "a", "g"]:
                            odc = row[col]
                            col += 1
                            mmr = row[col]
                            col += 1
                            for i in range(0, no_of_type):
                                tempdf = pd.DataFrame(
                                    [[sub_data_type, odc, mmr, i + 1, row[col]]],
                                    columns=[
                                        "sub_data_type_code",
                                        "offset_sensor_detection_code",
                                        "mass_measurement_resolution_kg",
                                        "number",
                                        "value",
                                    ],
                                )
                                sub_body_df = pd.concat([sub_body_df, tempdf])
                                col += 1
                        elif sub_data_type[0].lower() in ["s", "t", "c"]:
                            for i in range(0, no_of_type):
                                tempdf = pd.DataFrame(
                                    [[sub_data_type, i + 1, row[col]]],
                                    columns=["sub_data_type_code", "number", "value"],
                                )
                                sub_body_df = pd.concat([sub_body_df, tempdf])
                                col += 1
                        elif sub_data_type[0].lower() in ["v"]:
                            odc = row[col]
                            col += 1
                            for i in range(0, no_of_type):
                                tempdf = pd.DataFrame(
                                    [[sub_data_type, odc, i + 1, row[col]]],
                                    columns=[
                                        "sub_data_type_code",
                                        "offset_sensor_detection_code",
                                        "number",
                                        "value",
                                    ],
                                )
                                sub_body_df = pd.concat([sub_body_df, tempdf])
                                col += 1
            else:
                sub_body_df = pd.DataFrame(
                    columns=[
                        "sub_data_type_code",
                        "offset_sensor_detection_code",
                        "mass_measurement_resolution_kg",
                        "number",
                        "value",
                    ]
                )

            sub_body_df = sub_body_df.merge(
                ddf["data_id"], how="left", left_index=True, right_index=True
            )

            ddf = ddf.fillna(0)
            ddf["assigned_lane_number"] = ddf["assigned_lane_number"].astype(int)
            ddf["lane_number"] = ddf["physical_lane_number"].astype(int)
            ddf["physical_lane_number"] = ddf["physical_lane_number"].astype(int)

            ddf = ddf.replace(r"^\s*$", np.NaN, regex=True)
            sub_body_df = sub_body_df.replace(r"^\s*$", np.NaN, regex=True)
            sub_body_df = sub_body_df.drop("index", axis=1, errors="ignore")

            scols = ddf.select_dtypes("object").columns
            ddf[scols] = ddf[scols].apply(pd.to_numeric, axis=1, errors="ignore")

            ddf["year"] = ddf["start_datetime"].dt.year
            ddf["site_id"] = site_id
            ddf["header_id"] = header_id

            ddf = ddf[ddf.columns.intersection(t10_cols)]

            return ddf, sub_body_df

import pandas as pd
from datetime import timedelta, date
import traceback
import config


def header_calcs(
    header: pd.DataFrame, data: pd.DataFrame, type: int, site_id
) -> pd.DataFrame:
    """
    It takes a dataframe, and a type, and returns a dataframe

    :param header: a dataframe with the header information
    :type header: pd.DataFrame
    :param data: pd.DataFrame
    :type data: pd.DataFrame
    :param type: int
    :type type: int
    :return: A dataframe with the calculated values.
    """
    try:
        if data is None and header is None:
            pass
        else:
            speed_limit_qry = (
                f"select max_speed from trafc.countstation where tcname = '{site_id}' ;"
            )
            speed_limit = pd.read_sql_query(speed_limit_qry, config.ENGINE).reset_index(
                drop=True
            )
            try:
                speed_limit = speed_limit["max_speed"].iloc[0]
            except IndexError:
                speed_limit = 60
            data = data.fillna(0)
            if type == 21:
                try:
                    header["adt_total"] = (
                        data["total_vehicles"]
                        .groupby([data["start_datetime"].dt.to_period("D")])
                        .sum()
                        .mean()
                        .round()
                        .astype(int)
                    )
                    try:
                        header["adt_positive_direction"] = round(
                            data["total_vehicles"]
                            .groupby(
                                [
                                    data["start_datetime"].dt.to_period("D"),
                                    data["direction"].loc[data["direction"] == "P"],
                                ]
                            )
                            .sum()
                            .mean()
                        )
                    except ValueError:
                        header["adt_positive_direction"] = 0
                    try:
                        header["adt_negative_direction"] = round(
                            data["total_vehicles"]
                            .groupby(
                                [
                                    data["start_datetime"].dt.to_period("D"),
                                    data["direction"].loc[data["direction"] == "N"],
                                ]
                            )
                            .sum()
                            .mean()
                        )
                    except ValueError:
                        header["adt_negative_direction"] = 0

                    header["adtt_total"] = (
                        data["total_heavy_vehicles"]
                        .groupby([data["start_datetime"].dt.to_period("D")])
                        .sum()
                        .mean()
                        .round()
                        .astype(int)
                    )
                    try:
                        header["adtt_positive_direction"] = round(
                            data["total_vehicles"]
                            .groupby(
                                [
                                    data["start_datetime"].dt.to_period("D"),
                                    data["direction"].loc[data["direction"] == "P"],
                                ]
                            )
                            .sum()
                            .mean()
                        )
                    except ValueError:
                        header["adtt_positive_direction"] = 0
                    try:
                        header["adtt_negative_direction"] = round(
                            data["total_vehicles"]
                            .groupby(
                                [
                                    data["start_datetime"].dt.to_period("D"),
                                    data["direction"].loc[data["direction"] == "N"],
                                ]
                            )
                            .sum()
                            .mean()
                        )
                    except ValueError:
                        header["adtt_negative_direction"] = 0

                    header["total_vehicles"] = data["total_vehicles"].astype(int).sum()
                    try:
                        header["total_vehicles_positive_direction"] = (
                            data["total_vehicles"]
                            .groupby(data["direction"].loc[data["direction"] == "P"])
                            .astype(int)
                            .sum()[0]
                        )
                    except IndexError:
                        header["total_vehicles_positive_direction"] = 0
                    try:
                        header["total_vehicles_negative_direction"] = (
                            data["total_vehicles"]
                            .groupby(data["direction"].loc[data["direction"] == "N"])
                            .astype(int)
                            .sum()[0]
                        )
                    except IndexError:
                        header["total_vehicles_negative_direction"] = 0

                    header["total_heavy_vehicles"] = data["total_heavy_vehicles"].sum()
                    try:
                        header["total_heavy_negative_direction"] = (
                            data["total_heavy_vehicles"]
                            .groupby([data["direction"].loc[data["direction"] == "N"]])
                            .astype(int)
                            .sum()[0]
                        )
                    except IndexError:
                        header["total_heavy_negative_direction"] = 0
                    try:
                        header["total_heavy_positive_direction"] = (
                            data["total_heavy_vehicles"]
                            .groupby([data["direction"].loc[data["direction"] == "P"]])
                            .astype(int)
                            .sum()[0]
                        )
                    except IndexError:
                        header["total_heavy_positive_direction"] = 0
                    try:
                        header["truck_split_negative_direction"] = (
                            data["total_heavy_vehicles"]
                            .groupby([data["direction"].loc[data["direction"] == "N"]])
                            .sum()[0]
                            / data["total_heavy_vehicles"].sum()
                        )
                    except IndexError:
                        header["truck_split_negative_direction"] = 0
                    try:
                        header["truck_split_positive_direction"] = (
                            data["total_heavy_vehicles"]
                            .groupby([data["direction"].loc[data["direction"] == "P"]])
                            .sum()[0]
                            / data["total_heavy_vehicles"].sum()
                        )
                    except IndexError:
                        header["truck_split_positive_direction"] = 0

                    header["total_light_vehicles"] = data["total_light_vehicles"].sum()
                    try:
                        header["total_light_positive_direction"] = (
                            data["total_light_vehicles"]
                            .groupby([data["direction"].loc[data["direction"] == "P"]])
                            .sum()[0]
                        )
                    except IndexError:
                        header["total_light_positive_direction"] = 0
                    try:
                        header["total_light_negative_direction"] = (
                            data["total_light_vehicles"]
                            .groupby([data["direction"].loc[data["direction"] == "N"]])
                            .sum()[0]
                        )
                    except IndexError:
                        header["total_light_negative_direction"] = 0

                    header["short_heavy_vehicles"] = data["short_heavy_vehicles"].sum()
                    try:
                        header["short_heavy_positive_direction"] = (
                            data["short_heavy_vehicles"]
                            .groupby([data["direction"].loc[data["direction"] == "P"]])
                            .sum()[0]
                        )
                    except IndexError:
                        header["short_heavy_positive_direction"] = 0
                    try:
                        header["short_heavy_negative_direction"] = (
                            data["short_heavy_vehicles"]
                            .groupby([data["direction"].loc[data["direction"] == "N"]])
                            .sum()[0]
                        )
                    except IndexError:
                        header["short_heavy_negative_direction"] = 0

                    header["Medium_heavy_vehicles"] = data[
                        "medium_heavy_vehicles"
                    ].sum()
                    try:
                        header["Medium_heavy_negative_direction"] = (
                            data["medium_heavy_vehicles"]
                            .groupby([data["direction"].loc[data["direction"] == "N"]])
                            .sum()[0]
                        )
                    except IndexError:
                        header["Medium_heavy_negative_direction"] = 0
                    try:
                        header["Medium_heavy_positive_direction"] = (
                            data["medium_heavy_vehicles"]
                            .groupby([data["direction"].loc[data["direction"] == "P"]])
                            .sum()[0]
                        )
                    except IndexError:
                        header["Medium_heavy_positive_direction"] = 0

                    header["long_heavy_vehicles"] = data["long_heavy_vehicles"].sum()
                    try:
                        header["long_heavy_positive_direction"] = (
                            data["long_heavy_vehicles"]
                            .groupby([data["direction"].loc[data["direction"] == "P"]])
                            .sum()[0]
                        )
                    except IndexError:
                        header["long_heavy_positive_direction"] = 0
                    try:
                        header["long_heavy_negative_direction"] = (
                            data["long_heavy_vehicles"]
                            .groupby([data["direction"].loc[data["direction"] == "N"]])
                            .sum()[0]
                        )
                    except IndexError:
                        header["long_heavy_negative_direction"] = 0

                    try:
                        header[
                            "vehicles_with_rear_to_rear_headway_less_than_2sec_positive_dire"
                        ] = (
                            data["rear_to_rear_headway_shorter_than_2_seconds"]
                            .groupby([data["direction"].loc[data["direction"] == "P"]])
                            .sum()[0]
                        )
                    except IndexError:
                        header[
                            "vehicles_with_rear_to_rear_headway_less_than_2sec_positive_dire"
                        ] = 0
                    try:
                        header[
                            "vehicles_with_rear_to_rear_headway_less_than_2sec_negative_dire"
                        ] = (
                            data["rear_to_rear_headway_shorter_than_2_seconds"]
                            .groupby([data["direction"].loc[data["direction"] == "N"]])
                            .sum()[0]
                        )
                    except IndexError:
                        header[
                            "vehicles_with_rear_to_rear_headway_less_than_2sec_negative_dire"
                        ] = 0
                    header[
                        "vehicles_with_rear_to_rear_headway_less_than_2sec_total"
                    ] = data["rear_to_rear_headway_shorter_than_2_seconds"].sum()

                    header["summary_interval_minutes"] = data["duration_min"].mean()

                    try:
                        header["highest_volume_per_hour_positive_direction"] = round(
                            data["total_vehicles"]
                            .groupby(
                                [
                                    data["start_datetime"].dt.to_period("H"),
                                    data["direction"].loc[data["direction"] == "P"],
                                ]
                            )
                            .sum()
                            .max()
                        )
                    except ValueError:
                        header["highest_volume_per_hour_positive_direction"] = 0
                    try:
                        header["highest_volume_per_hour_negative_direction"] = round(
                            data["total_vehicles"]
                            .groupby(
                                [
                                    data["start_datetime"].dt.to_period("H"),
                                    data["direction"].loc[data["direction"] == "N"],
                                ]
                            )
                            .sum()
                            .max()
                        )
                    except ValueError:
                        header["highest_volume_per_hour_negative_direction"] = 0
                    header["highest_volume_per_hour_total"] = (
                        data["total_vehicles"]
                        .groupby([data["start_datetime"].dt.to_period("H")])
                        .sum()
                        .max()
                    )

                    try:
                        header[
                            "15th_highest_volume_per_hour_positive_direction"
                        ] = round(
                            data["total_vehicles"]
                            .groupby(
                                [
                                    data["start_datetime"].dt.to_period("H"),
                                    data["direction"].loc[data["direction"] == "P"],
                                ]
                            )
                            .sum()
                            .quantile(q=0.15, interpolation="linear")
                        )
                    except ValueError:
                        header["15th_highest_volume_per_hour_positive_direction"] = 0
                    try:
                        header[
                            "15th_highest_volume_per_hour_negative_direction"
                        ] = round(
                            data["total_vehicles"]
                            .groupby(
                                [
                                    data["start_datetime"].dt.to_period("H"),
                                    data["direction"].loc[data["direction"] == "N"],
                                ]
                            )
                            .sum()
                            .quantile(q=0.15, interpolation="linear")
                        )
                    except ValueError:
                        header["15th_highest_volume_per_hour_negative_direction"] = 0
                    header["15th_highest_volume_per_hour_total"] = round(
                        data["total_vehicles"]
                        .groupby([data["start_datetime"].dt.to_period("H")])
                        .sum()
                        .quantile(q=0.15, interpolation="linear")
                    )

                    try:
                        header[
                            "30th_highest_volume_per_hour_positive_direction"
                        ] = round(
                            data["total_vehicles"]
                            .groupby(
                                [
                                    data["start_datetime"].dt.to_period("H"),
                                    data["direction"].loc[data["direction"] == "P"],
                                ]
                            )
                            .sum()
                            .quantile(q=0.3, interpolation="linear")
                        )
                    except ValueError:
                        header["30th_highest_volume_per_hour_positive_direction"] = 0
                    try:
                        header[
                            "30th_highest_volume_per_hour_negative_direction"
                        ] = round(
                            data["total_vehicles"]
                            .groupby(
                                [
                                    data["start_datetime"].dt.to_period("H"),
                                    data["direction"].loc[data["direction"] == "N"],
                                ]
                            )
                            .sum()
                            .quantile(q=0.3, interpolation="linear")
                        )
                    except ValueError:
                        header["30th_highest_volume_per_hour_negative_direction"] = 0
                    header["30th_highest_volume_per_hour_total"] = round(
                        data["total_vehicles"]
                        .groupby([data["start_datetime"].dt.to_period("H")])
                        .sum()
                        .quantile(q=0.3, interpolation="linear")
                    )

                    header["average_speed_positive_direction"] = (
                        round(
                            (
                                (
                                    header["speedbin1"]
                                    * data.loc[
                                        data["direction"] == "P", "speedbin1"
                                    ].sum()
                                )
                                + (
                                    header["speedbin2"]
                                    * data.loc[
                                        data["direction"] == "P", "speedbin2"
                                    ].sum()
                                )
                                + (
                                    header["speedbin3"]
                                    * data.loc[
                                        data["direction"] == "P", "speedbin3"
                                    ].sum()
                                )
                                + (
                                    header["speedbin4"]
                                    * data.loc[
                                        data["direction"] == "P", "speedbin4"
                                    ].sum()
                                )
                                + (
                                    header["speedbin5"]
                                    * data.loc[
                                        data["direction"] == "P", "speedbin5"
                                    ].sum()
                                )
                                + (
                                    header["speedbin6"]
                                    * data.loc[
                                        data["direction"] == "P", "speedbin6"
                                    ].sum()
                                )
                                + (
                                    header["speedbin7"]
                                    * data.loc[
                                        data["direction"] == "P", "speedbin7"
                                    ].sum()
                                )
                                + (
                                    header["speedbin8"]
                                    * data.loc[
                                        data["direction"] == "P", "speedbin8"
                                    ].sum()
                                )
                                + (
                                    header["speedbin9"]
                                    * data.loc[
                                        data["direction"] == "P", "speedbin9"
                                    ].sum()
                                )
                            )
                        )
                        / data.loc[
                            data["direction"] == "P", "sum_of_heavy_vehicle_speeds"
                        ]
                        .astype(int)
                        .sum()
                    )

                    header["average_speed_negative_direction"] = (
                        round(
                            (
                                (
                                    header["speedbin1"]
                                    * data.loc[
                                        data["direction"] == "N", "speedbin1"
                                    ].sum()
                                )
                                + (
                                    header["speedbin2"]
                                    * data.loc[
                                        data["direction"] == "N", "speedbin2"
                                    ].sum()
                                )
                                + (
                                    header["speedbin3"]
                                    * data.loc[
                                        data["direction"] == "N", "speedbin3"
                                    ].sum()
                                )
                                + (
                                    header["speedbin4"]
                                    * data.loc[
                                        data["direction"] == "N", "speedbin4"
                                    ].sum()
                                )
                                + (
                                    header["speedbin5"]
                                    * data.loc[
                                        data["direction"] == "N", "speedbin5"
                                    ].sum()
                                )
                                + (
                                    header["speedbin6"]
                                    * data.loc[
                                        data["direction"] == "N", "speedbin6"
                                    ].sum()
                                )
                                + (
                                    header["speedbin7"]
                                    * data.loc[
                                        data["direction"] == "N", "speedbin7"
                                    ].sum()
                                )
                                + (
                                    header["speedbin8"]
                                    * data.loc[
                                        data["direction"] == "N", "speedbin8"
                                    ].sum()
                                )
                                + (
                                    header["speedbin9"]
                                    * data.loc[
                                        data["direction"] == "N", "speedbin9"
                                    ].sum()
                                )
                            )
                        )
                        / data.loc[
                            data["direction"] == "N", "sum_of_heavy_vehicle_speeds"
                        ]
                        .astype(int)
                        .sum()
                    )

                    header["average_speed"] = (
                        round(
                            (
                                (header["speedbin1"] * data["speedbin1"].sum())
                                + (header["speedbin2"] * data["speedbin2"].sum())
                                + (header["speedbin3"] * data["speedbin3"].sum())
                                + (header["speedbin4"] * data["speedbin4"].sum())
                                + (header["speedbin5"] * data["speedbin5"].sum())
                                + (header["speedbin6"] * data["speedbin6"].sum())
                                + (header["speedbin7"] * data["speedbin7"].sum())
                                + (header["speedbin8"] * data["speedbin8"].sum())
                                + (header["speedbin9"] * data["speedbin9"].sum())
                            )
                        )
                        / data["total_vehicles"].astype(int).sum()
                    )

                    header["average_speed_light_vehicles_positive_direction"] = (
                        round(
                            (
                                (
                                    header["speedbin1"]
                                    * data.loc[
                                        data["direction"] == "P", "speedbin1"
                                    ].sum()
                                )
                                + (
                                    header["speedbin2"]
                                    * data.loc[
                                        data["direction"] == "P", "speedbin2"
                                    ].sum()
                                )
                                + (
                                    header["speedbin3"]
                                    * data.loc[
                                        data["direction"] == "P", "speedbin3"
                                    ].sum()
                                )
                                + (
                                    header["speedbin4"]
                                    * data.loc[
                                        data["direction"] == "P", "speedbin4"
                                    ].sum()
                                )
                                + (
                                    header["speedbin5"]
                                    * data.loc[
                                        data["direction"] == "P", "speedbin5"
                                    ].sum()
                                )
                                + (
                                    header["speedbin6"]
                                    * data.loc[
                                        data["direction"] == "P", "speedbin6"
                                    ].sum()
                                )
                                + (
                                    header["speedbin7"]
                                    * data.loc[
                                        data["direction"] == "P", "speedbin7"
                                    ].sum()
                                )
                                + (
                                    header["speedbin8"]
                                    * data.loc[
                                        data["direction"] == "P", "speedbin8"
                                    ].sum()
                                )
                                + (
                                    header["speedbin9"]
                                    * data.loc[
                                        data["direction"] == "P", "speedbin9"
                                    ].sum()
                                )
                                - data.loc[
                                    data["direction"] == "P",
                                    "sum_of_heavy_vehicle_speeds",
                                ].sum()
                            )
                        )
                        / data.loc[data["direction"] == "P", "total_light_vehicles"]
                        .astype(int)
                        .sum()
                    )

                    header["average_speed_light_vehicles_negative_direction"] = (
                        round(
                            (
                                (
                                    header["speedbin1"]
                                    * data.loc[
                                        data["direction"] == "N", "speedbin1"
                                    ].sum()
                                )
                                + (
                                    header["speedbin2"]
                                    * data.loc[
                                        data["direction"] == "N", "speedbin2"
                                    ].sum()
                                )
                                + (
                                    header["speedbin3"]
                                    * data.loc[
                                        data["direction"] == "N", "speedbin3"
                                    ].sum()
                                )
                                + (
                                    header["speedbin4"]
                                    * data.loc[
                                        data["direction"] == "N", "speedbin4"
                                    ].sum()
                                )
                                + (
                                    header["speedbin5"]
                                    * data.loc[
                                        data["direction"] == "N", "speedbin5"
                                    ].sum()
                                )
                                + (
                                    header["speedbin6"]
                                    * data.loc[
                                        data["direction"] == "N", "speedbin6"
                                    ].sum()
                                )
                                + (
                                    header["speedbin7"]
                                    * data.loc[
                                        data["direction"] == "N", "speedbin7"
                                    ].sum()
                                )
                                + (
                                    header["speedbin8"]
                                    * data.loc[
                                        data["direction"] == "N", "speedbin8"
                                    ].sum()
                                )
                                + (
                                    header["speedbin9"]
                                    * data.loc[
                                        data["direction"] == "N", "speedbin9"
                                    ].sum()
                                )
                                - data.loc[
                                    data["direction"] == "N",
                                    "sum_of_heavy_vehicle_speeds",
                                ].sum()
                            )
                        )
                        / data.loc[data["direction"] == "N", "total_light_vehicles"]
                        .astype(int)
                        .sum()
                    )

                    header["average_speed_light_vehicles"] = (
                        round(
                            (header["speedbin1"] * data["speedbin1"].sum())
                            + (header["speedbin2"] * data["speedbin2"].sum())
                            + (header["speedbin3"] * data["speedbin3"].sum())
                            + (header["speedbin4"] * data["speedbin4"].sum())
                            + (header["speedbin5"] * data["speedbin5"].sum())
                            + (header["speedbin6"] * data["speedbin6"].sum())
                            + (header["speedbin7"] * data["speedbin7"].sum())
                            + (header["speedbin8"] * data["speedbin8"].sum())
                            + (header["speedbin9"] * data["speedbin9"].sum())
                            - data["sum_of_heavy_vehicle_speeds"].sum()
                        )
                        / data["total_light_vehicles"].sum()
                    )

                    header["average_speed_heavy_vehicles_positive_direction"] = int(
                        (
                            data.loc[
                                data["direction"] == "P", "sum_of_heavy_vehicle_speeds"
                            ].sum()
                            / data.loc[
                                data["direction"] == "P", "total_heavy_vehicles"
                            ].sum()
                        )
                    )

                    header["average_speed_heavy_vehicles_negative_direction"] = int(
                        (
                            data.loc[
                                data["direction"] == "N", "sum_of_heavy_vehicle_speeds"
                            ].sum()
                            / data.loc[
                                data["direction"] == "N", "total_heavy_vehicles"
                            ].sum()
                        )
                    )

                    header["average_speed_heavy_vehicles"] = int(
                        (
                            data["sum_of_heavy_vehicle_speeds"].sum()
                            / data["total_heavy_vehicles"].sum()
                        )
                    )

                    try:
                        shv_p = str(
                            round(
                                round(
                                    data["short_heavy_vehicles"]
                                    .groupby(
                                        [
                                            data["direction"].loc[
                                                data["direction"] == "P"
                                            ]
                                        ]
                                    )
                                    .sum()
                                    .values[0]
                                )
                                / round(
                                    data["total_heavy_vehicles"]
                                    .groupby(
                                        [
                                            data["direction"].loc[
                                                data["direction"] == "P"
                                            ]
                                        ]
                                    )
                                    .sum()
                                    .values[0]
                                    * 100
                                )
                            )
                        )
                    except (ValueError, IndexError, ZeroDivisionError):
                        shv_p = 0
                    try:
                        mhv_p = str(
                            round(
                                round(
                                    data["medium_heavy_vehicles"]
                                    .groupby(
                                        [
                                            data["direction"].loc[
                                                data["direction"] == "P"
                                            ]
                                        ]
                                    )
                                    .sum()
                                    .values[0]
                                )
                                / round(
                                    data["total_heavy_vehicles"]
                                    .groupby(
                                        [
                                            data["direction"].loc[
                                                data["direction"] == "P"
                                            ]
                                        ]
                                    )
                                    .sum()[0]
                                    * 100
                                )
                            )
                        )
                    except (ValueError, IndexError, ZeroDivisionError):
                        mhv_p = 0
                    try:
                        lhv_p = str(
                            round(
                                round(
                                    data["long_heavy_vehicles"]
                                    .groupby(
                                        [
                                            data["direction"].loc[
                                                data["direction"] == "P"
                                            ]
                                        ]
                                    )
                                    .sum()
                                    .values[0]
                                )
                                / round(
                                    data["total_heavy_vehicles"]
                                    .groupby(
                                        [
                                            data["direction"].loc[
                                                data["direction"] == "P"
                                            ]
                                        ]
                                    )
                                    .sum()[0]
                                    * 100
                                )
                            )
                        )
                    except (ValueError, IndexError, ZeroDivisionError):
                        lhv_p = 0
                    header["truck_split_positive_direction"] = str(
                        str(shv_p) + " : " + str(mhv_p) + " : " + str(lhv_p)
                    )

                    try:
                        shv_n = str(
                            round(
                                round(
                                    data["short_heavy_vehicles"]
                                    .groupby(
                                        [
                                            data["direction"].loc[
                                                data["direction"] == "N"
                                            ]
                                        ]
                                    )
                                    .sum()
                                    .values[0]
                                )
                                / round(
                                    data["total_heavy_vehicles"]
                                    .groupby(
                                        [
                                            data["direction"].loc[
                                                data["direction"] == "N"
                                            ]
                                        ]
                                    )
                                    .sum()
                                    .values[0]
                                    * 100
                                )
                            )
                        )
                    except (ValueError, IndexError, ZeroDivisionError):
                        shv_n = "0"
                    try:
                        mhv_n = str(
                            round(
                                round(
                                    data["medium_heavy_vehicles"]
                                    .groupby(
                                        [
                                            data["direction"].loc[
                                                data["direction"] == "N"
                                            ]
                                        ]
                                    )
                                    .sum()
                                    .values[0]
                                )
                                / round(
                                    data["total_heavy_vehicles"]
                                    .groupby(
                                        [
                                            data["direction"].loc[
                                                data["direction"] == "N"
                                            ]
                                        ]
                                    )
                                    .sum()[0]
                                    * 100
                                )
                            )
                        )
                    except (ValueError, IndexError, ZeroDivisionError):
                        mhv_n = "0"
                    try:
                        lhv_n = str(
                            round(
                                round(
                                    data["long_heavy_vehicles"]
                                    .groupby(
                                        [
                                            data["direction"].loc[
                                                data["direction"] == "N"
                                            ]
                                        ]
                                    )
                                    .sum()
                                    .values[0]
                                )
                                / round(
                                    data["total_heavy_vehicles"]
                                    .groupby(
                                        [
                                            data["direction"].loc[
                                                data["direction"] == "N"
                                            ]
                                        ]
                                    )
                                    .sum()[0]
                                    * 100
                                )
                            )
                        )
                    except (ValueError, IndexError, ZeroDivisionError):
                        lhv_n = "0"
                    header["truck_split_positive_direction"] = str(
                        str(shv_n) + " : " + str(mhv_n) + " : " + str(lhv_n)
                    )

                    try:
                        shv_t = str(
                            round(
                                round(data["short_heavy_vehicles"].sum())
                                / round(data["total_heavy_vehicles"].sum() * 100)
                            )
                        )
                    except (ValueError, IndexError, ZeroDivisionError):
                        shv_t = "0"
                    try:
                        mhv_t = str(
                            round(
                                round(data["medium_heavy_vehicles"].sum())
                                / round(data["total_heavy_vehicles"].sum() * 100)
                            )
                        )
                    except (ValueError, IndexError, ZeroDivisionError):
                        mhv_t = "0"
                    try:
                        lhv_t = str(
                            round(
                                round(data["long_heavy_vehicles"].sum())
                                / round(data["total_heavy_vehicles"].sum() * 100)
                            )
                        )
                    except (ValueError, IndexError, ZeroDivisionError):
                        lhv_t = "0"
                    header["truck_split_total"] = str(
                        str(shv_t) + " : " + str(mhv_t) + " : " + str(lhv_t)
                    )

                    header["percentage_heavy_vehicles_positive_direction"] = float(
                        round(
                            data.loc[
                                data["direction"] == "P", "heavy_vehicles_type21"
                            ].sum()
                            / data.loc[
                                data["direction"] == "P", "total_vehicles"
                            ].sum(),
                            2,
                        )
                    )
                    header["percentage_heavy_vehicles_negative_direction"] = float(
                        round(
                            data.loc[
                                data["direction"] == "N", "heavy_vehicles_type21"
                            ].sum()
                            / data.loc[
                                data["direction"] == "N", "total_vehicles"
                            ].sum(),
                            2,
                        )
                    )
                    header["percentage_heavy_vehicles"] = float(
                        round(
                            data["heavy_vehicles_type21"].sum()
                            / data["total_vehicles"].sum(),
                            2,
                        )
                    )

                    header["night_adt"] = data.loc[
                        data["start_datetime"].dt.hour >= 18
                        and data["start_datetime"].dt.hour <= 6
                    ]["total_vehicles"].sum()
                    header["night_adtt"] = data.loc[
                        data["start_datetime"].dt.hour >= 18
                        and data["start_datetime"].dt.hour <= 6
                    ]["total_heavy_vehicles"].sum()

                    header["number_of_days_counted"] = (
                        data.groupby(data["start_datetime"].dt.to_period("D"))
                        .count()
                        .count()[0]
                    )
                    header["duration_hours"] = (
                        data.groupby(data["start_datetime"].dt.to_period("H"))
                        .count()
                        .count()[0]
                    )

                    # TODO: add percentile speeds (15th, 30th, 85th)

                except KeyError:
                    pass
                try:
                    header["summary_interval_minutes"] = (
                        header["summary_interval_minutes"].round().astype(int)
                    )
                except (KeyError, pd.errors.IntCastingNaNError):
                    pass

                return header

            elif type == 30:
                if header["total_vehicles"].isna().all():
                    header["total_vehicles"] = data["number_of_vehicles"].sum()
                    header["total_vehicles_positive_direction"] = (
                        data["number_of_vehicles"]
                        .groupby([data["direction"].loc["direction"] == "P"])
                        .sum()
                        .values[0]
                    )
                    header["total_vehicles_negative_direction"] = (
                        data["number_of_vehicles"]
                        .groupby([data["direction"].loc["direction"] == "N"])
                        .sum()
                        .values[0]
                    )
                    header["total_heavy_vehicles"] = data.loc[
                        data["vehicle_class"].isin(int) > 1
                    ].sum()
                try:
                    if header["adt_total"].isna().all():
                        header["adt_total"] = (
                            data["total_vehicles"]
                            .groupby([data["start_datetime"].dt.to_period("D")])
                            .sum()
                            .mean()
                            .astype(int)
                        )
                        try:
                            header["adt_positive_direction"] = round(
                                data["total_vehicles"]
                                .groupby(
                                    [
                                        data["start_datetime"].dt.to_period("D"),
                                        data["direction"].loc[data["direction"] == "P"],
                                    ]
                                )
                                .sum()
                                .mean()
                            )
                        except (ValueError, IndexError):
                            header["adt_positive_direction"] = 0
                        try:
                            header["adt_negative_direction"] = round(
                                data["total_vehicles"]
                                .groupby(
                                    [
                                        data["start_datetime"].dt.to_period("D"),
                                        data["direction"].loc[data["direction"] == "N"],
                                    ]
                                )
                                .sum()
                                .mean()
                            )
                        except (ValueError, IndexError):
                            header["adt_negative_direction"] = 0
                    else:
                        pass

                    if header["adtt_total"].isna().all():
                        header["adtt_total"] = (
                            data["total_heavy_vehicles"]
                            .groupby([data["start_datetime"].dt.to_period("D")])
                            .sum()
                            .mean()
                            .astype(int)
                        )
                        try:
                            header["adtt_positive_direction"] = round(
                                data["total_vehicles"]
                                .groupby(
                                    [
                                        data["start_datetime"].dt.to_period("D"),
                                        data["direction"].loc[data["direction"] == "P"],
                                    ]
                                )
                                .sum()
                                .mean()
                            )
                        except (ValueError, IndexError):
                            header["adtt_positive_direction"] = 0
                        try:
                            header["adtt_negative_direction"] = round(
                                data["total_vehicles"]
                                .groupby(
                                    [
                                        data["start_datetime"].dt.to_period("D"),
                                        data["direction"].loc[data["direction"] == "N"],
                                    ]
                                )
                                .sum()
                                .mean()
                            )
                        except (ValueError, IndexError):
                            header["adtt_negative_direction"] = 0
                    else:
                        pass

                    if header["total_vehicles"].isna().all():
                        header["total_vehicles"] = data["total_vehicles"].sum()
                        try:
                            header["total_vehicles_positive_direction"] = (
                                data["total_vehicles"]
                                .groupby(
                                    data["direction"].loc[data["direction"] == "P"]
                                )
                                .sum()[0]
                            )
                        except (ValueError, IndexError):
                            header["total_vehicles_positive_direction"] = 0
                        try:
                            header["total_vehicles_negative_direction"] = (
                                data["total_vehicles"]
                                .groupby(
                                    data["direction"].loc[data["direction"] == "N"]
                                )
                                .sum()[0]
                            )
                        except (ValueError, IndexError):
                            header["total_vehicles_negative_direction"] = 0
                    else:
                        pass

                    if header["total_heavy_vehicles"].isna().all():
                        header["total_heavy_vehicles"] = data[
                            "total_heavy_vehicles"
                        ].sum()
                        try:
                            header["total_heavy_negative_direction"] = (
                                data["total_heavy_vehicles"]
                                .groupby(
                                    [data["direction"].loc[data["direction"] == "N"]]
                                )
                                .sum()[0]
                            )
                        except (ValueError, IndexError):
                            header["total_heavy_negative_direction"] = 0
                        try:
                            header["total_heavy_positive_direction"] = (
                                data["total_heavy_vehicles"]
                                .groupby(
                                    [data["direction"].loc[data["direction"] == "P"]]
                                )
                                .sum()[0]
                            )
                        except (ValueError, IndexError):
                            header["total_heavy_positive_direction"] = 0
                        try:
                            header["truck_split_negative_direction"] = (
                                data["total_heavy_vehicles"]
                                .groupby(
                                    [data["direction"].loc[data["direction"] == "N"]]
                                )
                                .sum()[0]
                                / data["total_heavy_vehicles"].sum()
                            )
                        except (ValueError, IndexError):
                            header["truck_split_negative_direction"] = 0
                        try:
                            header["truck_split_positive_direction"] = (
                                data["total_heavy_vehicles"]
                                .groupby(
                                    [data["direction"].loc[data["direction"] == "P"]]
                                )
                                .sum()[0]
                                / data["total_heavy_vehicles"].sum()
                            )
                        except (ValueError, IndexError):
                            header["truck_split_positive_direction"] = 0
                    else:
                        pass

                    if header["total_light_vehicles"].isna().all():
                        header["total_light_vehicles"] = data[
                            "total_light_vehicles"
                        ].sum()
                        try:
                            header["total_light_positive_direction"] = (
                                data["total_light_vehicles"]
                                .groupby(
                                    [data["direction"].loc[data["direction"] == "P"]]
                                )
                                .sum()[0]
                            )
                        except (ValueError, IndexError):
                            header["total_light_positive_direction"] = 0
                        try:
                            header["total_light_negative_direction"] = (
                                data["total_light_vehicles"]
                                .groupby(
                                    [data["direction"].loc[data["direction"] == "N"]]
                                )
                                .sum()[0]
                            )
                        except (ValueError, IndexError):
                            header["total_light_negative_direction"] = 0
                    else:
                        pass
                except KeyError:
                    pass

                try:
                    header["vehicle_classification_scheme"] = (
                        header["vehicle_classification_scheme"].round().astype(int)
                    )
                except (KeyError, pd.errors.IntCastingNaNError):
                    pass

                header["number_of_days_counted"] = (
                    data.groupby(data["start_datetime"].dt.to_period("D"))
                    .count()
                    .count()[0]
                )
                header["duration_hours"] = (
                    data.groupby(data["start_datetime"].dt.to_period("H"))
                    .count()
                    .count()[0]
                )

                return header

            elif type == 70:
                if header["total_vehicles"].isna().all():
                    header["total_vehicles"] = (
                        data[
                            [
                                "number_of_error_vehicles",
                                "total_free_flowing_light_vehicles",
                                "total_following_light_vehicles",
                                "total_free_flowing_heavy_vehicles",
                                "total_following_heavy_vehicles",
                            ]
                        ]
                        .astype(int)
                        .sum()
                    )
                    header["total_light_vehicles"] = (
                        data[
                            [
                                "total_free_flowing_light_vehicles",
                                "total_following_light_vehicles",
                            ]
                        ]
                        .astype(int)
                        .sum()
                    )
                    header["total_heavy_vehicles"] = (
                        data[
                            [
                                "total_free_flowing_heavy_vehicles",
                                "total_following_heavy_vehicles",
                            ]
                        ]
                        .astype(int)
                        .sum()
                    )
                    try:
                        header["maximum_gap_milliseconds"] = (
                            header["maximum_gap_milliseconds"].round().astype(int)
                        )
                    except (KeyError, pd.errors.IntCastingNaNError):
                        pass

                    header["number_of_days_counted"] = (
                        data.groupby(data["start_datetime"].dt.to_period("D"))
                        .count()
                        .count()[0]
                    )
                    header["duration_hours"] = (
                        data.groupby(data["start_datetime"].dt.to_period("H"))
                        .count()
                        .count()[0]
                    )
                else:
                    pass

                return header

            elif type == 60:
                return header

            else:
                return header
    except (IndexError, AttributeError):
        return header

import pandas as pd
from datetime import timedelta
import uuid

class Headers(object):
    def __init__(self, df: pd.DataFrame, file: str):
        self.df = Headers.get_head(df)
        self.header = Headers.headers(self.df, file)
        self.lanes = Headers.lanes(self.df)

    def get_head(df) -> pd.DataFrame:
        df = pd.DataFrame(
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
        df["index"] = df.index
        breaks = df["index"].diff() != 1
        groups = breaks.cumsum()
        df["newindex"] = groups
        df = df.set_index("newindex")
        df = df.drop(columns=["index"])
        return df

    def headers(dfh: pd.DataFrame, file: str) -> pd.DataFrame:
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
            except:
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
                headers["type_21_count_interval_minutes"] = headers["type_21_count_interval_minutes"].round().astype(int)
            except pd.errors.IntCastingNaNError:
                pass

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

            headers['document_url'] = file

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


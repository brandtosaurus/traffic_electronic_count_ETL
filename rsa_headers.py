import pandas as pd
from datetime import timedelta
import uuid


class Headers(object):
    def __init__(self, df):
        self.get_head = Headers.get_head(df)
        self.header = Headers.headers(self.get_head)

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

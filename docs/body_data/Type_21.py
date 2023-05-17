import pandas as pd

def type_21(body_df, site_id) -> pd.DataFrame:
    if body_df is None:
        pass
    else:
        data = body_df.loc[(body_df[0] == "21")].dropna(
            axis=1, how="all"
        ).reset_index(drop=True).copy()
        if data.empty:
            pass
        else:
            if (data[1] == "0").any():
                ddf = data
                ddf.rename(columns={
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
                    21: "rear_to_rear_headways_shorter_than_programmed_time"
                }, inplace=True)
                ddf["speedbin0"] = 0

            elif (data[1] == "1").any():
                ddf = data
                ddf.rename(columns={
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
                }, inplace=True)

            ddf = ddf.fillna(0)

            ddf[["duration_min",
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
                    "rear_to_rear_headways_shorter_than_programmed_time"
                    ]] = ddf[["duration_min",
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
                            "rear_to_rear_headways_shorter_than_programmed_time"]].astype(int)

            ddf["total_heavy_vehicles"] = (
                ddf[["short_heavy_vehicles", "medium_heavy_vehicles",
                    "long_heavy_vehicles"]].astype(int).sum(axis=1)
            )

            ddf["total_light_vehicles"] = (
                ddf[["speedbin0", "speedbin1", "speedbin2", "speedbin3", "speedbin4", "speedbin5", "speedbin6", "speedbin7", "speedbin8", "speedbin9", "speedbin10"]].astype(
                    int).sum() - ddf[["short_heavy_vehicles", "medium_heavy_vehicles", "long_heavy_vehicles"]].astype(int).sum()
            )

            ddf["total_vehicles"] = (ddf[["speedbin0", "speedbin1", "speedbin2", "speedbin3", "speedbin4", "speedbin5", "speedbin6", "speedbin7", "speedbin8", "speedbin9", "speedbin10"]].astype(int).sum()
                                        )

            try:
                ddf['year'] = ddf['start_datetime'].dt.year.astype(int)
            except AttributeError:
                ddf['year'] = int(ddf['start_datetime'].str[:4][0])

            ddf["site_id"] = site_id
            # ddf["header_id"] = header_id

            return ddf
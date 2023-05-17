import pandas as pd
import numpy as np


def type_70(body_df, site_id) -> pd.DataFrame:
    if body_df is None:
        pass
    else:
        data = (
            body_df.loc[(body_df[0] == "70")]
            .dropna(axis=1, how="all")
            .reset_index(drop=True)
            .copy()
        )
        if data.empty:
            pass
        else:
            if data[1].all() == "1":
                ddf = data.iloc[:, 3:]
                ddf = pd.DataFrame(ddf).dropna(axis=1, how="all").reset_index(drop=True)

                ddf.rename(
                    columns={
                        4: "duration_min",
                        5: "lane_number",
                        6: "number_of_error_vehicles",
                        7: "total_free_flowing_light_vehicles",
                        8: "total_following_light_vehicles",
                        9: "total_free_flowing_heavy_vehicles",
                        10: "total_following_heavy_vehicles",
                        11: "sum_of_inverse_of_speeds_for_free_flowing_lights",
                        12: "sum_of_inverse_of_speeds_for_following_lights",
                        13: "sum_of_inverse_of_speeds_for_free_flowing_heavies",
                        14: "sum_of_inverse_of_speeds_for_following_heavies",
                        15: "sum_of_speeds_for_free_flowing_lights",
                        16: "sum_of_speeds_for_following_lights",
                        17: "sum_of_speeds_for_free_flowing_heavies",
                        18: "sum_of_speeds_for_following_heavies",
                        19: "sum_of_squared_speeds_of_free_flowing_lights",
                        20: "sum_of_squared_speeds_for_following_lights",
                        21: "sum_of_squared_speeds_of_free_flowing_heavies",
                        22: "sum_of_squared_speeds_for_following_heavies",
                    },
                    inplace=True,
                )

            else:
                ddf = data.iloc[:, 2:]
                ddf = pd.DataFrame(ddf).dropna(axis=1, how="all").reset_index(drop=True)
                ddf.rename(
                    columns={
                        4: "duration_min",
                        5: "lane_number",
                        6: "total_free_flowing_light_vehicles",
                        7: "total_following_light_vehicles",
                        8: "total_free_flowing_heavy_vehicles",
                        9: "total_following_heavy_vehicles",
                        10: "sum_of_inverse_of_speeds_for_free_flowing_lights",
                        11: "sum_of_inverse_of_speeds_for_following_lights",
                        12: "sum_of_inverse_of_speeds_for_free_flowing_heavies",
                        13: "sum_of_inverse_of_speeds_for_following_heavies",
                        14: "sum_of_speeds_for_free_flowing_lights",
                        15: "sum_of_speeds_for_following_lights",
                        16: "sum_of_speeds_for_free_flowing_heavies",
                        17: "sum_of_speeds_for_following_heavies",
                        18: "sum_of_squared_speeds_of_free_flowing_lights",
                        19: "sum_of_squared_speeds_for_following_lights",
                        20: "sum_of_squared_speeds_of_free_flowing_heavies",
                        21: "sum_of_squared_speeds_for_following_heavies",
                    },
                    inplace=True,
                )
                ddf["number_of_error_vehicles"] = 0

            ddf = ddf.fillna(0)

            ddf[
                [
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
            ] = ddf[
                [
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
            ].astype(
                int
            )

            m = ddf.select_dtypes(np.number)
            ddf[m.columns] = m.round().astype(int)
            ddf["year"] = ddf["start_datetime"].dt.year.astype(int)
            ddf["site_id"] = site_id
            # ddf["header_id"] = header_id

            return ddf

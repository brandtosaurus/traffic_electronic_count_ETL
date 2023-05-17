import pandas as pd


def type_30(body_df, site_id, head_df, max_lanes) -> pd.DataFrame:
    """
    It takes a dataframe, splits it into two dataframes, then loops through the second dataframe and
    inserts the data into a database
    :return: A dataframe
    """
    if body_df is None:
        pass
    else:
        data = (
            body_df.loc[(body_df[0] == "30")]
            .dropna(axis=1, how="all")
            .reset_index(drop=True)
            .copy()
        )
        header = (
            head_df.loc[(head_df[0] == "30")]
            .dropna(axis=1, how="all")
            .reset_index(drop=True)
            .copy()
        )

        if data.empty:
            pass
        else:
            if header.shape[1] > 3:
                classification_scheme = header.iloc[0, 3]
                number_of_data_records = header.iloc[0, 4]
            else:
                classification_scheme = header.iloc[0, 2]
                number_of_data_records = header.iloc[0, 3]

            # vc_df = select_classification_scheme(classification_scheme)

            if data[1].isin(["0", "2", 0, 2]).any():
                ddf = data.iloc[:, 1:].dropna(axis=1, how="all").reset_index(drop=True)

                ddf[ddf.select_dtypes(include=["object"]).columns] = ddf[
                    ddf.select_dtypes(include=["object"]).columns
                ].apply(pd.to_numeric, axis=1, errors="ignore")

                ddf["vehicle_classification_scheme"] = int(classification_scheme)

                ddf.columns = ddf.columns.astype(str)

                df3 = pd.DataFrame(
                    columns=[
                        "edit_code",
                        "start_datetime",
                        "end_date",
                        "end_time",
                        "duration_of_summary",
                        "lane_number",
                        "number_of_vehicles",
                        "class_number",
                        "direction",
                        "compass_heading",
                    ]
                )
                for lane_no in range(1, int(max_lanes) + 1):
                    for i in range(6, int(number_of_data_records) + 6):
                        join_to_df3 = ddf.loc[
                            ddf["5"].astype(int) == lane_no,
                            [
                                "1",
                                "start_datetime",
                                "end_date",
                                "end_time",
                                "4",
                                "5",
                                str(i),
                                "direction",
                                "compass_heading",
                            ],
                        ]
                        join_to_df3["class_number"] = i - 6
                        join_to_df3.rename(
                            columns={
                                "1": "edit_code",
                                "2": "end_date",
                                "3": "end_time",
                                "4": "duration_of_summary",
                                "5": "lane_number",
                                str(i): "number_of_vehicles",
                            },
                            inplace=True,
                        )
                        # TODO: test efficiency of this vs merge then insert
                        # OPTIMIZE: merge then insert
                        # df3 = pd.concat([df3, join_to_df3], keys=[
                        #                 'start_datetime', 'lane_number'], ignore_index=True, axis=0)
                        # df3 = df3.apply(pd.to_numeric, axis=1, errors="ignore")
                        # df3["header_id"] = header_id
                        join_to_df3["classification_scheme"] = int(
                            classification_scheme
                        )
                        join_to_df3["site_id"] = site_id
                        join_to_df3["year"] = int(data["start_datetime"].at[0].year)
                        join_to_df3[
                            [
                                "edit_code",
                                "duration_of_summary",
                                "lane_number",
                                "number_of_vehicles",
                                "class_number",
                                "compass_heading",
                                "classification_scheme",
                                "year",
                            ]
                        ] = join_to_df3[
                            [
                                "edit_code",
                                "duration_of_summary",
                                "lane_number",
                                "number_of_vehicles",
                                "class_number",
                                "compass_heading",
                                "classification_scheme",
                                "year",
                            ]
                        ].astype(
                            int
                        )  # .apply(pd.to_numeric, errors="coerce")
                        push_to_db(join_to_df3, config.TYPE_30_TBL_NAME)
                return df3
            else:
                pass

from wim_header_upsert import (
    wim_header_upsert_func1,
    wim_header_upsert_func2,
    wim_header_upsert,
)


class Wim:
    def __init__(self, data, head_df, header_id, site_id, pt_cols) -> None:
        self.header_id = header_id
        self.site_id = site_id
        self.data_df = data
        self.head_df = head_df
        self.pt_cols = pt_cols
        if self.head_df is None:
            pass
        else:
            self.primary_vehicle_class = int(
                self.head_df.loc[self.head_df[0] == "10", 1].values[0]
            )
            self.secondary_vehicle_class = int(
                self.head_df.loc[self.head_df[0] == "10", 2].values[0]
            )
        self.header_ids = self.get_headers_to_update()
        self.t10_cols = list(
            pd.read_sql_query(
                q.SELECT_ELECTRONIC_COUNT_DATA_TYPE_10_LIMIT1, config.ENGINE
            ).columns
        )
        self.wim_header_cols = list(
            pd.read_sql_query(q.SEELECT_HSWIM_HEADER_COLS, config.ENGINE).columns
        )
        self.vm_limits = pd.read_sql_query(
            f"SELECT * FROM {config.TRAFFIC_LOOKUP_SCHEMA}.gross_vehicle_mass_limits;",
            config.ENGINE,
        )

    def update_existing(self, header_ids: pd.DataFrame) -> None:
        """
        It takes a list of header_ids, and for each header_id, it runs a series of functions that calculate
        values for the header_id, and then upserts the calculated values into the database
        """
        for header_id in list(header_ids["header_id"].astype(str)):
            if header_id is None:
                pass
            else:
                (
                    SELECT_TYPE10_QRY,
                    AXLE_SPACING_SELECT_QRY,
                    WHEEL_MASS_SELECT_QRY,
                ) = self.wim_header_upsert_func1(header_id)
                df, df2, df3 = self.wim_header_upsert_func2(
                    SELECT_TYPE10_QRY, AXLE_SPACING_SELECT_QRY, WHEEL_MASS_SELECT_QRY
                )
                if df2 is None or df3 is None:
                    print(f"passing {header_id}")
                else:
                    print(f"working on {header_id}")
                    self.wim_header_calcs(df, df2, df3)
                    insert_string = self.wim_header_upsert(header_id)
                    with config.ENGINE.connect() as conn:
                        print(f"upserting: {header_id}")
                        conn.execute(insert_string)
                        print("COMPLETE")

    def main(self):
        """
        It takes a dataframe, splits it into two dataframes, then pushes the first dataframe to a
        database table, and the second dataframe to a number of other database tables
        """
        try:
            if self.data_df is not None and self.header_id is not None:
                pass
            else:
                data, sub_data = self.type_10()
                data = data[data.columns.intersection(self.t10_cols)]
                main.push_to_db(data, config.TYPE_10_TBL_NAME)

                self.sub_data = sub_data.replace(r"^\s*$", np.NaN, regex=True)
                self.sub_data = sub_data.drop("index", axis=1, errors="ignore")
                self.wx_data = sub_data.loc[
                    sub_data["sub_data_type_code"].str.lower().str[0] == "w"
                ]
                self.sx_data = sub_data.loc[
                    sub_data["sub_data_type_code"].str.lower().str[0] == "s"
                ]
                self.gx_data = sub_data.loc[
                    sub_data["sub_data_type_code"].str.lower().str[0] == "g"
                ]
                self.vx_data = sub_data.loc[
                    sub_data["sub_data_type_code"].str.lower().str[0] == "v"
                ]
                self.tx_data = sub_data.loc[
                    sub_data["sub_data_type_code"].str.lower().str[0] == "t"
                ]
                self.ax_data = sub_data.loc[
                    sub_data["sub_data_type_code"].str.lower().str[0] == "a"
                ]
                self.cx_data = sub_data.loc[
                    sub_data["sub_data_type_code"].str.lower().str[0] == "c"
                ]

                if self.wx_data.empty:
                    pass
                else:
                    self.wx_data.rename(
                        columns={
                            "value": "wheel_mass",
                            "number": "wheel_mass_number",
                            "id": "type10_id",
                        },
                        inplace=True,
                    )
                    main.push_to_db(self.wx_data, config.WX_TABLE)

                if self.ax_data.empty:
                    pass
                else:
                    main.push_to_db(self.ax_data, config.AX_TABLE)

                if self.gx_data.empty:
                    pass
                else:
                    main.push_to_db(self.gx_data, config.GX_TABLE)

                if self.sx_data.empty:
                    pass
                else:
                    self.sx_data.rename(
                        columns={
                            "value": "axle_spacing_cm",
                            "number": "axle_spacing_number",
                            "id": "type10_id",
                        },
                        inplace=True,
                    )
                    self.sx_data = self.sx_data.drop(
                        [
                            "offset_sensor_detection_code",
                            "mass_measurement_resolution_kg",
                        ],
                        axis=1,
                    )
                    main.push_to_db(self.sx_data, config.SX_TABLE)

                if self.tx_data.empty:
                    pass
                else:
                    self.tx_data.rename(
                        columns={
                            "value": "tyre_code",
                            "number": "tyre_number",
                            "id": "type10_id",
                        },
                        inplace=True,
                    )
                    self.tx_data = self.tx_data.drop(
                        [
                            "offset_sensor_detection_code",
                            "mass_measurement_resolution_kg",
                        ],
                        axis=1,
                    )
                    main.push_to_db(self.tx_data, config.TX_TABLE)

                if self.cx_data.empty:
                    pass
                else:
                    main.push_to_db(self.cx_data, config.CX_TABLE)

                if self.vx_data.empty:
                    pass
                else:
                    self.vx_data.rename(
                        columns={
                            "value": "group_axle_count",
                            "offset_sensor_detection_code": "vehicle_registration_number",
                            "number": "group_axle_number",
                            "id": "type10_id",
                        },
                        inplace=True,
                    )
                    self.vx_data = self.vx_data.drop(
                        ["mass_measurement_resolution_kg"], axis=1
                    )
                    main.push_to_db(self.vx_data, config.VX_TABLE)
        except Exception:
            traceback.print_exc()


if __name__ == "__main__":
    WIM = Wim(None, None, None, None, None)

    header_ids = WIM.header_ids
    print(header_ids)
    WIM.update_existing(header_ids)

    ## below is fot testing
    # SELECT_TYPE10_QRY, AXLE_SPACING_SELECT_QRY, WHEEL_MASS_SELECT_QRY = WIM.wim_header_upsert_func1(
    #     'bba9b8bf-9db6-4970-95d3-80f72393af99')
    # df, df2, df3 = WIM.wim_header_upsert_func2(
    #     SELECT_TYPE10_QRY, AXLE_SPACING_SELECT_QRY, WHEEL_MASS_SELECT_QRY)
    # WIM.wim_header_calcs(df, df2, df3)
    # upsert = WIM.wim_header_upsert('bba9b8bf-9db6-4970-95d3-80f72393af99')
    # print(upsert)

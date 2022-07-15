import csv
import os
import traceback
import numpy as np
import tqdm
import pandas as pd
import multiprocessing as mp
import gc
from psycopg2.errors import UniqueViolation

import rsa_data_summary as rd
import rsa_data_wim as wim
import rsa_headers as rh
import config
import queries as q
import tools

#### MAIN EXECUTABLE
def main(files: str):
    partition_table_columns = list(pd.read_sql_query("SELECT * from trafc.electronic_count_data_partitioned limit 1",config.ENGINE).columns)
    electronic_count_data_type_21_columns = list(pd.read_sql_query("SELECT * from trafc.electronic_count_data_type_21 limit 1",config.ENGINE).columns)
    electronic_count_data_type_30_columns = list(pd.read_sql_query("SELECT * from trafc.electronic_count_data_type_30 limit 1",config.ENGINE).columns)
    electronic_count_data_type_60_columns = list(pd.read_sql_query("SELECT * from trafc.electronic_count_data_type_60 limit 1",config.ENGINE).columns)
    electronic_count_data_type_70_columns = list(pd.read_sql_query("SELECT * from trafc.electronic_count_data_type_70 limit 1",config.ENGINE).columns)
    header_columns = list(pd.read_sql_query("SELECT * from trafc.electronic_count_header limit 1",config.ENGINE).columns)

    try:
        df = tools.to_df(files)
        DATA = rd.Data(df)

        H = rh.Headers(df, file)
        header = H.header
        lanes = H.lanes
        header["document_url"] = str(files)

        data = DATA.dtype21

        d2 = data.copy()
        if d2 is None:
            pass
        else:
            d2.rename(columns={'duration_min':'duration_of_summary',
                'speedbin0':'number_of_vehicles_in_speedbin_0',
                'speedbin1':'number_of_vehicles_in_speedbin_1', 
                'speedbin2':'number_of_vehicles_in_speedbin_2', 
                'speedbin3':'number_of_vehicles_in_speedbin_3', 
                'speedbin4':'number_of_vehicles_in_speedbin_4',
                'speedbin5':'number_of_vehicles_in_speedbin_5', 
                'speedbin6':'number_of_vehicles_in_speedbin_6', 
                'speedbin7':'number_of_vehicles_in_speedbin_7', 
                'speedbin8':'number_of_vehicles_in_speedbin_8' ,
                'speedbin9':'number_of_vehicles_in_speedbin_9',
                'speedbin10':'number_of_vehicles_in_speedbin_10',
                'short_heavy_vehicles':'number_of_short_heavy_vehicles',
                'medium_heavy_vehicles':'number_of_medium_heavy_vehicles', 
                'long_heavy_vehicles':'number_of_long_heavy_vehicles',
                'rear_to_rear_headway_shorter_than_2_seconds':'number_of_rear_to_rear_headway_shorter_than_2_seconds'
                }, inplace=True)
            d2 = d2[d2.columns.intersection(electronic_count_data_type_21_columns)]
            try:
                d2.to_sql("electronic_count_data_type_21",
                        con=config.ENGINE,
                        schema="trafc",
                        if_exists="append",
                        index=False,
                        method=tools.psql_insert_copy,
                    )
            except UniqueViolation:
                pass

        d2 = DATA.dtype70
        if d2 is None:
            pass
        else:
            data = rd.merge_summary_dataframes(d2, data)
            d2 = d2[d2.columns.intersection(electronic_count_data_type_70_columns)]
            try:
                d2.to_sql("electronic_count_data_type_70",
                    con=config.ENGINE,
                    schema="trafc",
                    if_exists="append",
                    index=False,
                    method=tools.psql_insert_copy,
                )
            except UniqueViolation:
                pass

        d2 = DATA.dtype30
        if d2 is None:
            pass
        else:
            data = rd.merge_summary_dataframes(d2, data)

        data = data[data.columns.intersection(partition_table_columns)]
        if data is None:
            pass
        else:
            try:
                data = data.T.drop_duplicates().T
                data.to_sql("electronic_count_data_partitioned",
                        con=config.ENGINE,
                        schema="trafc",
                        if_exists="append",
                        index=False,
                        method=tools.psql_insert_copy,
                    )
            except UniqueViolation:
                pass

        WIM = wim.WimData(df)
        d2, sub_data_df = WIM.dtype10
        if d2 is None:
            pass
        else:
            tools.push_to_db(d2,
            "electronic_count_data_type_10",
            ["site_id", "start_datetime", "assigned_lane_number"],
            )

        sub_data_df = sub_data_df.replace(r'^\s*$', np.NaN, regex=True)
        sub_data_df = sub_data_df.drop("index", axis=1) 
        wx_data = sub_data_df.loc[sub_data_df['sub_data_type_code'].str.lower().str[0] == 'w']
        sx_data = sub_data_df.loc[sub_data_df['sub_data_type_code'].str.lower().str[0] == 's']
        gx_data = sub_data_df.loc[sub_data_df['sub_data_type_code'].str.lower().str[0] == 'g']
        vx_data = sub_data_df.loc[sub_data_df['sub_data_type_code'].str.lower().str[0] == 'v']
        tx_data = sub_data_df.loc[sub_data_df['sub_data_type_code'].str.lower().str[0] == 't']
        ax_data = sub_data_df.loc[sub_data_df['sub_data_type_code'].str.lower().str[0] == 'a']
        cx_data = sub_data_df.loc[sub_data_df['sub_data_type_code'].str.lower().str[0] == 'c']

        if wx_data.empty:
            pass
        else:
            wx_data.rename(columns = {"value":"wheel_mass", "number":"wheel_mass_number", "id":"type10_id"}, inplace=True)
            wx_data.to_sql(
                "traffic_e_type10_wheel_mass",
                con=config.ENGINE,
                schema="trafc",
                if_exists="append",
                index=False,
                method=tools.postgres_upsert,
            )

        if ax_data.empty:
            pass
        else:
            ax_data.rename(columns = {"value":"axle_mass", "number":"axle_mass_number", "id":"type10_id"}, inplace=True)
            ax_data.to_sql(
                "traffic_e_type10_axle_mass",
                con=config.ENGINE,
                schema="trafc",
                if_exists="append",
                index=False,
                method=tools.postgres_upsert,
            )

        if gx_data.empty:
            pass
        else:
            gx_data.rename(columns = {"value":"axle_group_mass", "number":"axle_group_mass_number", "id":"type10_id"}, inplace=True)
            gx_data.to_sql(
                "traffic_e_type10_axle_group_mass",
                con=config.ENGINE,
                schema="trafc",
                if_exists="append",
                index=False,
                method=tools.postgres_upsert,
            )

        if sx_data.empty:
            pass
        else:
            sx_data.rename(columns = {"value":"axle_spacing_cm", "number":"axle_spacing_number", "id":"type10_id"}, inplace=True)
            sx_data = sx_data.drop(["offset_sensor_detection_code","mass_measurement_resolution_kg"], axis=1)
            sx_data.to_sql(
                "traffic_e_type10_axle_spacing",
                con=config.ENGINE,
                schema="trafc",
                if_exists="append",
                index=False,
                method=tools.postgres_upsert,
            )

        if tx_data.empty:
            pass
        else:
            tx_data.rename(columns = {"value":"tyre_code", "number":"tyre_number", "id":"type10_id"}, inplace=True)
            tx_data = tx_data.drop(["offset_sensor_detection_code","mass_measurement_resolution_kg"], axis=1)
            tx_data.to_sql(
                "traffic_e_type10_tyre",
                con=config.ENGINE,
                schema="trafc",
                if_exists="append",
                index=False,
                method=tools.postgres_upsert,
            )

        if cx_data.empty:
            pass
        else:
            cx_data.rename(columns = {"value":"group_axle_count", "number":"group_axle_number", "id":"type10_id"}, inplace=True)
            cx_data = cx_data.drop(["offset_sensor_detection_code","mass_measurement_resolution_kg"], axis=1)
            cx_data.to_sql(
                "traffic_e_type10_axle_group_configuration",
                con=config.ENGINE,
                schema="trafc",
                if_exists="append",
                index=False,
                method=tools.postgres_upsert,
            )

        if vx_data.empty:
            pass
        else:
            vx_data.rename(columns = {"value":"group_axle_count", "offset_sensor_detection_code":"vehicle_registration_number" ,"number":"group_axle_number", "id":"type10_id"}, inplace=True)
            vx_data = vx_data.drop(["mass_measurement_resolution_kg"], axis=1)
            vx_data.to_sql(
                "traffic_e_type10_identification_data_images",
                con=config.ENGINE,
                schema="trafc",
                if_exists="append",
                index=False,
                method=tools.postgres_upsert,
            )

        if header is None:
            pass
        else:
            try:
                header = rd.Data.header_calcs(header, data, 21)
                header = rd.Data.header_calcs(header, data, 30)
                header = rd.Data.header_calcs(header, data, 70)

                header = header[header.columns.intersection(header_columns)]
                try:
                    header.to_sql("electronic_count_header",
                            con=config.ENGINE,
                            schema="trafc",
                            if_exists="append",
                            index=False,
                            method=tools.psql_insert_copy,
                        )
                except UniqueViolation:
                    pass
            except AttributeError:
                pass

        with open(
            os.path.expanduser(config.FILES_COMPLETE),
            "a",
            newline="",
        ) as f:
            write = csv.writer(f)
            write.writerows([[files]])

    except Exception as e:
        print(e)
        traceback.print_exc()
        with open(
            os.path.expanduser(config.PROBLEM_FILES),
            "a",
            newline="",
        ) as f:
            write = csv.writer(f)
            write.writerows([[files]])
        pass

    gc.collect()

def main_type10(files: str):
    try:
        df = tools.to_df(files)
        H = rh.Headers(df)
        H = rh.Headers(df)
        DATA = rd.Data.dtype10(df)

        header = H.header
        header["document_url"] = str(files)

        # data = DATA.dtype10
        data = tools.data_join(data, header)
        data.drop("station_name", axis=1, inplace=True)

        data = data.rename(columns=(lambda x: x[:-2] if '_x' in x else x))
        # header = header.rename(columns=(lambda x: x[:-2] if '_x' in x else x))

        data = data[data.columns.intersection(config.TYPE10_DATA_COLUMN_NAMES)]
        # header = header[header.columns.intersection(config.TYPE10_HEADER_COLUMN_NAMES)]

        tools.push_to_partitioned_table(
            data,
            "electronic_count_data_type_10",
            ["site_id", "start_datetime", "lane_number"],
        )

        # push_to_db(
        #     header,
        #     "electronic_count_header_type_10",
        #     ["site_id", "start_datetime", "end_datetime"],
        # )

        # data.to_csv(r"C:\Users\MB2705851\Desktop\Temp\rsa_traffic_counts\data.csv", index=False, mode='a')
        # header.to_csv(r"C:\Users\MB2705851\Desktop\Temp\rsa_traffic_counts\header.csv", index=False, mode='a')

        with open(
            os.path.expanduser(config.FILES_COMPLETE),
            "a",
            newline="",
        ) as f:
            write = csv.writer(f)
            write.writerows([[files]])

    except Exception as e:
        print(e)
        traceback.print_exc()
        with open(
            os.path.expanduser(config.PROBLEM_FILES),
            "a",
            newline="",
        ) as f:
            write = csv.writer(f)
            write.writerows([[files]])
        pass

    gc.collect()

def main_to_csv(files: str):
    try:
        df = to_df(files)
        DATA = rd.Data(df)

        header = DATA.header
        header["document_url"] = str(files)

        data = DATA.dtype21
        data = tools.data_join(data, header)
        data.drop("station_name", axis=1, inplace=True)

        d2 = DATA.dtype30
        if d2 is None:
            pass
        else:
            d2 = tools.data_join(d2, header)
            data = data.merge(
                d2, how="outer", on=["site_id", "start_datetime", "lane_number"]
            )

        d2 = DATA.dtype70
        if d2 is None:
            pass
        else:
            data = tools.data_join(d2, header)
            data.drop("station_name", axis=1, inplace=True)
            data["start_datetime"] = data["start_datetime"].astype("datetime64[ns]")
            d2["start_datetime"] = d2["start_datetime"].astype("datetime64[ns]")
            data = data.merge(
                d2, how="outer", on=["site_id", "start_datetime", "lane_number"]
            )

        d2, sub_data_df = DATA.dtype10
        if d2 is None:
            pass
        else:
            tools.push_to_db(d2,
            "electronic_count_data_type_10",
            ["site_id", "start_datetime", "assigned_lane_number"],
            )

        sub_data_df = sub_data_df.replace(r'^\s*$', np.NaN, regex=True)
        sub_data_df = sub_data_df.drop("index", axis=1) 
        wx_data = sub_data_df.loc[sub_data_df['sub_data_type_code'].str.lower().str[0] == 'w']
        sx_data = sub_data_df.loc[sub_data_df['sub_data_type_code'].str.lower().str[0] == 's']
        gx_data = sub_data_df.loc[sub_data_df['sub_data_type_code'].str.lower().str[0] == 'g']
        vx_data = sub_data_df.loc[sub_data_df['sub_data_type_code'].str.lower().str[0] == 'v']
        tx_data = sub_data_df.loc[sub_data_df['sub_data_type_code'].str.lower().str[0] == 't']
        ax_data = sub_data_df.loc[sub_data_df['sub_data_type_code'].str.lower().str[0] == 'a']
        cx_data = sub_data_df.loc[sub_data_df['sub_data_type_code'].str.lower().str[0] == 'c']

        if wx_data.empty:
            pass
        else:
            wx_data.rename(columns = {"value":"wheel_mass", "number":"wheel_mass_number", "id":"type10_id"}, inplace=True)
            wx_data.to_csv(
                "traffic_e_type10_wheel_mass",
                con=config.ENGINE,
                schema="trafc",
                if_exists="append",
                index=False,
                method=tools.postgres_upsert,
            )

        if ax_data.empty:
            pass
        else:
            ax_data.rename(columns = {"value":"axle_mass", "number":"axle_mass_number", "id":"type10_id"}, inplace=True)
            ax_data.to_sql(
                "traffic_e_type10_axle_mass",
                con=config.ENGINE,
                schema="trafc",
                if_exists="append",
                index=False,
                method=tools.postgres_upsert,
            )

        if gx_data.empty:
            pass
        else:
            gx_data.rename(columns = {"value":"axle_group_mass", "number":"axle_group_mass_number", "id":"type10_id"}, inplace=True)
            gx_data.to_sql(
                "traffic_e_type10_axle_group_mass",
                con=config.ENGINE,
                schema="trafc",
                if_exists="append",
                index=False,
                method=tools.postgres_upsert,
            )

        if sx_data.empty:
            pass
        else:
            sx_data.rename(columns = {"value":"axle_spacing_cm", "number":"axle_spacing_number", "id":"type10_id"}, inplace=True)
            sx_data = sx_data.drop(["offset_sensor_detection_code","mass_measurement_resolution_kg"], axis=1)
            sx_data.to_sql(
                "traffic_e_type10_axle_spacing",
                con=config.ENGINE,
                schema="trafc",
                if_exists="append",
                index=False,
                method=tools.postgres_upsert,
            )

        if tx_data.empty:
            pass
        else:
            tx_data.rename(columns = {"value":"tyre_code", "number":"tyre_number", "id":"type10_id"}, inplace=True)
            tx_data = tx_data.drop(["offset_sensor_detection_code","mass_measurement_resolution_kg"], axis=1)
            tx_data.to_sql(
                "traffic_e_type10_tyre",
                con=config.ENGINE,
                schema="trafc",
                if_exists="append",
                index=False,
                method=tools.postgres_upsert,
            )

        if cx_data.empty:
            pass
        else:
            cx_data.rename(columns = {"value":"group_axle_count", "number":"group_axle_number", "id":"type10_id"}, inplace=True)
            cx_data = cx_data.drop(["offset_sensor_detection_code","mass_measurement_resolution_kg"], axis=1)
            cx_data.to_sql(
                "traffic_e_type10_axle_group_configuration",
                con=config.ENGINE,
                schema="trafc",
                if_exists="append",
                index=False,
                method=tools.postgres_upsert,
            )

        if vx_data.empty:
            pass
        else:
            vx_data.rename(columns = {"value":"group_axle_count", "offset_sensor_detection_code":"vehicle_registration_number" ,"number":"group_axle_number", "id":"type10_id"}, inplace=True)
            vx_data = vx_data.drop(["mass_measurement_resolution_kg"], axis=1)
            vx_data.to_sql(
                "traffic_e_type10_identification_data_images",
                con=config.ENGINE,
                schema="trafc",
                if_exists="append",
                index=False,
                method=tools.postgres_upsert,
            )

        d2 = DATA.dtype60
        if d2 is None:
            pass
        else:
            data = tools.data_join(d2, header)
            data.drop("station_name", axis=1, inplace=True)
            data = data.merge(
                d2, how="outer", on=["site_id", "start_datetime", "lane_number"]
            )

        data = data.rename(columns=(lambda x: x[:-2] if '_x' in x else x))
        header = header.rename(columns=(lambda x: x[:-2] if '_x' in x else x))

        header = rh.header_calcs(header, data, 21)
        header = rh.header_calcs(header, data, 30)
        header = rh.header_calcs(header, data, 70)
        header = rh.header_calcs(header, data, 60)

        data = data[data.columns.intersection(config.DATA_COLUMN_NAMES)]
        header = header[header.columns.intersection(config.HEADER_COLUMN_NAMES)]


        data.to_csv(r"C:\Users\MB2705851\Desktop\Temp\rsa_traffic_counts\data.csv", index=False, mode='a')
        header.to_csv(r"C:\Users\MB2705851\Desktop\Temp\rsa_traffic_counts\header.csv", index=False, mode='a')

        with open(
            os.path.expanduser(config.FILES_COMPLETE),
            "a",
            newline="",
        ) as f:
            write = csv.writer(f)
            write.writerows([[files]])

    except Exception as e:
        print(e)
        traceback.print_exc()
        with open(
            os.path.expanduser(config.PROBLEM_FILES),
            "a",
            newline="",
        ) as f:
            write = csv.writer(f)
            write.writerows([[files]])
        pass

    gc.collect()


##################################################################################################################################
##################################################################################################################################


if __name__ == "__main__":

    filesToDo = r"C:\Users\MB2705851\Desktop\Temp\rsa_traffic_counts\SMEC RSA Files_GP PRM Sites_Dec21toFeb22"
    if tools.s_zip(filesToDo) == False:
        filesToDo = tools.getfiles(filesToDo)
    else:
        raise SystemExit

    if not os.path.exists(os.path.expanduser(config.FILES_COMPLETE)):
        with open(
            os.path.expanduser(config.FILES_COMPLETE),
            "w",
        ) as f:
            pass
    fileComplete = os.path.expanduser(config.FILES_COMPLETE)
    try:
        fileComplete = pd.read_csv(fileComplete, header=None)
        fileComplete = fileComplete[0].tolist()
    except Exception:
        fileComplete = []

    files = [i for i in filesToDo if i not in fileComplete]

    if not os.path.exists(os.path.expanduser(config.PATH)):
        os.makedirs(os.path.expanduser(config.PATH))

    if not os.path.exists(os.path.expanduser(config.PATH)):
        with open(
            os.path.expanduser(config.PROBLEM_FILES),
            "w",
        ) as f:
            pass

    #################################################################

    ########### THE BELOW WORKS SO DON'T MESS WITH IT ###########
    # with mp.Pool(int(mp.cpu_count()/2)) as p:
    # 	with tqdm.tqdm(total=len(files)-1) as pbar:
    # 		for i, _ in enumerate(p.imap_unordered(main, files)):
    # 			pbar.update()

    ########### ATTEMPT AT GETTING A SINGLE PROGRESS BAR ###########
    # pool = mp.Pool(int(mp.cpu_count()))
    # for _ in tqdm.tqdm(pool.imap_unordered(main, files), total=len(files)):
    #     pass
    # pool.close()
    # pool.join()

########### FOR TYPE 10's ONLY ###########
    pool = mp.Pool(int(mp.cpu_count()))
    for _ in tqdm.tqdm(pool.imap_unordered(main, files), total=len(files)):
        pass
    pool.close()
    pool.join()

    ########### FOR TESTING ###########
    # for file in files:
    # 	print('BUSY WITH : ',file)
    # 	main(file)

    print("COMPLETE")

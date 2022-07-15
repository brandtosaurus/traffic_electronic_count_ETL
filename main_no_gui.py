import csv
import os
import tqdm
import traceback
import pandas as pd
import numpy as np
import multiprocessing as mp
from psycopg2.errors import UniqueViolation

import rsa_data_summary as rd
import rsa_data_wim as wim
import rsa_headers as rh
import config
import queries as q
import tools

partition_table_columns = list(pd.read_sql_query("SELECT * from trafc.electronic_count_data_partitioned limit 1",config.ENGINE).columns)
electronic_count_data_type_21_columns = list(pd.read_sql_query("SELECT * from trafc.electronic_count_data_type_21 limit 1",config.ENGINE).columns)
electronic_count_data_type_30_columns = list(pd.read_sql_query("SELECT * from trafc.electronic_count_data_type_30 limit 1",config.ENGINE).columns)
electronic_count_data_type_60_columns = list(pd.read_sql_query("SELECT * from trafc.electronic_count_data_type_60 limit 1",config.ENGINE).columns)
electronic_count_data_type_70_columns = list(pd.read_sql_query("SELECT * from trafc.electronic_count_data_type_70 limit 1",config.ENGINE).columns)
header_columns = list(pd.read_sql_query("SELECT * from trafc.electronic_count_header limit 1",config.ENGINE).columns)
electronic_count_data_type_10_columns = list(pd.read_sql_query("SELECT * from trafc.electronic_count_data_type_10 limit 1",config.ENGINE).columns)

#### MAIN EXECUTABLE
def main(file: str):
    df = tools.to_df(file)
    H = rh.Headers(df, file)
    header = H.header
    lanes = H.lanes

    D = rd.Data(df, header)
    data = D.dtype21

    if header is None:
        pass
    else:
        try:
            header = rd.Data.header_calcs(header, data, 21)
            header = rd.Data.header_calcs(header, data, 70)
            header = rd.Data.header_calcs(header, data, 30)
            header = rd.Data.header_calcs(header, data, 60)
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
    
    if data is None:
        d2 = data
    else:
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

    d2 = D.dtype70
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

    d2 = D.dtype30
    if d2 is None:
        pass
    else:
        data = rd.merge_summary_dataframes(d2, data)

    if data is None:
        main_type10(df, file)
    else:
        try:
            data = data[data.columns.intersection(partition_table_columns)]
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

    print('DONE WITH '+file)
    with open(
        os.path.expanduser(config.FILES_COMPLETE),
        "a",
        newline="",
    ) as f:
        write = csv.writer(f)
        write.writerows([[file]])

def main_type10(df: pd.DataFrame, file: str):
    WIM = wim.WimData(df)
    df, sub_data_df = WIM.dtype10

    if df is None:
        pass
    else:
        try:
            data = data[data.columns.intersection(electronic_count_data_type_10_columns)]
            try:
                tools.push_to_db(df,
                "electronic_count_data_type_10",
                ["site_id", "start_datetime", "assigned_lane_number"],
                )
            except UniqueViolation:
                pass

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
                try:
                    wx_data.to_sql(
                        "traffic_e_type10_wheel_mass",
                        con=config.ENGINE,
                        schema="trafc",
                        if_exists="append",
                        index=False,
                        method=tools.psql_insert_copy,
                    )
                except UniqueViolation:
                    pass

            if ax_data.empty:
                pass
            else:
                ax_data.rename(columns = {"value":"axle_mass", "number":"axle_mass_number", "id":"type10_id"}, inplace=True)
                try:
                    ax_data.to_sql(
                        "traffic_e_type10_axle_mass",
                        con=config.ENGINE,
                        schema="trafc",
                        if_exists="append",
                        index=False,
                        method=tools.psql_insert_copy,
                    )
                except UniqueViolation:
                    pass

            if gx_data.empty:
                pass
            else:
                gx_data.rename(columns = {"value":"axle_group_mass", "number":"axle_group_mass_number", "id":"type10_id"}, inplace=True)
                try:
                    gx_data.to_sql(
                        "traffic_e_type10_axle_group_mass",
                        con=config.ENGINE,
                        schema="trafc",
                        if_exists="append",
                        index=False,
                        method=tools.psql_insert_copy,
                    )
                except UniqueViolation:
                    pass

            if sx_data.empty:
                pass
            else:
                sx_data.rename(columns = {"value":"axle_spacing_cm", "number":"axle_spacing_number", "id":"type10_id"}, inplace=True)
                sx_data = sx_data.drop(["offset_sensor_detection_code","mass_measurement_resolution_kg"], axis=1)
                try:
                    sx_data.to_sql(
                        "traffic_e_type10_axle_spacing",
                        con=config.ENGINE,
                        schema="trafc",
                        if_exists="append",
                        index=False,
                        method=tools.psql_insert_copy,
                    )
                except UniqueViolation:
                    pass

            if tx_data.empty:
                pass
            else:
                tx_data.rename(columns = {"value":"tyre_code", "number":"tyre_number", "id":"type10_id"}, inplace=True)
                tx_data = tx_data.drop(["offset_sensor_detection_code","mass_measurement_resolution_kg"], axis=1)
                try:
                    tx_data.to_sql(
                        "traffic_e_type10_tyre",
                        con=config.ENGINE,
                        schema="trafc",
                        if_exists="append",
                        index=False,
                        method=tools.psql_insert_copy,
                    )
                except UniqueViolation:
                    pass

            if cx_data.empty:
                pass
            else:
                cx_data.rename(columns = {"value":"group_axle_count", "number":"group_axle_number", "id":"type10_id"}, inplace=True)
                cx_data = cx_data.drop(["offset_sensor_detection_code","mass_measurement_resolution_kg"], axis=1)
                try:
                    cx_data.to_sql(
                        "traffic_e_type10_axle_group_configuration",
                        con=config.ENGINE,
                        schema="trafc",
                        if_exists="append",
                        index=False,
                        method=tools.psql_insert_copy,
                    )
                except UniqueViolation:
                    pass

            if vx_data.empty:
                pass
            else:
                vx_data.rename(columns = {"value":"group_axle_count", "offset_sensor_detection_code":"vehicle_registration_number" ,"number":"group_axle_number", "id":"type10_id"}, inplace=True)
                vx_data = vx_data.drop(["mass_measurement_resolution_kg"], axis=1)
                try:
                    vx_data.to_sql(
                        "traffic_e_type10_identification_data_images",
                        con=config.ENGINE,
                        schema="trafc",
                        if_exists="append",
                        index=False,
                        method=tools.psql_insert_copy,
                    )
                except UniqueViolation:
                    pass
            
            with open(
                os.path.expanduser(config.FILES_COMPLETE),
                "a",
                newline="",
            ) as f:
                write = csv.writer(f)
                write.writerows([[file]])

            gc.collect()
        
        except Exception as e:
            print(e)
            traceback.print_exc()
            with open(
                os.path.expanduser(config.PROBLEM_FILES),
                "a",
                newline="",
            ) as f:
                write = csv.writer(f)
                write.writerows([[file]])
            pass

    



##################################################################################################################################
##################################################################################################################################


if __name__ == "__main__":

    filesToDo = config.PATH

    if tools.is_zip(filesToDo) == False:
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

    ########### MIULTIPROCESSING ###########
    pool = mp.Pool(int(mp.cpu_count()))
    for _ in tqdm.tqdm(pool.imap_unordered(main, files), total=len(files)):
        pass
    pool.close()
    pool.join()

    print("COMPLETE")

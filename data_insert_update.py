import config
from sqlalchemy.exc import SQLAlchemyError, SQLError
from typing import List
import csv
import os
import tools
import pandas as pd
import multiprocessing as mp
import tqdm

def main(files):
    df = tools.to_df(files)
    dfh = tools.get_head(df)
    header = tools.headers(dfh)
    data = tools.dtype21(df)
    data = tools.data_join(data, header)
    data.drop("station_name", axis=1, inplace=True)
    header = tools.header_calcs(header, data, 21)
    for index, row in data.iterrows():
        insert_qry = tools.data_insert_type21(row)
        update_qry = tools.data_update_type21(row)
        
        try:
            with config.ENGINE.connect() as conn:
                conn.execute(insert_qry)
        except:
            with config.ENGINE.connect() as conn:
                conn.execute(update_qry)

    for index, row in header.iterrows():
        try:
            with config.ENGINE.connect() as conn:
                conn.execute(tools.header_insert(row))
        except:
            with config.ENGINE.connect() as conn:
                conn.execute(tools.header_update(row))
    
    data = tools.dtype30
    data = tools.data_join(data, header)
    for index, row in data.iterrows():
        insert_qry = tools.data_insert_type30(row)
        update_qry = tools.data_update_type30(row)
        try:
            with config.ENGINE.connect() as conn:
                conn.execute(insert_qry)
        except SQLError:
            with config.ENGINE.connect() as conn:
                conn.execute(update_qry)
    
    data, sub_data_df = tools.dtype10
    data = tools.data_join(data, header)

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

    if ax_data.empty:
        pass
    else:
        ax_data.rename(columns = {"value":"axle_mass", "number":"axle_mass_number", "id":"type10_id"}, inplace=True)

    if gx_data.empty:
        pass
    else:
        gx_data.rename(columns = {"value":"axle_group_mass", "number":"axle_group_mass_number", "id":"type10_id"}, inplace=True)

    if sx_data.empty:
        pass
    else:
        sx_data.rename(columns = {"value":"axle_spacing_cm", "number":"axle_spacing_number", "id":"type10_id"}, inplace=True)
        sx_data = sx_data.drop(["offset_sensor_detection_code","mass_measurement_resolution_kg"], axis=1)

    if tx_data.empty:
        pass
    else:
        tx_data.rename(columns = {"value":"tyre_code", "number":"tyre_number", "id":"type10_id"}, inplace=True)
        tx_data = tx_data.drop(["offset_sensor_detection_code","mass_measurement_resolution_kg"], axis=1)

    if cx_data.empty:
        pass
    else:
        cx_data.rename(columns = {"value":"group_axle_count", "number":"group_axle_number", "id":"type10_id"}, inplace=True)
        cx_data = cx_data.drop(["offset_sensor_detection_code","mass_measurement_resolution_kg"], axis=1)

    if vx_data.empty:
        pass
    else:
        vx_data.rename(columns = {"value":"group_axle_count", "offset_sensor_detection_code":"vehicle_registration_number" ,"number":"group_axle_number", "id":"type10_id"}, inplace=True)
        vx_data = vx_data.drop(["mass_measurement_resolution_kg"], axis=1)

    for index, row in data.iterrows():
        insert_qry = tools.data_insert_type30(row)
        update_qry = tools.data_update_type30(row)
        try:
            with config.ENGINE.connect() as conn:
                conn.execute(insert_qry)
        except SQLError:
            with config.ENGINE.connect() as conn:
                conn.execute(update_qry)

    with open(
    os.path.expanduser(config.FILES_COMPLETE),
    "a",
    newline="",) as f:
        write = csv.writer(f)
        write.writerows([[file]])

if __name__=="__main__":
    
    filesToDo = config.DATA_FOLDER
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

    pool = mp.Pool(int(mp.cpu_count()))
    for _ in tqdm.tqdm(pool.imap_unordered(main, files), total=len(files)):
        pass
    pool.close()
    pool.join()

    print("COMPLETE")
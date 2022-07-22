import csv
import os
import tqdm
import traceback
import pandas as pd
import numpy as np
import multiprocessing as mp
from psycopg2.errors import UniqueViolation, NotNullViolation

import rsa_data_summary as rds
import rsa_data_wim as wim
import rsa_headers as rh
import config
import queries as q
import tools

import time
from functools import wraps
import psycopg2
import psycopg2.extensions

from typing import List

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
pd.options.mode.chained_assignment = None

pt_cols = list(pd.read_sql_query(
    q.SELECT_PARTITION_TABLE_LIMIT1, config.ENGINE).columns)
t21_cols = list(pd.read_sql_query(
    q.SELECT_ELECTRONIC_COUNT_DATA_TYPE_21_LIMIT1, config.ENGINE).columns)
t30_cols = list(pd.read_sql_query(
    q.SELECT_ELECTRONIC_COUNT_DATA_TYPE_30_LIMIT1, config.ENGINE).columns)
t60_cols = list(pd.read_sql_query(
    q.SELECT_ELECTRONIC_COUNT_DATA_TYPE_60_LIMIT1, config.ENGINE).columns)
t70_cols = list(pd.read_sql_query(
    q.SELECT_ELECTRONIC_COUNT_DATA_TYPE_70_LIMIT1, config.ENGINE).columns)
t10_cols = list(pd.read_sql_query(
    q.SELECT_ELECTRONIC_COUNT_DATA_TYPE_10_LIMIT1, config.ENGINE).columns)
h_cols = list(pd.read_sql_query(q.SELECT_HEADER_LIMIT1, config.ENGINE).columns)
lane_cols = list(pd.read_sql_query(
    q.SELECT_LANES_LIMIT1, config.ENGINE).columns)

# def retry(fn):
#     @wraps(fn)
#     def wrapper(*args, **kw):
#         cls = args[0]
#         for x in range(cls._reconnectTries):
#             print(x, cls._reconnectTries)
#             try:
#                 return fn(*args, **kw)
#             except (psycopg2.InterfaceError, psycopg2.OperationalError) as e:
#                 print ("\nDatabase Connection [InterfaceError or OperationalError]")
#                 print ("Idle for %s seconds" % (cls._reconnectIdle))
#                 time.sleep(cls._reconnectIdle)
#                 cls._connect()
#     return wrapper

# MAIN EXECUTABLE


def main(file: str):
    print("--- Busy With "+file)
    df = tools.to_df(file)
    if df is None:
        pass
    else:
        header = rh.header(rh.get_head(df), file)

        lanes = rh.lanes(header)
        lanes = d2[lanes.columns.intersection(lane_cols)]
        try:
            lanes.to_sql("traffic_lane_configuration",
                         con=config.ENGINE,
                         schema="trafc",
                         if_exists="append",
                         index=False,
                         method=tools.psql_insert_copy,
                         )
        except (UniqueViolation, NotNullViolation):
            pass

        data = rds.join(header, rds.type_21(df))

        if data is None:
            data = rds.join(header, rds.type_30(df))
            if data is None:
                data = pd.DataFrame(columns=pt_cols)
            else:
                d2 = data.copy()
                data = rds.merge_summary_dataframes(d2, data)
        else:
            data = data.reset_index(drop=True)
            d2 = data.copy()
            if d2 is None:
                pass
            else:
                d2.rename(
                    columns=config.ELECTRONIC_COUNT_DATA_TYPE21_NAME_CHANGE, inplace=True)
                d2 = d2[d2.columns.intersection(t21_cols)]
                try:
                    d2.to_sql("electronic_count_data_type_21",
                              con=config.ENGINE,
                              schema="trafc",
                              if_exists="append",
                              index=False,
                              method=tools.psql_insert_copy,
                              )
                except (UniqueViolation, NotNullViolation):
                    pass

        d2 = rds.type_70
        if d2 is None:
            pass
        else:
            data = rds.merge_summary_dataframes(d2, data)
            d2 = d2[d2.columns.intersection(t70_cols)]
            try:
                d2.to_sql("electronic_count_data_type_70",
                          con=config.ENGINE,
                          schema="trafc",
                          if_exists="append",
                          index=False,
                          method=tools.psql_insert_copy,
                          )
            except (UniqueViolation, NotNullViolation):
                pass

        d2 = rds.electronic_count_data_type_30
        if d2 is None:
            pass
        else:
            try:
                d2.to_sql("electronic_count_data_type_30",
                          con=config.ENGINE,
                          schema="trafc",
                          if_exists="append",
                          index=False,
                          method=tools.psql_insert_copy,
                          )
            except (UniqueViolation, NotNullViolation):
                pass

        if data is None:
            type_10_data = main_type10(df, file, header)
        else:
            type_10_data = None
            try:
                data = data.loc[:, ~data.columns.duplicated()]
                data = data[data.columns.intersection(pt_cols)]
                data.loc[:, data.columns.difference(config.DATA_IMPORTANT_COLUMNS)] = data.loc[:, data.columns.difference(
                    config.DATA_IMPORTANT_COLUMNS)].convert_dtypes()
                try:
                    data.to_sql("electronic_count_data_partitioned",
                                con=config.ENGINE,
                                schema="trafc",
                                if_exists="append",
                                index=False,
                                method=tools.psql_insert_copy,
                                )
                except (UniqueViolation, NotNullViolation):
                    pass
            except:
                pass

        if header is None:
            pass
        else:
            try:
                data.reset_index(drop=True, inplace=True)
                data = data.loc[:, ~data.columns.duplicated()]
                header = header.loc[:, ~header.columns.duplicated()]
                # header = header.rename(columns=(lambda x: x[:-2] if '_x' in x else x))
                header = rds.header_calcs(header, data, 21)
                # header =  rds.header_calcs(header, data, 70)tes
                header = rds.header_calcs(header, data, 30)
                # header =  rds.header_calcs(header, data, 60)
                if type_10_data is None:
                    pass
                else:
                    header = rds.header_calcs(header, type_10_data, 10)
                header = header[header.columns.intersection(h_cols)]
                try:
                    header.to_sql("electronic_count_header_test",
                                  con=config.ENGINE,
                                  schema="trafc",
                                  if_exists="append",
                                  index=False,
                                  method=tools.psql_insert_copy,
                                  )
                except (UniqueViolation, NotNullViolation):
                    pass
                try:
                    header.to_sql("electronic_count_header",
                                  con=config.ENGINE,
                                  schema="trafc",
                                  if_exists="append",
                                  index=False,
                                  method=tools.psql_insert_copy,
                                  )
                except (UniqueViolation, NotNullViolation):
                    try:
                        for index, row in header.iterrows():
                            qry = f"""update trafc.electronic_count_header set 
                            document_url = '{file}'
                            where site_id = '{row['site_id']}'
                            and start_datetime = '{row['start_datetime']}'
                            and end_datetime = '{row['end_datetime']}'
                            and document_url is null;
                            """
                            with config.ENGINE.connect() as conn:
                                conn.execute(qry)
                    except Exception as e:
                        print(e)
                        raise Exception(
                            "error with updating document_url in main file")

            except AttributeError as exc:
                raise Exception("Issue with HEADER "+exc) from exc

        print('DONE WITH '+file)
        with open(
            os.path.expanduser(config.FILES_COMPLETE),
            "a",
            newline="",
        ) as f:
            write = csv.writer(f)
            write.writerows([[file]])


# TYPE 10 (INDIVIDUAL COUNT) SUB-FUNCTION (REFERENCED IN MAIN FUNCTION)
def main_type10(df: pd.DataFrame, file: str, header: pd.DataFrame):
    df, sub_data_df = wim.type_10(df)
    if df is None:
        pass
    else:
        try:
            df = df[df.columns.intersection(t10_cols)]
            try:
                tools.push_to_db(df,
                                 "electronic_count_data_type_10",
                                 ["site_id", "start_datetime",
                                     "assigned_lane_number"],
                                 )
            except (UniqueViolation, NotNullViolation):
                pass

            sub_data_df = sub_data_df.replace(r'^\s*$', np.NaN, regex=True)
            sub_data_df = sub_data_df.drop("index", axis=1)
            wx_data = sub_data_df.loc[sub_data_df['sub_data_type_code'].str.lower(
            ).str[0] == 'w']
            sx_data = sub_data_df.loc[sub_data_df['sub_data_type_code'].str.lower(
            ).str[0] == 's']
            gx_data = sub_data_df.loc[sub_data_df['sub_data_type_code'].str.lower(
            ).str[0] == 'g']
            vx_data = sub_data_df.loc[sub_data_df['sub_data_type_code'].str.lower(
            ).str[0] == 'v']
            tx_data = sub_data_df.loc[sub_data_df['sub_data_type_code'].str.lower(
            ).str[0] == 't']
            ax_data = sub_data_df.loc[sub_data_df['sub_data_type_code'].str.lower(
            ).str[0] == 'a']
            cx_data = sub_data_df.loc[sub_data_df['sub_data_type_code'].str.lower(
            ).str[0] == 'c']

            if wx_data.empty:
                pass
            else:
                wx_data.rename(columns={
                               "value": "wheel_mass", "number": "wheel_mass_number", "id": "type10_id"}, inplace=True)
                try:
                    wx_data.to_sql(
                        "traffic_e_type10_wheel_mass",
                        con=config.ENGINE,
                        schema="trafc",
                        if_exists="append",
                        index=False,
                        method=tools.psql_insert_copy,
                    )
                except (UniqueViolation, NotNullViolation):
                    pass

            if ax_data.empty:
                pass
            else:
                ax_data.rename(columns={
                               "value": "axle_mass", "number": "axle_mass_number", "id": "type10_id"}, inplace=True)
                try:
                    ax_data.to_sql(
                        "traffic_e_type10_axle_mass",
                        con=config.ENGINE,
                        schema="trafc",
                        if_exists="append",
                        index=False,
                        method=tools.psql_insert_copy,
                    )
                except (UniqueViolation, NotNullViolation):
                    pass

            if gx_data.empty:
                pass
            else:
                gx_data.rename(columns={
                               "value": "axle_group_mass", "number": "axle_group_mass_number", "id": "type10_id"}, inplace=True)
                try:
                    gx_data.to_sql(
                        "traffic_e_type10_axle_group_mass",
                        con=config.ENGINE,
                        schema="trafc",
                        if_exists="append",
                        index=False,
                        method=tools.psql_insert_copy,
                    )
                except (UniqueViolation, NotNullViolation):
                    pass

            if sx_data.empty:
                pass
            else:
                sx_data.rename(columns={
                               "value": "axle_spacing_cm", "number": "axle_spacing_number", "id": "type10_id"}, inplace=True)
                sx_data = sx_data.drop(
                    ["offset_sensor_detection_code", "mass_measurement_resolution_kg"], axis=1)
                try:
                    sx_data.to_sql(
                        "traffic_e_type10_axle_spacing",
                        con=config.ENGINE,
                        schema="trafc",
                        if_exists="append",
                        index=False,
                        method=tools.psql_insert_copy,
                    )
                except (UniqueViolation, NotNullViolation):
                    pass

            if tx_data.empty:
                pass
            else:
                tx_data.rename(columns={
                               "value": "tyre_code", "number": "tyre_number", "id": "type10_id"}, inplace=True)
                tx_data = tx_data.drop(
                    ["offset_sensor_detection_code", "mass_measurement_resolution_kg"], axis=1)
                try:
                    tx_data.to_sql(
                        "traffic_e_type10_tyre",
                        con=config.ENGINE,
                        schema="trafc",
                        if_exists="append",
                        index=False,
                        method=tools.psql_insert_copy,
                    )
                except (UniqueViolation, NotNullViolation):
                    pass

            if cx_data.empty:
                pass
            else:
                cx_data.rename(columns={
                               "value": "group_axle_count", "number": "group_axle_number", "id": "type10_id"}, inplace=True)
                cx_data = cx_data.drop(
                    ["offset_sensor_detection_code", "mass_measurement_resolution_kg"], axis=1)
                try:
                    cx_data.to_sql(
                        "traffic_e_type10_axle_group_configuration",
                        con=config.ENGINE,
                        schema="trafc",
                        if_exists="append",
                        index=False,
                        method=tools.psql_insert_copy,
                    )
                except (UniqueViolation, NotNullViolation):
                    pass

            if vx_data.empty:
                pass
            else:
                vx_data.rename(columns={"value": "group_axle_count", "offset_sensor_detection_code": "vehicle_registration_number",
                               "number": "group_axle_number", "id": "type10_id"}, inplace=True)
                vx_data = vx_data.drop(
                    ["mass_measurement_resolution_kg"], axis=1)
                try:
                    vx_data.to_sql(
                        "traffic_e_type10_identification_data_images",
                        con=config.ENGINE,
                        schema="trafc",
                        if_exists="append",
                        index=False,
                        method=tools.psql_insert_copy,
                    )
                except (UniqueViolation, NotNullViolation):
                    pass

            with open(os.path.expanduser(config.FILES_COMPLETE), "a", newline="",) as f:
                write = csv.writer(f)
                write.writerows([[file]])

            return df

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


def get_files(files: str) -> List:
    if tools.is_zip(files) == False:
        files = tools.getfiles(files)
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

    files = [i for i in files if i not in fileComplete]

    if not os.path.exists(os.path.expanduser(config.PATH)):
        os.makedirs(os.path.expanduser(config.PATH))

    if not os.path.exists(os.path.expanduser(config.PATH)):
        with open(
            os.path.expanduser(config.PROBLEM_FILES),
            "w",
        ) as f:
            pass
    return files

# Multiprocessing


def run_multiprocess(path):
    files = get_files(path)
    pool = mp.Pool(int(mp.cpu_count()))
    for _ in tqdm.tqdm(pool.imap_unordered(main, files), total=len(files)):
        pass
    pool.close()
    pool.join()


def run_individually(path):
    files = get_files(path)
    for file in files:
        main(file)

##################################################################################################################################
##################################################################################################################################


if __name__ == "__main__":

    PATH = r"C:\PQ410"
    # PATH = r"C:\FTP\Syntell"

    # try:
    #     run_multiprocess(PATH)
    # except Exception as e:
    #     print(e)
    #     time.sleep(10)
    #     run_multiprocess(PATH)

    run_individually(PATH)

    print("COMPLETE")

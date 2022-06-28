import csv
import os
import zipfile
import traceback
from numpy import empty
import numpy as np
import tqdm
import pandas as pd
from pandasql import sqldf
import multiprocessing as mp

from sqlalchemy.dialects.postgresql import insert

from io import StringIO

import rsa_data as rd
import rsa_headers as rh
import config
import queries as q
import tools

from tkinter import filedialog
from tkinter import *
import gc
from typing import List



def is_zip(path: str) -> bool:
    for filename in path:
        return zipfile.is_zipfile(filename)

def getfiles(path: str) -> List[str]:
    print("COLLECTING FILES......")
    src = []
    for root, dirs, files in os.walk(path):
        for name in files:
            if (
                name.endswith(".RSA")
                or name.endswith(".rsa")
                or name.endswith(".rsv")
                or name.endswith(".RSV")
            ):
                p = os.path.join(root, name)
                src.append(p)
    src = list(set(src))
    return src

def to_df(file: str) -> pd.DataFrame:
    df = pd.read_csv(file, header=None)
    df = df[0].str.split("\s+|,\s+|,", expand=True)
    df = pd.DataFrame(df)
    return df


def join(header: pd.DataFrame, data: pd.DataFrame) -> pd.DataFrame:
    if data.empty:
        df = pd.DataFrame()
    else:
        q = """
		SELECT header.header_id, header.station_name, data.*
		FROM header
		LEFT JOIN data ON data.start_datetime WHERE data.start_datetime >= header.start_datetime AND data.end_datetime <= header.end_datetime;
		"""
        q2 = """UPDATE data set header_id = (SELECT header_id from header WHERE data.start_datetime >= header.start_datetime AND data.counttime_end <= header.enddate)"""
        pysqldf = lambda q: sqldf(q, globals())
        df = sqldf(q, locals())
        df = pd.DataFrame(df)
    return df


def data_join(data: pd.DataFrame, header: pd.DataFrame) -> pd.DataFrame:
    if data is None:
        pass
    elif data.empty:
        pass
    else:
        data = pd.DataFrame(data)
        data = join(header, data)
    return data


def save_to_temp_csv(df: pd.DataFrame, label: str):
    if not os.path.exists(os.path.expanduser(config.OUTPUT_FILE + label + ".csv")):
        df.to_csv(
            os.path.expanduser(config.OUTPUT_FILE + label + ".csv"),
            mode="a",
            header=True,
            index=False,
        )
    else:
        df.to_csv(
            os.path.expanduser(config.OUTPUT_FILE + label + ".csv"),
            mode="a",
            header=False,
            index=False,
        )


def dropDuplicatesDoSomePostProcesingAndSaveCsv(csv_label: str):
    df = pd.DataFrame()
    for i in pd.read_csv(
        os.path.expanduser(
            f"~\Desktop\Temp\rsa_traffic_counts\TEMP_E_COUNT_{csv_label}.csv"
        ),
        chunksize=50000,
        error_bad_lines=False,
        low_memory=False,
    ):
        df = pd.concat([df, i])
        df = df.drop_duplicates()
    cols = df.columns.tolist()
    df2 = pd.DataFrame(columns=cols)
    df2.to_csv(
        os.path.expanduser(
            f"~\Desktop\Temp\rsa_traffic_counts\TEMP_E_COUNT_{csv_label}.csv"
        ),
        header=True,
    )
    df.to_csv(
        os.path.expanduser(
            f"~\Desktop\Temp\rsa_traffic_counts\TEMP_E_COUNT_{csv_label}.csv"
        ),
        mode="a",
        header=False,
    )


def push_to_partitioned_table(df, table, subset) -> None:
    yr = data['year'].unique()[0]
    print(yr)
    try:
        df.to_sql(
            table+'_'+str(yr),
            con=config.ENGINE,
            schema="trafc",
            if_exists="append",
            index=False,
            method=psql_insert_copy,
        )
    except Exception:
        df = df.drop_duplicates(subset=subset)
        df.to_sql(
            table+'_'+yr,
            con=config.ENGINE,
            schema="trafc",
            if_exists="append",
            index=False,
            method=psql_insert_copy,
        )

def push_to_db(df, table, subset) -> None:
    try:
        df.to_sql(
            table,
            con=config.ENGINE,
            schema="trafc",
            if_exists="append",
            index=False,
            method=psql_insert_copy,
        )
    except Exception:
        df = df.drop_duplicates(subset=subset)
        df.to_sql(
            table,
            con=config.ENGINE,
            schema="trafc",
            if_exists="append",
            index=False,
            method=psql_insert_copy,
        )

def postgres_upsert(table, conn, keys, data_iter):
    data = [dict(zip(keys, row)) for row in data_iter]

    insert_statement = insert(table.table).values(data)
    upsert_statement = insert_statement.on_conflict_do_update(
        constraint=f"{table.table.name}_un",
        set_={c.key: c for c in insert_statement.excluded},
    )
    conn.execute(upsert_statement)


def psql_insert_copy(table, conn, keys, data_iter):
    """
    Execute SQL statement inserting data

    Parameters
    ----------
    table : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list of str
        Column names
    data_iter : Iterable that iterates the values to be inserted
    """
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ", ".join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = "{}.{}".format(table.schema, table.name)
        else:
            table_name = table.name

        sql = "COPY {} ({}) FROM STDIN WITH CSV".format(table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)

def create_database_tables():
    conn=config.CONN
    cur = conn.cursor()
    cur.execute(q.CREATE_AXLE_GROU_MASS_GX)
    cur.execute(q.CREATE_AXLE_GROUP_CONFIG_TABLE_CX)
    cur.execute(q.CREATE_AXLE_MASS_TABLE_AX)
    cur.execute(q.CREATE_AXLE_GROUP_MASS_TABLE_GX)
    cur.execute(q.CREATE_AXLE_SPACING_TABLE_SX)
    cur.execute(q.CREATE_IMAGES_TABLE_VX)
    cur.execute(q.CREATE_TYRE_TABLE_TX)
    cur.execute(q.CREATE_WHEEL_MASS_TABLE_WX)
    cur.commit()
    cur.close()


##################################################################################################################################
##################################################################################################################################
#### MAIN EXECUTABLE
def main(files: str):
    try:
        df = to_df(files)

        header = tools.get_head(df)
        header = tools.headers(header)
        header["document_url"] = str(files)

        data = pd.DataFrame(colums=config.DATA_COLUMN_NAMES)

        d2 = tools.dtype21(df)
        if d2 is None:
            pass
        else:
            data = tools.data_join(d2, header)
            data.drop("station_name", axis=1, inplace=True)
            

        d2 = tools.dtype30
        if d2 is None:
            pass
        else:
            d2 = data_join(d2, header)

        d2 = tools.dtype70
        if d2 is None:
            pass
        else:
            data = data_join(d2, header)
            data.drop("station_name", axis=1, inplace=True)
            data["start_datetime"] = data["start_datetime"].astype("datetime64[ns]")
            d2["start_datetime"] = d2["start_datetime"].astype("datetime64[ns]")

        d2, sub_data_df = tools.dtype10
        if d2 is None:
            pass
        else:
            push_to_db(d2,
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
                method=postgres_upsert,
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
                method=postgres_upsert,
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
                method=postgres_upsert,
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
                method=postgres_upsert,
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
                method=postgres_upsert,
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
                method=postgres_upsert,
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
                method=postgres_upsert,
            )

        d2 = tools.dtype60
        if d2 is None:
            pass
        else:
            data = data_join(d2, header)
            data.drop("station_name", axis=1, inplace=True)
            data = data.merge(
                d2, how="outer", on=["site_id", "start_datetime", "lane_number"]
            )

        data = data.rename(columns=(lambda x: x[:-2] if '_x' in x else x))
        header = header.rename(columns=(lambda x: x[:-2] if '_x' in x else x))

        header = header_calcs(header, data, 21)
        header = header_calcs(header, data, 30)
        header = header_calcs(header, data, 70)
        header = header_calcs(header, data, 60)

        data = data[data.columns.intersection(config.DATA_COLUMN_NAMES)]
        header = header[header.columns.intersection(config.HEADER_COLUMN_NAMES)]

        push_to_partitioned_table(
            data,
            "electronic_count_data_partitioned",
            ["site_id", "start_datetime", "lane_number"],
        )

        push_to_db(
            header,
            "electronic_count_header",
            ["site_id", "start_datetime", "end_datetime"],
        )

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

def main_type10(files: str):
    try:
        df = to_df(files)
        H = rh.Headers(df)
        H = rh.Headers(df)
        DATA = rd.Data.dtype10(df)

        header = H.header
        header["document_url"] = str(files)

        # data = DATA.dtype10
        data = data_join(data, header)
        data.drop("station_name", axis=1, inplace=True)

        data = data.rename(columns=(lambda x: x[:-2] if '_x' in x else x))
        # header = header.rename(columns=(lambda x: x[:-2] if '_x' in x else x))

        data = data[data.columns.intersection(config.TYPE10_DATA_COLUMN_NAMES)]
        # header = header[header.columns.intersection(config.TYPE10_HEADER_COLUMN_NAMES)]

        push_to_partitioned_table(
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

##################################################################################################################################
##################################################################################################################################


if __name__ == "__main__":

    filesToDo = r"C:\Users\MB2705851\Desktop\Temp\rsa_traffic_counts\SMEC RSA Files_GP PRM Sites_Dec21toFeb22"
    if is_zip(filesToDo) == False:
        filesToDo = getfiles(filesToDo)
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

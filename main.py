import csv
import os
import zipfile
import traceback
import tqdm
import pandas as pd
from pandasql import sqldf
import multiprocessing as mp

from io import StringIO

import rsa_data as rd
import rsa_headers as rh
import config

from tkinter import filedialog
from tkinter import *
import gc
from typing import List


def gui():
    root = Tk().withdraw()
    f = filedialog.askdirectory()
    # root.destroy()
    return str(f)


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
    df = pd.read_csv(file, header=None, sep="\n")
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


def push_to_db(df: pd.DataFrame, table: str, subset: List[str]) -> None:
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


##################################################################################################################################
##################################################################################################################################
#### MAIN EXECUTABLE
def main(files: str):
    global finalheaders, finaldata
    try:
        df = to_df(files)
        H = rh.Headers(df)
        DATA = rd.Data(df)

        header = H.header
        header["document_url"] = str(files)

        data = DATA.dtype21
        data = main.data_join(data, header)
        data.drop("station_name", axis=1, inplace=True)

        d2 = DATA.dtype30
        if d2 is None:
            pass
        else:
            d2 = main.data_join(d2, header)
            data = data.merge(
                d2, how="outer", on=["site_id", "start_datetime", "lane_number"]
            )

        d2 = DATA.dtype70
        if d2 is None:
            pass
        else:
            data = main.data_join(d2, header)
            data.drop("station_name", axis=1, inplace=True)
            data["start_datetime"] = data["start_datetime"].astype("datetime64[ns]")
            d2["start_datetime"] = d2["start_datetime"].astype("datetime64[ns]")
            data = data.merge(
                d2, how="outer", on=["site_id", "start_datetime", "lane_number"]
            )

        d2 = DATA.dtype10
        if d2 is None:
            pass
        else:
            data = main.data_join(d2, header)
            data.drop("station_name", axis=1, inplace=True)
            data = data.merge(
                d2, how="outer", on=["site_id", "start_datetime", "lane_number"]
            )

        d2 = DATA.dtype60
        if d2 is None:
            pass
        else:
            data = main.data_join(d2, header)
            data.drop("station_name", axis=1, inplace=True)
            data = data.merge(
                d2, how="outer", on=["site_id", "start_datetime", "lane_number"]
            )

        push_to_db(
            data,
            "electronic_count_data_partitioned",
            ["site_id", "start_datetime", "lane_number"],
        )

        push_to_db(
            header,
            "electronic_count_header",
            ["site_id", "start_datetime", "end_datetime"],
        )

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

    filesToDo = gui()
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
        fileComplete = pd.read_csv(fileComplete, header=None, sep="\n")
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
    pool = mp.Pool(int(mp.cpu_count()))
    for _ in tqdm.tqdm(pool.imap_unordered(main, files), total=len(files)):
        pass
    pool.close()
    pool.join()

    ########### FOR TESTING ###########
    # for file in files:
    # 	print('BUSY WITH : ',file)
    # 	main(file)

    ########################################################################################################################################################
    ########################################################################################################################################################
    #### POST PROCESSING

    # with config.ENGINE.connect() as con:
    #     con.execute(
    #         "VACUUM (FULL, VERBOSE, ANALYZE) trafc.electronic_count_data_partitioned;"
    #     )
    #     con.execute("VACUUM (FULL, VERBOSE, ANALYZE) trafc.electronic_count_header;")

    print("COMPLETE")

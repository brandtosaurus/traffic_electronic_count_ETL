import sys
import numpy as np
import csv
import os
import traceback
from pandas.core.frame import DataFrame
import psycopg2
from sqlalchemy import create_engine
import multiprocessing as mp
from concurrent.futures import ThreadPoolExecutor
import tqdm
from datetime import datetime, timedelta, date
import pandas as pd
from pandasql import sqldf
import rsa_data as rd
import rsa_headers as rh
from tkinter import filedialog
from tkinter import *
import json
import gc
from typing import List


#### DB CONNECTION
engine = create_engine("postgresql://postgres:Lin3@r1in3!431@linearline.dedicated.co.za:5432/gauteng")
# engine = create_engine(
#     "postgresql://postgres:$admin@localhost:5432/asset_management_master"
# )


def gui():
    root = Tk().withdraw()
    f = filedialog.askdirectory()
    root.destroy()
    return str(f)


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


def join(header, data):
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


def save_to_temp_csv(df: pd.DataFrame, label: str):
    if not os.path.exists(
        os.path.expanduser(
            "~" + r"\Desktop\Temp\rsa_traffic_counts\TEMP_E_COUNT_" + label + ".csv"
        )
    ):
        df.to_csv(
            os.path.expanduser(
                "~" + r"\Desktop\Temp\rsa_traffic_counts\TEMP_E_COUNT_" + label + ".csv"
            ),
            mode="a",
            header=True,
            index=False,
        )
    else:
        df.to_csv(
            os.path.expanduser(
                "~" + r"\Desktop\Temp\rsa_traffic_counts\TEMP_E_COUNT_" + label + ".csv"
            ),
            mode="a",
            header=False,
            index=False,
        )


def dropDuplicatesDoSomePostProcesingAndSaveCsv(csv_label: str):
    df = pd.DataFrame()
    for i in pd.read_csv(
        os.path.expanduser(
            "~" + r"\Desktop\Temp\rsa_traffic_counts\TEMP_E_COUNT_" + csv_label + ".csv"
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
            "~" + r"\Desktop\Temp\rsa_traffic_counts\TEMP_E_COUNT_" + csv_label + ".csv"
        ),
        header=True,
    )
    df.to_csv(
        os.path.expanduser(
            "~" + r"\Desktop\Temp\rsa_traffic_counts\TEMP_E_COUNT_" + csv_label + ".csv"
        ),
        mode="a",
        header=False,
    )


##################################################################################################################################
##################################################################################################################################
##################################################################################################################################
##################################################################################################################################
#### MAIN EXECUTABLE
def main(files: str):
    global finalheaders, finaldata
    try:
        df = to_df(files)

        header = rh.headers(df)
        header = pd.DataFrame(header)
        header["document_url"] = str(files)
        try:
            header.to_sql(
                "v_electronic_count_header_import",
                con=engine,
                schema="trafc",
                if_exists="append",
                index=False,
            )
        except Exception:
            header = header.drop_duplicates(
                subset=["site_id", "start_datetime", "end_datetime"]
            )
            header.to_sql(
                "v_electronic_count_header_import",
                con=engine,
                schema="trafc",
                if_exists="append",
                index=False,
            )

        data = rd.dtype21(df)
        if data is None:
            pass
        elif data.empty:
            pass
        else:
            data = pd.DataFrame(data)
            data = join(header, data)
            try:
                data.to_sql(
                    "v_electronic_count_data_import",
                    con=engine,
                    schema="trafc",
                    if_exists="append",
                    index=False,
                )
            except Exception:
                data = data.drop_duplicates(
                    subset=["site_id", "start_datetime", "lane_number"]
                )
                data.to_sql(
                    "v_electronic_count_data_import",
                    con=engine,
                    schema="trafc",
                    if_exists="append",
                    index=False,
                )

        data = rd.dtype30(df)
        if data is None:
            pass
        elif data.empty:
            pass
        else:
            data = pd.DataFrame(data)
            data = join(header, data)
            try:
                data.to_sql(
                    "v_electronic_count_data_import",
                    con=engine,
                    schema="trafc",
                    if_exists="append",
                    index=False,
                )
            except Exception:
                data = data.drop_duplicates(
                    subset=["site_id", "start_datetime", "lane_number"]
                )
                data.to_sql(
                    "v_electronic_count_data_import",
                    con=engine,
                    schema="trafc",
                    if_exists="append",
                    index=False,
                )

        data = rd.dtype70(df)
        if data is None:
            pass
        elif data.empty:
            pass
        else:
            data = pd.DataFrame(data)
            data = join(header, data)
            try:
                data.to_sql(
                    "v_electronic_count_data_import",
                    con=engine,
                    schema="trafc",
                    if_exists="append",
                    index=False,
                )
            except Exception:
                data = data.drop_duplicates(
                    subset=["site_id", "start_datetime", "lane_number"]
                )
                data.to_sql(
                    "v_electronic_count_data_import",
                    con=engine,
                    schema="trafc",
                    if_exists="append",
                    index=False,
                )

        data = rd.dtype10(df)
        if data is None:
            pass
        elif data.empty:
            pass
        else:
            data = pd.DataFrame(data)
            data = join(header, data)
            try:
                data.to_sql(
                    "v_electronic_count_data_import",
                    con=engine,
                    schema="trafc",
                    if_exists="append",
                    index=False,
                )
            except Exception:
                data = data.drop_duplicates(
                    subset=["site_id", "start_datetime", "lane_number"]
                )
                data.to_sql(
                    "v_electronic_count_data_import",
                    con=engine,
                    schema="trafc",
                    if_exists="append",
                    index=False,
                )

        ############ before uncommenting this, make sure that the appropriate changes are made to the database import table.
        # 	data = rd.dtype60(df)
        # if data is None:
        # 	pass
        # else:
        # 	data = pd.DataFrame(data)
        # 	data = join(header,data)
        # 	data.to_sql('v_electronic_count_data_import',con=engine,schema='trafc',if_exists='append',index=False)#, chunksize=5000)

        with open(
            os.path.expanduser(
                "~"
                + r"\Desktop\Temp\rsa_traffic_counts\RSA_FILES_COMPLETE_localhost.csv"
            ),
            "a",
            newline="",
        ) as f:
            write = csv.writer(f)
            write.writerows([[files]])

    except Exception as e:
        print(e)
        traceback.print_exc()
        with open(
            os.path.expanduser(
                "~" + r"\Desktop\Temp\rsa_traffic_counts\RSA_COUNT_PROBLEM_FILES.csv"
            ),
            "a",
            newline="",
        ) as f:
            write = csv.writer(f)
            write.writerows([[files]])
        pass

    gc.collect()


##################################################################################################################################
##################################################################################################################################
##################################################################################################################################
##################################################################################################################################


if __name__ == "__main__":
    # fileIncomplete = os.path.expanduser('~'+ r'\Desktop\Temp\rsa_traffic_counts\RSA_COUNT_PROBLEM_FILES - Copy.csv')
    # fileIncomplete = pd.read_csv(fileIncomplete, header=None, sep='\n')
    # files = fileIncomplete[0].tolist()

    filesToDo = gui()
    filesToDo = getfiles(filesToDo)

    if not os.path.exists(
        os.path.expanduser(
            "~" + r"\Desktop\Temp\rsa_traffic_counts\RSA_FILES_COMPLETE_localhost.csv"
        )
    ):
        with open(
            os.path.expanduser(
                "~"
                + r"\Desktop\Temp\rsa_traffic_counts\RSA_FILES_COMPLETE_localhost.csv"
            ),
            "w",
        ) as f:
            pass
    fileComplete = os.path.expanduser(
        "~" + r"\Desktop\Temp\rsa_traffic_counts\RSA_FILES_COMPLETE_localhost.csv"
    )
    try:
        fileComplete = pd.read_csv(fileComplete, header=None, sep="\n")
        fileComplete = fileComplete[0].tolist()
    except Exception:
        fileComplete = []

    files = [i for i in filesToDo if i not in fileComplete]

    if not os.path.exists(
        os.path.expanduser("~" + r"\Desktop\Temp\rsa_traffic_counts")
    ):
        os.makedirs(os.path.expanduser("~" + r"\Desktop\Temp\rsa_traffic_counts"))

    if not os.path.exists(
        os.path.expanduser("~" + r"\Desktop\Temp\rsa_traffic_counts")
    ):
        with open(
            os.path.expanduser(
                "~" + r"\Desktop\Temp\rsa_traffic_counts\RSA_COUNT_PROBLEM_FILES.csv"
            ),
            "w",
        ) as f:
            pass

    #################################################################

    ########### THE BELOW WORKS SO DONT MESS WITH IT ###########
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
    ########################################################################################################################################################
    ########################################################################################################################################################
    #### POST PROCESSING

	with engine.connect() as con:
		con.execute("VACUUM (FULL, VERBOSE, ANALYZE) trafc.electronic_count_data_partitioned;")
		con.execute("VACUUM (FULL, VERBOSE, ANALYZE) trafc.electronic_count_header;")
		
    print("COMPLETE")

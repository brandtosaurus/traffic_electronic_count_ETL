import csv
import os
import traceback
import tqdm
import pandas as pd
from pandasql import sqldf
import multiprocessing as mp

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
        )
    except Exception:
        df = df.drop_duplicates(subset=subset)
        df.to_sql(
            table,
            con=config.ENGINE,
            schema="trafc",
            if_exists="append",
            index=False,
        )


##################################################################################################################################
##################################################################################################################################
#### MAIN EXECUTABLE
def main(files: str):
    global finalheaders, finaldata
    try:
        df = to_df(files)

        header = rh.Headers(df)
        header = pd.DataFrame(header)
        header["document_url"] = str(files)
        push_to_db(
            header,
            "v_electronic_count_header_import",
            ["site_id", "start_datetime", "end_datetime"],
        )

        data = rd.Data.dtype21(df)
        data = data_join(data, header)
        push_to_db(
            data,
            "v_electronic_count_data_import",
            ["site_id", "start_datetime", "lane_number"],
        )

        data = rd.Data.dtype30(df)
        data = data_join(data, header)
        push_to_db(
            data,
            "v_electronic_count_data_import",
            ["site_id", "start_datetime", "lane_number"],
        )

        data = rd.Data.dtype70(df)
        data = data_join(data, header)
        push_to_db(
            data,
            "v_electronic_count_data_import",
            ["site_id", "start_datetime", "lane_number"],
        )

        data = rd.Data.dtype10(df)
        data = data_join(data, header)
        push_to_db(
            data,
            "v_electronic_count_data_import",
            ["site_id", "start_datetime", "lane_number"],
        )

        ############ before uncommenting this, make sure that the appropriate changes are made to the database import table.
        # 	data = rd.Data.dtype60(df)
        # if data is None:
        # 	pass
        # else:
        # 	data = pd.DataFrame(data)
        # 	data = join(header,data)
        # 	data.to_sql('v_electronic_count_data_import',con=config.ENGINE,schema='trafc',if_exists='append',index=False)#, chunksize=5000)

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
    # fileIncomplete = os.path.expanduser('~'+ r'\Desktop\Temp\rsa_traffic_counts\RSA_COUNT_PROBLEM_FILES - Copy.csv')
    # fileIncomplete = pd.read_csv(fileIncomplete, header=None, sep='\n')
    # files = fileIncomplete[0].tolist()

    filesToDo = gui()
    filesToDo = getfiles(filesToDo)

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

    with config.ENGINE.connect() as con:
        con.execute(
            "VACUUM (FULL, VERBOSE, ANALYZE) trafc.electronic_count_data_partitioned;"
        )
        con.execute("VACUUM (FULL, VERBOSE, ANALYZE) trafc.electronic_count_header;")

    print("COMPLETE")

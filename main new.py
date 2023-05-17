import os
import csv
import pandas as pd
from psycopg2.errors import UniqueViolation, NotNullViolation, ExclusionViolation
import psycopg2
import psycopg2.extensions
import uuid
import gc
from typing import List
import multiprocessing as mp
import traceback
import tqdm
import config
import docs.queries as q
import traffic_wim
import warnings

from docs.header_data.header_calcs import header_calcs
from docs.header_data.process_headers import header
from docs.body_data.Type_21 import type_21
from docs.body_data.Type_30 import type_30
from docs.body_data.Type_60 import type_60
from docs.body_data.Type_70 import type_70

from docs.wim.

warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.simplefilter(action="ignore", category=FutureWarning)
pd.options.mode.chained_assignment = None
pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", 20)

# Reading the columns from the database and storing them in a list.
pt_cols = list(
    pd.read_sql_query(q.SELECT_PARTITION_TABLE_LIMIT1, config.ENGINE).columns
)
t21_cols = list(
    pd.read_sql_query(
        q.SELECT_ELECTRONIC_COUNT_DATA_TYPE_21_LIMIT1, config.ENGINE
    ).columns
)
t30_cols = list(
    pd.read_sql_query(
        q.SELECT_ELECTRONIC_COUNT_DATA_TYPE_30_LIMIT1, config.ENGINE
    ).columns
)
t60_cols = list(
    pd.read_sql_query(
        q.SELECT_ELECTRONIC_COUNT_DATA_TYPE_60_LIMIT1, config.ENGINE
    ).columns
)
t70_cols = list(
    pd.read_sql_query(
        q.SELECT_ELECTRONIC_COUNT_DATA_TYPE_70_LIMIT1, config.ENGINE
    ).columns
)
t10_cols = list(
    pd.read_sql_query(
        q.SELECT_ELECTRONIC_COUNT_DATA_TYPE_10_LIMIT1, config.ENGINE
    ).columns
)
h_cols = list(pd.read_sql_query(q.SELECT_HEADER_LIMIT1, config.ENGINE).columns)
lane_cols = list(pd.read_sql_query(q.SELECT_LANES_LIMIT1, config.ENGINE).columns)


class Traffic:
    def __init__(self, file) -> None:
        self.file = file
        self.header_id = str(uuid.uuid4())
        self.df = to_df()
        if self.df is None:
            print(f"Empty DataFrame: {self.file}")
            self.head_df = None
            self.header_df = None
            pass
        else:
            self.head_df = get_head(self.df)
            self.site_id = head_df.loc[self.head_df[0] == "S0", 1].iat[0]
            self.site_id = site_id.replace(".rsa", "")
            self.site_id = self.site_id.upper()
            self.lanes = get_lanes(self.head_df)
            self.header_df = header(self.head_df)
            self.data_df = get_summary_data(self.df)
            self.indv_data_df = get_indv_data(self.df)
            if self.indv_data_df is None or (
                self.indv_data_df is not None and self.data_df is not None
            ):
                pass
            else:
                self.indv_data_df = self.process_datetimes(self.indv_data_df, "indv")
                self.indv_data_df = self.get_direction(self.indv_data_df, "indv")
            if self.data_df is None:
                pass
            else:
                self.data_df = self.process_datetimes(self.data_df, "summary")
                self.data_df = self.get_direction(self.data_df, "summary")
            self.t21_table = config.TYPE_21_TBL_NAME
            self.t30_table = config.TYPE_30_TBL_NAME
            self.t70_table = config.TYPE_70_TBL_NAME
            self.t60_table = config.TYPE_60_TBL_NAME


    def select_classification_scheme(self, classification_scheme):
        if int(classification_scheme) == 8:
            vc_df = pd.read_sql_query(q.SELECT_CLASSIFICAITON_SCHEME_8, config.ENGINE)
        elif int(classification_scheme) == 1:
            vc_df = pd.read_sql_query(q.SELECT_CLASSIFICAITON_SCHEME_1, config.ENGINE)
        elif int(classification_scheme) == 5:
            vc_df = pd.read_sql_query(q.SELECT_CLASSIFICAITON_SCHEME_5, config.ENGINE)
        elif int(classification_scheme) == 9:
            vc_df = pd.read_sql_query(q.SELECT_CLASSIFICAITON_SCHEME_9, config.ENGINE)
        else:
            vc_df = None
        return vc_df

def merge_summary_dataframes(
    join_this_df: pd.DataFrame, onto_this_df: pd.DataFrame
) -> pd.DataFrame:
    """
    It takes two dataframes, joins them together, and then drops any duplicate columns

    :param join_this_df: The dataframe that you want to join onto the other dataframe
    :type join_this_df: pd.DataFrame
    :param onto_this_df: The dataframe that you want to merge the other dataframe onto
    :type onto_this_df: pd.DataFrame
    :return: A dataframe with the columns from the two dataframes merged.
    """
    onto_this_df = pd.concat(
        [onto_this_df, join_this_df],
        keys=["site_id", "start_datetime", "lane_number"],
        ignore_index=False,
        axis=1,
    )
    onto_this_df = onto_this_df.droplevel(0, axis=1)
    onto_this_df = onto_this_df.loc[:, ~onto_this_df.T.duplicated(keep="first")]
    return onto_this_df


def push_to_db(df: pd.DataFrame, table_name: str):
    """
    It takes a dataframe and a table name, and pushes the dataframe to the table name

    :param df: pd.DataFrame
    :type df: pd.DataFrame
    :param table_name: the name of the table in the database
    :type table_name: str
    """
    try:
        df = df.loc[:, ~df.columns.duplicated()]
        df.to_sql(
            table_name,
            con=config.ENGINE,
            schema=config.TRAFFIC_SCHEMA,
            if_exists="append",
            index=False,
            method=tools.psql_insert_copy,
        )
        print(f"~~{table_name} pushed to db")
    except (UniqueViolation, ExclusionViolation):
        print("Data already in : " + table_name)
    except AttributeError:
        pass


def get_files(path: str) -> List:
    """
    It takes a list of files, checks if they are in a zip file, if not, it gets the files from the
    directory, if they are in a zip file, it exits.

    Then it checks if the file exists, if not, it creates it.

    Then it reads the file, if it can't read it, it creates an empty list.

    Then it checks if the files are in the list, if they are, it removes them.

    Then it checks if the directory exists, if not, it creates it.

    Then it checks if the file exists, if not, it creates it.

    Then it returns the list of files.

    :param files: str = "C:/Users/user/Desktop/files"
    :type files: str
    :return: A list of files that are not in the fileComplete list.
    """
    if not os.path.exists(os.path.expanduser(config.PATH)):
        os.makedirs(os.path.expanduser(config.PATH))

    if not os.path.exists(os.path.expanduser(config.FILES_COMPLETE)):
        with open(
            os.path.expanduser(config.FILES_COMPLETE),
            "w",
        ) as f:
            pass

    if not os.path.exists(os.path.expanduser(config.PATH)):
        with open(
            os.path.expanduser(config.FILES_FAILED),
            "w",
        ) as f:
            pass

    try:
        file_complete = pd.read_csv(config.FILES_COMPLETE, header=None)
        file_complete = file_complete[0].tolist()
    except pd.errors.EmptyDataError:
        file_complete = []

    if tools.is_zip(path) == False:
        files = tools.get_files(path)
    else:
        raise SystemExit

    files = [i for i in files if i not in file_complete]

    return files


# Multiprocessing
def run_multiprocess(path):
    """
    It takes a path, gets all the files in that path, creates a pool of processes equal to the number of
    CPU cores, and then runs the main function on each file in the path.

    :param path: The path to the folder containing the files you want to process
    """
    files = get_files(path)
    try:
        pool = mp.Pool(int(mp.cpu_count()))
        for _ in tqdm.tqdm(pool.imap_unordered(main, files), total=len(files)):
            pass
        pool.close()
        pool.join()
    except Exception as exc:
        traceback.print_exc()
        print(exc)


def run_individually(path):
    """
    It takes a path, gets all the files in that path, and then runs the main function on each file

    :param path: the path to the folder containing the files
    """
    files = get_files(path)
    print(len(files))

    for file in files:
        try:
            main(file)
        except Exception as exc:
            traceback.print_exc()
            break


def main(file: str):
    """
    It reads a file, does some stuff, and then writes the results to a database.

    :param file: str
    :type file: str
    """
    print(f"-- Busy With {file}")
    TR = Traffic(file)
    head_df = TR.head_df
    header = TR.header_df

    if header is None:
        with open(
            os.path.expanduser(config.FILES_FAILED),
            "a",
            newline="",
        ) as f:
            write = csv.writer(f)
            write.writerows([[file]])
        gc.collect()
        pass
    else:
        try:
            lanes = TR.lanes
            site_id = TR.site_id
            header_id = TR.header_id
            indv_data_df = TR.data_df

            pt_df = pd.DataFrame()

            if lanes is None:
                pass
            else:
                try:
                    lanes = lanes[lanes.columns.intersection(lane_cols)]
                    push_to_db(lanes, config.LANES_TBL_NAME)
                except Exception as exc:
                    traceback.print_exc()
                    with open(
                        os.path.expanduser(config.FILES_FAILED),
                        "a",
                        newline="",
                    ) as f:
                        write = csv.writer(f)
                        write.writerows([[file]])
                    gc.collect()

            if head_df.loc[head_df[0] == "21"].empty:
                pass
            else:
                data = TR.type_21()
                if data is None:
                    pass
                else:
                    try:
                        pt_df = data
                        pt_df = merge_summary_dataframes(data, pt_df)
                        header = TR.header_calcs(header, data, 21)
                        data.rename(
                            columns=config.ELECTRONIC_COUNT_DATA_TYPE21_NAME_CHANGE,
                            inplace=True,
                        )
                        data = data[data.columns.intersection(t21_cols)]
                        push_to_db(data, config.TYPE_21_TBL_NAME)
                    except Exception as exc:
                        traceback.print_exc()
                        with open(
                            os.path.expanduser(config.FILES_FAILED),
                            "a",
                            newline="",
                        ) as f:
                            write = csv.writer(f)
                            write.writerows([[file]])
                        gc.collect()

            if head_df.loc[head_df[0] == "30"].empty:
                pass
            else:
                try:
                    data = TR.type_30()
                    header = TR.header_calcs(header, data, 30)
                    data = data[data.columns.intersection(t30_cols)]
                    push_to_db(data, config.TYPE_30_TBL_NAME)
                except Exception as exc:
                    traceback.print_exc()
                    with open(
                        os.path.expanduser(config.FILES_FAILED),
                        "a",
                        newline="",
                    ) as f:
                        write = csv.writer(f)
                        write.writerows([[file]])
                    gc.collect()

            if head_df.loc[head_df[0] == "60"].empty:
                pass
            else:
                try:
                    data = TR.type_60()
                    header = TR.header_calcs(header, data, 60)
                    if data is None:
                        pass
                    else:
                        data = data[data.columns.intersection(t60_cols)]
                        push_to_db(data, config.TYPE_60_TBL_NAME)
                except Exception as exc:
                    traceback.print_exc()
                    with open(
                        os.path.expanduser(config.FILES_FAILED),
                        "a",
                        newline="",
                    ) as f:
                        write = csv.writer(f)
                        write.writerows([[file]])
                    gc.collect()

            if head_df.loc[head_df[0] == "70"].empty:
                pass
            else:
                try:
                    data = TR.type_70()
                    header = TR.header_calcs(header, data, 70)
                    if data is None:
                        pass
                    else:
                        data = data[data.columns.intersection(t70_cols)]
                        pt_df = merge_summary_dataframes(data, pt_df)
                        push_to_db(data, config.TYPE_70_TBL_NAME)
                except Exception as exc:
                    traceback.print_exc()
                    with open(
                        os.path.expanduser(config.FILES_FAILED),
                        "a",
                        newline="",
                    ) as f:
                        write = csv.writer(f)
                        write.writerows([[file]])
                    gc.collect()

            if head_df.loc[head_df[0] == "10"].empty:
                pass
            elif (
                indv_data_df.loc[(indv_data_df[0] == "10")]
                .reset_index(drop=True)[0]
                .empty
            ):
                pass
            else:
                try:
                    W = traffic_wim.Wim(
                        data=indv_data_df,
                        head_df=head_df,
                        header_id=header_id,
                        site_id=site_id,
                        pt_cols=pt_cols,
                    )
                    W.main()
                except Exception:
                    traceback.print_exc()
                    pass

            if header is None:
                pass
            else:
                try:
                    header = header[header.columns.intersection(h_cols)]
                    push_to_db(header, config.HEADER_TBL_NAME)
                except AttributeError as exc:
                    raise Exception("Issue with HEADER " + exc) from exc

            pt_df = pt_df.apply(pd.to_numeric, axis=1, errors="ignore")
            pt_df = pt_df[pt_df.columns.intersection(pt_cols)]
            pt_df["site_id"] = site_id
            push_to_db(pt_df, config.MAIN_TBL_NAME)

            print("DONE WITH : " + file)
            with open(
                os.path.expanduser(config.FILES_COMPLETE),
                "a",
                newline="",
            ) as f:
                write = csv.writer(f)
                write.writerows([[file]])
            gc.collect()
        except Exception as exc:
            traceback.print_exc()
            with open(
                os.path.expanduser(config.FILES_FAILED),
                "a",
                newline="",
            ) as f:
                write = csv.writer(f)
                write.writerows([[file]])
            gc.collect()


# The above code is running the main function in the multiprocessing module.
if __name__ == "__main__":
    PATH = config.PATH

    # this is for local work only - comment this out when running on the server
    PATH = r"C:\PQ410"
    # PATH = r"C:\FTP"
    # PATH = r"C:\FTP\import_results\rsa_traffic_counts\FILES_FAILED.csv"
    # testfile = r"C:\FTP\Syntell\0087_20220331.RSA"
    # t10_file = r"C:\FTP\Syntell\SMEC RSV Files_GP PRM Sites_Dec21toFeb22_individuals\0400-20220222.RSV"

    # MAIN EXECUTABLE
    run_multiprocess(PATH)

    # RUN INDIVIDUALLY IF ANY PROBLEMS WITH MULTIPROCESSING TO FIND THE ISSUE
    # run_individually(PATH)

    # TEST INDIVIDUAL FILE BELOW FOR DEBUGGING
    # main(t10_file)

    print("COMPLETE")

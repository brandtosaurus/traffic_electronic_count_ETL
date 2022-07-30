from distutils.debug import DEBUG
import os
import csv
import pandas as pd
import numpy as np
from psycopg2.errors import UniqueViolation, NotNullViolation, ExclusionViolation
import psycopg2
import psycopg2.extensions
from datetime import timedelta, date
import uuid
import gc
from typing import List
import time
import multiprocessing as mp
import traceback
import pdb
import tqdm
import config
import queries as q
import wim
import tools
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
pd.options.mode.chained_assignment = None
pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", 20)

# Reading the columns from the database and storing them in a list.
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
h_cols = list(pd.read_sql_query(
    q.SELECT_HEADER_LIMIT1, config.ENGINE).columns)
lane_cols = list(pd.read_sql_query(
    q.SELECT_LANES_LIMIT1, config.ENGINE).columns)


def retry(fn):
    @wraps(fn)
    def wrapper(*args, **kw):
        cls = args[0]
        for x in range(cls._reconnectTries):
            print(x, cls._reconnectTries)
            try:
                return fn(*args, **kw)
            except (psycopg2.InterfaceError, psycopg2.OperationalError) as e:
                print(
                    "\nDatabase Connection [InterfaceError or OperationalError]")
                print("Idle for %s seconds" % (cls._reconnectIdle))
                time.sleep(cls._reconnectIdle)
                cls._connect()
    return wrapper


class Traffic():

    def __init__(self, file) -> None:
        self.file = file
        self.header_id = str(uuid.uuid4())
        self.df = self.to_df()
        if self.df is None:
            print(f"Empty DataFrame: {self.file}")
            self.head_df = None
            self.header_df = None
            pass
        else:
            self.head_df = self.get_head(self.df)
            self.site_id = self.head_df.loc[self.head_df[0]
                                            == "S0", 1].iat[0]
            self.site_id = self.site_id.replace('.rsa', '')
            self.lanes = self.get_lanes(self.head_df)
            self.header_df = self.header(self.head_df)
            self.data_df = self.get_data(self.df)
            self.data_df = self.process_datetimes(self.data_df)
            self.data_df = self.get_direction(self.data_df)
            self.t21_table = config.TYPE_21_TBL_NAME
            self.t30_table = config.TYPE_30_TBL_NAME
            self.t70_table = config.TYPE_70_TBL_NAME
            self.t60_table = config.TYPE_60_TBL_NAME

    def to_df(self) -> pd.DataFrame:  # CSV to DataFrame
        """
        It takes a CSV file, reads it in as a DataFrame, splits the first column on a variety of
        delimiters, and returns the resulting DataFrame.
        :return: A DataFrame
        """
        try:
            df = pd.read_csv(self.file, header=None, dtype=str,
                             sep="\s+\t", engine='python')
            df = df[0].str.split("\s+|,\s+|,|;|;\s+", expand=True)
            df = df.dropna(axis=1, how='all')
            return df
        except Exception as exc:
            traceback.print_exc()
            with open(os.path.expanduser(config.FILES_FAILED), "a", newline="",) as f:
                write = csv.writer(f)
                write.writerows([[self.file]])
            gc.collect()

    def get_head(self, df) -> pd.DataFrame:
        """
        It returns a dataframe that contains all rows where the first column is either H0, S0, I0, S1,
        D0, D1, D3, L0, L1, or where the first column is either 21, 22, 70, 30, 31, 60 and the third
        column is less than 80, or where the first column is 10 and the second column is 1, 8, 5, 9, 01,
        08, 05, or 09

        :param df: the dataframe to be processed
        :return: A dataframe
        """
        try:
            df = pd.DataFrame(df.loc[
                (df[0].isin(["H0", "S0", "I0", "S1", "D0", "D1", "D3", "L0", "L1"]))
                | (
                    (df[0].isin(["21", "22", "70", "30", "31", "60"]))
                    & (df.loc[df[0].isin(["21", "22", "70", "30", "31", "60"]), 2].fillna("0").astype(int) < 80)
                )
                | (
                    (df[0].isin(["10"]))
                    & (df[1].isin(["1", "8", "5", "9", "01", "08", "05", "09"]))
                )
            ]).dropna(axis=1, how="all").reset_index(drop=True).copy()
            return df
        except KeyError as exc:
            traceback.print_exc()
            with open(os.path.expanduser(config.FILES_FAILED), "a", newline="") as f:
                write = csv.writer(f)
                write.writerows([[self.file]])
            gc.collect()

    def get_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        It filters out rows that have a value in column 0 that is not in the list ["H0", "H9", "S0",
        "I0", "S1", "D0", "D1", "D3", "L0", "L1"] and that have a value in column 1 that is in the list
        ["0", "1", "2", "3", "4"] and that have a value in column 2 that is greater than 80

        :param df: pd.DataFrame - the dataframe to be filtered
        :type df: pd.DataFrame
        :return: A dataframe
        """
        try:
            df = pd.DataFrame(df.loc[
                (~df[0].isin(["H0", "H9", "S0", "I0",
                              "S1", "D0", "D1", "D3", "L0", "L1"]))
                & ((
                    (df[0].isin(["21", "22", "70", "30", "31", "60"]))
                    & (df[1].isin(["0", "1", "2", "3", "4"]))
                    & (df.loc[df[0].isin(["21", "22", "70", "30", "31", "60"]), 2].fillna("0").astype(int) > 80)
                ) | (
                    (df[0].isin(["10"]))
                    & (~df[1].isin(["1", "8", "5", "9", "01", "08", "05", "09"]))
                    & (df.loc[df[0].isin(["10"]), 4].fillna("0").astype(int) > 80)
                    & (~df[0].isin(["H0", "H9", "S0", "I0", "S1", "D0", "D1", "D3", "L0", "L1"]))
                ))
            ]).dropna(axis=1, how="all").reset_index(drop=True).copy()
            df = df.dropna(axis=0, how="all").reset_index(drop=True)
            return df
        except TypeError as exc:
            print(f"gat_data func: check filtering and file {self.file}")
            print(exc)
        except Exception as exc:
            traceback.print_exc()
            with open(os.path.expanduser(config.FILES_FAILED), "a", newline="") as f:
                write = csv.writer(f)
                write.writerows([[self.file]])
            gc.collect()

    def get_lanes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        It takes a dataframe, checks if it's empty, if it's not empty, it checks if the first column is
        equal to "L1", if it is, it drops all columns that have null values, drops duplicates, resets
        the index, and copies the dataframe. If the dataframe is empty, it checks if the first column is
        equal to "L0", if it is, it drops all columns that have null values, drops duplicates, resets
        the index, and copies the dataframe. It then sets the max_lanes variable to the first value in
        the second column of the dataframe. It then renames the columns. If the dataframe has 5 columns,
        it renames the columns to "lane_number", "direction_code", "lane_type_code", and
        "traffic_stream_number". If the dataframe has 11 columns, it renames the columns to
        "lane_number", "direction

        :param df: pd.DataFrame
        :type df: pd.DataFrame
        :return: A dataframe with the columns:
        """
        if df.empty:
            pass
        else:
            lanes = df.loc[
                df[0] == "L1"
            ].dropna(axis=1).drop_duplicates().reset_index(drop=True).copy()
            if lanes.empty:
                lanes = df.loc[
                    df[0] == "L0"
                ].dropna(axis=1).drop_duplicates().reset_index(drop=True).copy()
                self.max_lanes = lanes[1].astype(int).at[0]
                lanes.rename(columns={
                    1: "num_assigned_lanes",
                    2: "num_physical_lanes"}, inplace=True)
            else:
                lanes = lanes.drop([17, 18, 19, 20, 21, 22, 23,
                                    24, 25], axis=1, errors='ignore')
                if lanes.shape[1] == 5:
                    lanes.rename(columns={
                        1: "lane_number",
                        2: "direction_code",
                        3: "lane_type_code",
                        4: "traffic_stream_number"
                    }, inplace=True)
                elif lanes.shape[1] == 11:
                    lanes.rename(columns={
                        1: "lane_number",
                        2: "direction_code",
                        3: "lane_type_code",
                        4: "traffic_stream_number",
                        5: "traffic_stream_lane_position",
                        6: "reverse_direction_lane_number",
                        7: "vehicle_code",
                        8: "time_code",
                        9: "length_code",
                        10: "speed_code"
                    }, inplace=True)
                elif lanes.shape[1] == 17:
                    lanes.rename(columns={
                        1: "lane_number",
                        2: "direction_code",
                        3: "lane_type_code",
                        4: "traffic_stream_number",
                        5: "traffic_stream_lane_position",
                        6: "reverse_direction_lane_number",
                        7: "vehicle_code",
                        8: "time_code",
                        9: "length_code",
                        10: "speed_code",
                        11: "occupancy_time_code",
                        12: "vehicle_following_code",
                        13: "trailer_code",
                        14: "axle_code",
                        15: "mass_code",
                        16: "tyre_type_code"
                    }, inplace=True)
                else:
                    lanes.rename(columns={
                        1: "lane_number",
                        2: "direction_code",
                        3: "lane_type_code",
                        4: "traffic_stream_number"
                    }, inplace=True)
                lanes[lanes.select_dtypes(include=['object']).columns] = lanes[
                    lanes.select_dtypes(include=['object']).columns].apply(
                        pd.to_numeric, axis=1, errors='ignore')
                lanes["site_name"] = self.site_id
                lanes['site_id'] = self.site_id
                try:
                    self.max_lanes = int(
                        lanes['lane_number'].astype(int).max())
                except ValueError:
                    self.max_lanes = int(
                        lanes['lane_number'].drop_duplicates().count())
                return lanes

    # !most likely to go wrong
    def process_datetimes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        It takes a dataframe, checks if the first column is equal to 10, if it is, it does one thing, if
        it isn't, it does another

        :param df: the dataframe
        :type df: pd.DataFrame
        :return: A dataframe with the columns renamed and the datetime columns added.
        """
        if df.loc[df[0] == "10"].empty:
            duration_min_col = 4
            date_col = 2
            time_col = 3
            add_day = 1
            date_col_name = "end_date"
            time_col_name = "end_time"
            typ = "sum"
            max_time_str_len = int(df[time_col].str.len().max())
        else:
            date_col = 4
            time_col = 5
            add_day = 0
            date_col_name = "departure_date"
            time_col_name = "departure_time"
            typ = "indv"
            max_time_str_len = int(df[time_col].str.len().max())
        date_fmt = "%Y%m%d"  # YYYYMMDD
        date_format = "%Y-%m-%d"  # YYYY-MM-DD
        time_fmt = "%H%M%S%f"  # (includes 00000000, 0000000, 000000)
        time_format = "%H:%M:%S.%f"  # 00:00:00.00

        df[time_col] = df[time_col].str.pad(
            width=max_time_str_len, side='right', fillchar="0")
        df[time_col].loc[df[time_col].str[:2] == '24'] = (
            '0').zfill(max_time_str_len)

        df[date_col] = df[date_col].apply(lambda x: str(date.today())[:2]+x if int(x[:2]) <= 50 and len(
            x) == 6 else (str(int(str(date.today())[:2])-1)+x if int(x[:2]) > 50 and len(x) == 6 else x))

        if typ == "indv":
            df[date_col] = pd.to_datetime(
                df[date_col], format=date_fmt).dt.strftime(date_format)
        else:
            df[date_col] = df[date_col].apply(lambda x: pd.to_datetime(
                x, format=date_fmt).date() + timedelta(days=add_day)
                if x[time_col] in ['0'.zfill(max_time_str_len), '24'.ljust(max_time_str_len, '0')]
                else pd.to_datetime(x, format=date_fmt).date())

        df['end_datetime'] = pd.to_datetime((df[date_col].astype(str)+df[time_col].astype(str)),
                                            format=date_format+time_fmt)

        if df.loc[df[0] == "10"].empty:
            df['start_datetime'] = df.apply(lambda x: pd.to_datetime(str(x[date_col])+str(
                x[time_col]), format=date_format+time_fmt) - timedelta(minutes=int(x[duration_min_col])), axis=1)
        else:
            df['start_datetime'] = pd.to_datetime(
                df[date_col]+df[time_col], format=date_format+time_fmt)

        df[time_col] = pd.to_datetime(
            df[time_col], format=time_fmt).dt.strftime(time_format)

        df.rename(columns={
            date_col: date_col_name,
            time_col: time_col_name
        }, inplace=True)
        return df

    def get_direction(self, df):
        """
        It takes a dataframe and returns a dataframe with the columns 'direction', 'direction_code', and
        'compass_heading' added

        :param df: the dataframe that contains the data
        :return: A dataframe with the following columns:
        """
        try:
            if df.loc[df[0] == "10"].empty:
                lane_col = 5
            else:
                lane_col = 6
            try:
                dir_1 = self.lanes["direction_code"].astype(int).min()
                dir_2 = self.lanes["direction_code"].astype(int).max()
            except (TypeError, ValueError):
                dir_1 = 0
                dir_2 = 4

            direction_1 = 'P'
            direction_2 = 'N'

            if dir_1 == dir_2:
                dir_2 = dir_1
                direction_2 = direction_1
            else:
                pass

            df['direction'] = df[lane_col].astype(int)
            df['compass_heading'] = df[lane_col].astype(int)
            df['direction_code'] = df[lane_col].astype(int)
            df['direction'].loc[df[lane_col].astype(int).isin(
                list(self.lanes['lane_number'].astype(
                    int).loc[self.lanes['direction_code'].astype(int) == dir_1])
            )] = direction_1
            df['direction'].loc[df[lane_col].astype(int).isin(
                list(self.lanes['lane_number'].astype(
                    int).loc[self.lanes['direction_code'].astype(int) == dir_2])
            )] = direction_2
            df['compass_heading'].loc[df[lane_col].astype(int).isin(
                list(self.lanes['lane_number'].astype(
                    int).loc[self.lanes['direction_code'].astype(int) == dir_1])
            )] = str(dir_1)
            df['compass_heading'].loc[df[lane_col].astype(int).isin(
                list(self.lanes['lane_number'].astype(
                    int).loc[self.lanes['direction_code'].astype(int) == dir_2])
            )] = str(dir_2)
            df['direction_code'].loc[df[lane_col].astype(int).isin(
                list(self.lanes['lane_number'].astype(
                    int).loc[self.lanes['direction_code'].astype(int) == dir_2])
            )] = str(dir_2)
            df['direction_code'].loc[df[lane_col].astype(int).isin(
                list(self.lanes['lane_number'].astype(
                    int).loc[self.lanes['direction_code'].astype(int) == dir_1])
            )] = str(dir_1)

            return df
        except (KeyError, TypeError):
            df['direction'] = df.apply(
                lambda x: 'P' if int(x[lane_col]) <= int(self.max_lanes)/2 else 'N', axis=1)
            df['direction_code'] = df.apply(
                lambda x: 0 if int(x[lane_col]) <= int(self.max_lanes)/2 else 4, axis=1)
            df['compass_heading'] = df.apply(
                lambda x: 0 if int(x[lane_col]) <= int(self.max_lanes)/2 else 4, axis=1)
            return df

    def header(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        It takes a dataframe, finds the header information, and returns a dataframe with the header
        information

        :param df: pd.DataFrame
        :type df: pd.DataFrame
        :return: A dataframe
        """
        df = df.reset_index(drop=True)
        st_nd = df.loc[df[0].isin(["D1", "D3"]), 0:4].reset_index(
            drop=True).copy()

        # standard column identifiers
        st_dt_col = 1
        end_dt_col = 3
        st_tme_col = 2
        end_tme_col = 4

        # checks for all non numeric characters in the date and removes them
        st_nd[st_dt_col] = st_nd[st_dt_col].str.extract(
            "(\d+)", expand=False)
        st_nd[end_dt_col] = st_nd[end_dt_col].str.extract(
            "(\d+)", expand=False)
        st_nd[st_tme_col] = st_nd[st_tme_col].str.extract(
            "(\d+)", expand=False)
        st_nd[end_tme_col] = st_nd[end_tme_col].str.extract(
            "(\d+)", expand=False)
        st_nd.loc[st_nd[st_dt_col].str.len() < 6, st_dt_col] = None
        st_nd.loc[st_nd[end_dt_col].str.len() < 6, end_dt_col] = None

        # backfills the missing dates with the previous date
        st_nd[st_dt_col].bfill(inplace=True)
        st_nd[end_dt_col].bfill(inplace=True)
        st_nd[st_tme_col].bfill(inplace=True)
        st_nd[end_tme_col].bfill(inplace=True)

        # adds century to year if it is not there
        st_nd[st_dt_col] = st_nd[st_dt_col].apply(
            lambda x: str(date.today())[:2] + x if len(x) == 6 and int(x[:2]) <= 50
            else str(int(str(date.today())[:2])-1) + x
            if len(x) == 6 and int(x[:2]) > 50
            else x)
        st_nd[end_dt_col] = st_nd[end_dt_col].apply(
            lambda x: str(date.today())[:2] + x if len(x) == 6 and int(x[:2]) <= 50
            else str(int(str(date.today())[:2])-1) + x
            if len(x) == 6 and int(x[:2]) > 50
            else x)

        # index 2 and 4 are time, this makes the time uniform length
        st_nd[st_tme_col] = st_nd[st_tme_col].str.pad(
            width=7, side='right', fillchar="0")
        st_nd[end_tme_col] = st_nd[end_tme_col].str.pad(
            width=7, side='right', fillchar="0")

        # this filters for time = 24H00m and makes it zero hour
        st_nd[end_tme_col].loc[st_nd[end_tme_col].str[:2]
                               == '24'] = ('0').zfill(7)

        try:  # Looks for invalid date then changes to date format
            st_nd[st_dt_col] = st_nd[st_dt_col].apply(
                lambda x: pd.to_datetime(str(int(x)+1), format="%Y%m%d")
                if x[-2:] == "00"
                else pd.to_datetime(x, format="%Y%m%d").date())
        except ValueError:
            traceback.print_exc()

        try:  # adds a day if the hour is zero hour and changes string to datetime.date
            st_nd[end_dt_col] = st_nd[end_dt_col].apply(
                lambda x: pd.to_datetime(
                    x[end_dt_col], format="%Y%m%d").date() + timedelta(days=1)
                if x[4] == '0'.zfill(7) else pd.to_datetime(x[end_dt_col], format="%Y%m%d").date())
        except (ValueError, TypeError):
            print(self.file)
            st_nd[end_dt_col] = pd.to_datetime(
                st_nd[end_dt_col], format="%Y%m%d").dt.date

    # changes time string into datetime.time
        try:
            st_nd[st_tme_col] = pd.to_datetime(
                st_nd[st_tme_col], format="%H%M%S%f").dt.time
        except ValueError:
            st_nd[st_tme_col] = pd.to_datetime(
                "0".zfill(7), format="%H%M%S%f").strftime("%H:%M:%S")
        try:
            st_nd[end_tme_col] = pd.to_datetime(
                st_nd[end_tme_col], format="%H%M%S%f").dt.time
        except ValueError:
            st_nd[end_tme_col] = pd.to_datetime(
                "0".zfill(7), format="%H%M%S%f").strftime("%H:%M:%S")
        except:
            raise Exception(f"""header func: time not working 
            - st_nd[st_tme_col] = st_nd[st_tme_col].apply(lambda x: pd.to_datetime(x, format='%H%M%S%f').time())
             look at file {self.file}""")

        # creates start_datetime and end_datetime
        st_nd["start_datetime"] = pd.to_datetime((st_nd[st_dt_col].astype(
            str)+st_nd[st_tme_col].astype(str)), format='%Y-%m-%d%H:%M:%S')
        st_nd["end_datetime"] = pd.to_datetime((st_nd[end_dt_col].astype(
            str)+st_nd[end_tme_col].astype(str)), format='%Y-%m-%d%H:%M:%S')

        st_nd = st_nd.iloc[:, 1:].drop_duplicates()

        headers = pd.DataFrame()
        headers['start_datetime'] = st_nd.groupby(
            st_nd['start_datetime'].dt.year).min()['start_datetime']
        headers['end_datetime'] = st_nd.groupby(
            st_nd['end_datetime'].dt.year).max()['end_datetime']

        headers['site_id'] = self.site_id
        headers['document_url'] = self.file
        headers["header_id"] = self.header_id
        headers['year'] = headers['start_datetime'].dt.year
        headers["number_of_lanes"] = int(
            df.loc[df[0] == "L0", 2].drop_duplicates().reset_index(drop=True)[0])

        station_name = df.loc[df[0].isin(["S0"]), 3:].reset_index(
            drop=True).drop_duplicates().dropna(axis=1)
        headers["station_name"] = station_name[station_name.columns].apply(
            lambda row: ' '.join(row.values.astype(str)), axis=1)

        t21 = df.loc[df[0] == "21"].dropna(
            axis=1).drop_duplicates().reset_index().copy()
        t21 = t21.iloc[:, (t21.shape[1])-9:].astype(int)
        t21.columns = range(t21.columns.size)
        t21.rename(columns={
            0: 'speedbin1',
            1: 'speedbin2',
            2: 'speedbin3',
            3: 'speedbin4',
            4: 'speedbin5',
            5: 'speedbin6',
            6: 'speedbin7',
            7: 'speedbin8',
            8: 'speedbin9'}, inplace=True)

        headers = pd.concat([headers, t21], ignore_index=True,
                            axis=0).bfill().ffill().drop_duplicates()

        try:
            headers["summary_interval_minutes"] = int(
                df.loc[df[0] == "21", 1].drop_duplicates().reset_index(drop=True)[0])
            headers["summary_interval_minutes"] = int(
                df.loc[df[0] == "21", 1].drop_duplicates().reset_index(drop=True)[0])
            headers["summary_interval_minutes"] = int(
                df.loc[df[0] == "21", 1].drop_duplicates().reset_index(drop=True)[0])
        except KeyError:
            pass
        try:
            headers["summary_interval_minutes"] = int(
                df.loc[df[0] == "30", 1].drop_duplicates().reset_index(drop=True)[0])
            headers["summary_interval_minutes"] = int(
                df.loc[df[0] == "30", 1].drop_duplicates().reset_index(drop=True)[0])
            headers["summary_interval_minutes"] = int(
                df.loc[df[0] == "30", 1].drop_duplicates().reset_index(drop=True)[0])
            headers["vehicle_classification_scheme"] = int(
                df.loc[df[0] == "30", 3].drop_duplicates().reset_index(drop=True)[0])
        except KeyError:
            pass
        try:
            headers["summary_interval_minutes"] = int(
                df.loc[df[0] == "70", 1].drop_duplicates().reset_index(drop=True)[0])
            headers["summary_interval_minutes"] = int(
                df.loc[df[0] == "70", 1].drop_duplicates().reset_index(drop=True)[0])
            headers["summary_interval_minutes"] = int(
                df.loc[df[0] == "70", 1].drop_duplicates().reset_index(drop=True)[0])
        except KeyError:
            pass

        try:
            headers['dir1_id'] = int(df[df[0] == "L1"].dropna(
                axis=1).drop_duplicates().reset_index(drop=True)[2].min())
            headers['dir2_id'] = int(df[df[0] == "L1"].dropna(
                axis=1).drop_duplicates().reset_index(drop=True)[2].max())
        except (KeyError, ValueError):
            headers['dir1_id'] = 0
            headers['dir2_id'] = 4

        try:
            headers["vehicle_classification_scheme"] = int(df.loc[
                df[0] == "70", 2].drop_duplicates().reset_index(drop=True)[0])
            headers["maximum_gap_milliseconds"] = int(df.loc[
                df[0] == "70", 3].drop_duplicates().reset_index(drop=True)[0])
            headers["maximum_differential_speed"] = int(df.loc[
                df[0] == "70", 4].drop_duplicates().reset_index(drop=True)[0])
            headers["error_bin_code"] = int(df.loc[
                df[0] == "70", 5].drop_duplicates().reset_index(drop=True)[0])
        except KeyError:
            pass

        headers = headers.reset_index(drop=True)

        m = headers.select_dtypes(np.number)
        headers[m.columns] = m.round().astype('Int32')

        return headers

    def type_21(self) -> pd.DataFrame:  # deals with type 21
        if self.data_df is None:
            pass
        else:
            data = self.data_df.loc[(self.data_df[0] == "21")].dropna(
                axis=1, how="all"
            ).reset_index(drop=True).copy()
            if data.empty:
                pass
            else:
                if (data[1] == "0").any():
                    ddf = data
                    ddf.rename(columns={
                        4: "duration_min",
                        5: "lane_number",
                        6: "speedbin1",
                        7: "speedbin2",
                        8: "speedbin3",
                        9: "speedbin4",
                        10: "speedbin5",
                        11: "speedbin6",
                        12: "speedbin7",
                        13: "speedbin8",
                        14: "speedbin9",
                        15: "speedbin10",
                        16: "sum_of_heavy_vehicle_speeds",
                        17: "short_heavy_vehicles",
                        18: "medium_heavy_vehicles",
                        19: "long_heavy_vehicles",
                        20: "rear_to_rear_headway_shorter_than_2_seconds",
                        21: "rear_to_rear_headways_shorter_than_programmed_time"
                    }, inplace=True)
                    ddf["speedbin0"] = 0

                elif (data[1] == "1").any():
                    ddf = data
                    ddf.rename(columns={
                        4: "duration_min",
                        5: "lane_number",
                        6: "speedbin0",
                        7: "speedbin1",
                        8: "speedbin2",
                        9: "speedbin3",
                        10: "speedbin4",
                        11: "speedbin5",
                        12: "speedbin6",
                        13: "speedbin7",
                        14: "speedbin8",
                        15: "speedbin9",
                        16: "speedbin10",
                        17: "sum_of_heavy_vehicle_speeds",
                        18: "short_heavy_vehicles",
                        19: "medium_heavy_vehicles",
                        20: "long_heavy_vehicles",
                        21: "rear_to_rear_headway_shorter_than_2_seconds",
                        22: "rear_to_rear_headways_shorter_than_programmed_time",
                    }, inplace=True)

                ddf = ddf.fillna(0)

                ddf[["duration_min",
                    "lane_number",
                     "speedbin0",
                     "speedbin1",
                     "speedbin2",
                     "speedbin3",
                     "speedbin4",
                     "speedbin5",
                     "speedbin6",
                     "speedbin7",
                     "speedbin8",
                     "speedbin9",
                     "speedbin10",
                     "sum_of_heavy_vehicle_speeds",
                     "short_heavy_vehicles",
                     "medium_heavy_vehicles",
                     "long_heavy_vehicles",
                     "rear_to_rear_headway_shorter_than_2_seconds",
                     "rear_to_rear_headways_shorter_than_programmed_time"
                     ]] = ddf[["duration_min",
                               "lane_number",
                               "speedbin0",
                               "speedbin1",
                               "speedbin2",
                               "speedbin3",
                               "speedbin4",
                               "speedbin5",
                               "speedbin6",
                               "speedbin7",
                               "speedbin8",
                               "speedbin9",
                               "speedbin10",
                               "sum_of_heavy_vehicle_speeds",
                               "short_heavy_vehicles",
                               "medium_heavy_vehicles",
                               "long_heavy_vehicles",
                               "rear_to_rear_headway_shorter_than_2_seconds",
                               "rear_to_rear_headways_shorter_than_programmed_time"]].astype('Int32')

                ddf["total_heavy_vehicles"] = (
                    ddf[["short_heavy_vehicles", "medium_heavy_vehicles",
                        "long_heavy_vehicles"]].astype(int).sum(axis=1)
                )

                ddf["total_light_vehicles"] = (
                    ddf[["speedbin0", "speedbin1", "speedbin2", "speedbin3", "speedbin4", "speedbin5", "speedbin6", "speedbin7", "speedbin8", "speedbin9", "speedbin10"]].astype(
                        int).sum() - ddf[["short_heavy_vehicles", "medium_heavy_vehicles", "long_heavy_vehicles"]].astype(int).sum()
                )

                ddf["total_vehicles"] = (ddf[["speedbin0", "speedbin1", "speedbin2", "speedbin3", "speedbin4", "speedbin5", "speedbin6", "speedbin7", "speedbin8", "speedbin9", "speedbin10"]].astype(int).sum()
                                         )

                try:
                    ddf['year'] = ddf['start_datetime'].dt.year
                except AttributeError:
                    ddf['year'] = int(ddf['start_datetime'].str[:4][0])

                ddf["site_id"] = self.site_id
                ddf["header_id"] = self.header_id

                return ddf

    def type_30(self) -> pd.DataFrame:
        """
        It takes a dataframe, splits it into two dataframes, then loops through the second dataframe and
        inserts the data into a database
        :return: A dataframe
        """
        if self.data_df is None:
            pass
        else:
            data = self.data_df.loc[(self.data_df[0] == "30")].dropna(
                axis=1, how="all"
            ).reset_index(drop=True).copy()
            header = self.head_df.loc[(self.head_df[0] == "30")].dropna(
                axis=1, how="all"
            ).reset_index(drop=True).copy()

            if data.empty:
                pass
            else:
                if header.shape[1] > 3:
                    classification_scheme = header.iloc[0, 3]
                    number_of_data_records = header.iloc[0, 4]
                else:
                    classification_scheme = header.iloc[0, 2]
                    number_of_data_records = header.iloc[0, 3]

                # vc_df = select_classification_scheme(classification_scheme)

                if data[1].isin(["0", "2", 0, 2]).any():
                    ddf = data.iloc[:, 1:].dropna(
                        axis=1, how="all").reset_index(drop=True)

                    ddf[ddf.select_dtypes(include=['object']).columns] = ddf[
                        ddf.select_dtypes(include=['object']).columns].apply(
                        pd.to_numeric, axis=1, errors='ignore')

                    ddf['vehicle_classification_scheme'] = int(
                        classification_scheme)

                    ddf.columns = ddf.columns.astype(str)

                    df3 = pd.DataFrame(columns=[
                        'edit_code',
                        'start_datetime',
                        'end_date',
                        'end_time',
                        'duration_of_summary',
                        'lane_number',
                        'number_of_vehicles',
                        'class_number',
                        'direction',
                        'compass_heading'])
                    for lane_no in range(1, int(self.max_lanes)+1):
                        for i in range(6, int(number_of_data_records)+6):
                            join_to_df3 = ddf.loc[ddf['5'].astype(int) == lane_no, [
                                '1', 'start_datetime', 'end_date', 'end_time', '4', '5', str(i), 'direction', 'compass_heading']]
                            join_to_df3['class_number'] = i-5
                            join_to_df3.rename(columns={
                                '1': "edit_code",
                                '2': "end_date",
                                '3': "end_time",
                                '4': "duration_of_summary",
                                '5': 'lane_number',
                                str(i): 'number_of_vehicles'
                            }, inplace=True)
                            # TODO: test efficiency of this vs merge then insert
                            # OPTIMIZE: merge then insert
                            # df3 = pd.concat([df3, join_to_df3], keys=[
                            #                 'start_datetime', 'lane_number'], ignore_index=True, axis=0)
                            # df3 = df3.apply(pd.to_numeric, axis=1, errors="ignore")
                            df3["header_id"] = self.header_id
                            join_to_df3['classification_scheme'] = int(
                                classification_scheme)
                            join_to_df3['site_id'] = self.site_id
                            join_to_df3['year'] = int(
                                data['start_datetime'].at[0].year)
                            join_to_df3[['edit_code',
                                         'duration_of_summary',
                                         'lane_number',
                                         'number_of_vehicles',
                                         'class_number',
                                         'compass_heading',
                                         'classification_scheme',
                                         'year']] = join_to_df3[[
                                             'edit_code',
                                             'duration_of_summary',
                                             'lane_number',
                                             'number_of_vehicles',
                                             'class_number',
                                             'compass_heading',
                                             'classification_scheme',
                                             'year']].apply(pd.to_numeric, errors="coerce")
                            push_to_db(join_to_df3, config.TYPE_30_TBL_NAME)
                    return df3
                else:
                    pass

    def type_70(self) -> pd.DataFrame:
        if self.data_df is None:
            pass
        else:
            data = self.data_df.loc[(self.data_df[0] == "70")].dropna(
                axis=1, how="all"
            ).reset_index(drop=True).copy()
            if data.empty:
                pass
            else:
                if data[1].all() == "1":
                    ddf = data.iloc[:, 3:]
                    ddf = pd.DataFrame(ddf).dropna(
                        axis=1, how="all").reset_index(drop=True)

                    ddf.rename(columns={
                        4: "duration_min",
                        5: "lane_number",
                        6: "number_of_error_vehicles",
                        7: "total_free_flowing_light_vehicles",
                        8: "total_following_light_vehicles",
                        9: "total_free_flowing_heavy_vehicles",
                        10: "total_following_heavy_vehicles",
                        11: "sum_of_inverse_of_speeds_for_free_flowing_lights",
                        12: "sum_of_inverse_of_speeds_for_following_lights",
                        13: "sum_of_inverse_of_speeds_for_free_flowing_heavies",
                        14: "sum_of_inverse_of_speeds_for_following_heavies",
                        15: "sum_of_speeds_for_free_flowing_lights",
                        16: "sum_of_speeds_for_following_lights",
                        17: "sum_of_speeds_for_free_flowing_heavies",
                        18: "sum_of_speeds_for_following_heavies",
                        19: "sum_of_squared_speeds_of_free_flowing_lights",
                        20: "sum_of_squared_speeds_for_following_lights",
                        21: "sum_of_squared_speeds_of_free_flowing_heavies",
                        22: "sum_of_squared_speeds_for_following_heavies",
                    }, inplace=True)

                else:
                    ddf = data.iloc[:, 2:]
                    ddf = pd.DataFrame(ddf).dropna(
                        axis=1, how="all").reset_index(drop=True)
                    ddf.rename(columns={
                        4: "duration_min",
                        5: "lane_number",
                        6: "total_free_flowing_light_vehicles",
                        7: "total_following_light_vehicles",
                        8: "total_free_flowing_heavy_vehicles",
                        9: "total_following_heavy_vehicles",
                        10: "sum_of_inverse_of_speeds_for_free_flowing_lights",
                        11: "sum_of_inverse_of_speeds_for_following_lights",
                        12: "sum_of_inverse_of_speeds_for_free_flowing_heavies",
                        13: "sum_of_inverse_of_speeds_for_following_heavies",
                        14: "sum_of_speeds_for_free_flowing_lights",
                        15: "sum_of_speeds_for_following_lights",
                        16: "sum_of_speeds_for_free_flowing_heavies",
                        17: "sum_of_speeds_for_following_heavies",
                        18: "sum_of_squared_speeds_of_free_flowing_lights",
                        19: "sum_of_squared_speeds_for_following_lights",
                        20: "sum_of_squared_speeds_of_free_flowing_heavies",
                        21: "sum_of_squared_speeds_for_following_heavies",
                    }, inplace=True)
                    ddf["number_of_error_vehicles"] = 0

                ddf = ddf.fillna(0)

                ddf[["duration_min",
                     "lane_number",
                     "number_of_error_vehicles",
                     "total_free_flowing_light_vehicles",
                     "total_following_light_vehicles",
                     "total_free_flowing_heavy_vehicles",
                     "total_following_heavy_vehicles",
                    "sum_of_inverse_of_speeds_for_free_flowing_lights",
                     "sum_of_inverse_of_speeds_for_following_lights",
                     "sum_of_inverse_of_speeds_for_free_flowing_heavies",
                     "sum_of_inverse_of_speeds_for_following_heavies",
                     "sum_of_speeds_for_free_flowing_lights",
                     "sum_of_speeds_for_following_lights",
                     "sum_of_speeds_for_free_flowing_heavies",
                     "sum_of_speeds_for_following_heavies",
                     "sum_of_squared_speeds_of_free_flowing_lights",
                     "sum_of_squared_speeds_for_following_lights",
                     "sum_of_squared_speeds_of_free_flowing_heavies",
                     "sum_of_squared_speeds_for_following_heavies"]] = ddf[["duration_min",
                                                                           "lane_number",
                                                                            "number_of_error_vehicles",
                                                                            "total_free_flowing_light_vehicles",
                                                                            "total_following_light_vehicles",
                                                                            "total_free_flowing_heavy_vehicles",
                                                                            "total_following_heavy_vehicles",
                                                                            "sum_of_inverse_of_speeds_for_free_flowing_lights",
                                                                            "sum_of_inverse_of_speeds_for_following_lights",
                                                                            "sum_of_inverse_of_speeds_for_free_flowing_heavies",
                                                                            "sum_of_inverse_of_speeds_for_following_heavies",
                                                                            "sum_of_speeds_for_free_flowing_lights",
                                                                            "sum_of_speeds_for_following_lights",
                                                                            "sum_of_speeds_for_free_flowing_heavies",
                                                                            "sum_of_speeds_for_following_heavies",
                                                                            "sum_of_squared_speeds_of_free_flowing_lights",
                                                                            "sum_of_squared_speeds_for_following_lights",
                                                                            "sum_of_squared_speeds_of_free_flowing_heavies",
                                                                            "sum_of_squared_speeds_for_following_heavies"]].astype('Int32')

                m = ddf.select_dtypes(np.number)
                ddf[m.columns] = m.round().astype('Int32')
                ddf['year'] = ddf['start_datetime'].dt.year
                ddf["site_id"] = self.site_id
                ddf["header_id"] = self.header_id

                return ddf

    def type_60(self) -> pd.DataFrame:
        """
        It takes a dataframe, and returns a dataframe
        :return: A dataframe
        """
        if self.data_df is None:
            pass
        else:
            data = self.data_df.loc[(self.data_df[0] == "60")].dropna(
                axis=1, how="all"
            ).reset_index(drop=True).copy()
            dfh = self.head_df.loc[(self.head_df[0] == "60")].dropna(
                axis=1, how="all"
            ).drop_duplicates().reset_index(drop=True).copy()
            if data.empty:
                pass
            else:
                dfh['error_bin'] = 0
                number_of_data_records = dfh.iloc[0, 3]

                if data[1].isin(["0", "1", "2", "3", "4"]).any():
                    ddf = data.iloc[:, 1:].reset_index(drop=True)
                    ddf = pd.DataFrame(ddf).dropna(
                        axis=1, how="all").reset_index(drop=True)

                    ddf[ddf.select_dtypes(include=['object']).columns] = ddf[
                        ddf.select_dtypes(include=['object']).columns
                    ].apply(pd.to_numeric, axis=1, errors='ignore')
                    ddf.columns = ddf.columns.astype(str)

                    df3 = pd.DataFrame(columns=[
                        'edit_code',
                        'start_datetime',
                        'end_date',
                        'end_time',
                        'duration_of_summary',
                        'lane_number',
                        'bin_number',
                        'number_of_vehicles',
                        'bin_boundary_length_cm',
                        'direction',
                        'compass_heading'])

                    for i in range(6, int(number_of_data_records)+6):
                        for lane_no in range(1, int(self.max_lanes)+1):
                            join_to_df3 = ddf.loc[ddf['5'].astype(int) == lane_no, [
                                '1',  # edit_code
                                'start_datetime',
                                'end_date',  # end_date
                                'end_time',  # end_time
                                '4',  # duration_of_summary
                                '5',  # lane_number
                                str(i),  # "number_of_vehicles"
                                'direction',
                                'compass_heading'
                            ]]
                            if str(int(i)-6) == "0":
                                bin_bound_col = 'error_bin'
                            else:
                                bin_bound_col = int(i)-3
                            join_to_df3['bin_number'] = str(i-6)
                            join_to_df3['bin_boundary_length_cm'] = int(
                                dfh[bin_bound_col][0])
                            join_to_df3.rename(columns={
                                '1': "edit_code",
                                '4': "duration_of_summary",
                                '5': "lane_number",
                                str(i): "number_of_vehicles"
                            }, inplace=True)
                            df3 = pd.concat([df3, join_to_df3],
                                            axis=0, ignore_index=True)
                    df3 = df3.apply(pd.to_numeric, axis=1, errors="ignore")
                    df3['site_id'] = self.site_id
                    df3["header_id"] = self.header_id
                    try:
                        if df3 is None:
                            pass
                        else:
                            df3 = df3[df3.columns.intersection(t60_cols)]
                            push_to_db(df3, config.TYPE_60_TBL_NAME)
                    except Exception as exc:
                        traceback.print_exc()
                        with open(os.path.expanduser(config.FILES_FAILED), "a", newline="",) as f:
                            write = csv.writer(f)
                            write.writerows([[self.file]])
                        gc.collect()
                else:
                    pass
                return df3

    def header_calcs(self, header: pd.DataFrame, data: pd.DataFrame, type: int) -> pd.DataFrame:
        """
        It takes a dataframe, and a type, and returns a dataframe

        :param header: a dataframe with the header information
        :type header: pd.DataFrame
        :param data: pd.DataFrame
        :type data: pd.DataFrame
        :param type: int
        :type type: int
        :return: A dataframe with the calculated values.
        """
        try:
            if data is None and header is None:
                pass
            else:
                speed_limit_qry = f"select max_speed from trafc.countstation where tcname = '{self.site_id}' ;"
                speed_limit = pd.read_sql_query(
                    speed_limit_qry, config.ENGINE).reset_index(drop=True)
                try:
                    speed_limit = speed_limit['max_speed'].iloc[0]
                except IndexError:
                    speed_limit = 60
                data = data.fillna(0)
                if type == 21:
                    try:
                        header['adt_total'] = data['total_vehicles'].groupby(
                            [data['start_datetime'].dt.to_period('D')]).sum().mean().round().astype(int)
                        try:
                            header['adt_positive_direction'] = round(data['total_vehicles'].groupby(
                                [data['start_datetime'].dt.to_period('D'), data['direction'].loc[data['direction'] == 'P']]).sum().mean())
                        except ValueError:
                            header['adt_positive_direction'] = 0
                        try:
                            header['adt_negative_direction'] = round(data['total_vehicles'].groupby(
                                [data['start_datetime'].dt.to_period('D'), data['direction'].loc[data['direction'] == 'N']]).sum().mean())
                        except ValueError:
                            header['adt_negative_direction'] = 0

                        header['adtt_total'] = data['total_heavy_vehicles'].groupby(
                            [data['start_datetime'].dt.to_period('D')]).sum().mean().round().astype(int)
                        try:
                            header['adtt_positive_direction'] = round(data['total_vehicles'].groupby(
                                [data['start_datetime'].dt.to_period('D'), data['direction'].loc[data['direction'] == 'P']]).sum().mean())
                        except ValueError:
                            header['adtt_positive_direction'] = 0
                        try:
                            header['adtt_negative_direction'] = round(data['total_vehicles'].groupby(
                                [data['start_datetime'].dt.to_period('D'), data['direction'].loc[data['direction'] == 'N']]).sum().mean())
                        except ValueError:
                            header['adtt_negative_direction'] = 0

                        header['total_vehicles'] = data['total_vehicles'].astype(
                            int).sum()
                        try:
                            header['total_vehicles_positive_direction'] = data['total_vehicles'].groupby(
                                data['direction'].loc[data['direction'] == 'P']).astype(int).sum()[0]
                        except IndexError:
                            header['total_vehicles_positive_direction'] = 0
                        try:
                            header['total_vehicles_negative_direction'] = data['total_vehicles'].groupby(
                                data['direction'].loc[data['direction'] == 'N']).astype(int).sum()[0]
                        except IndexError:
                            header['total_vehicles_negative_direction'] = 0

                        header['total_heavy_vehicles'] = data['total_heavy_vehicles'].sum(
                        )
                        try:
                            header['total_heavy_negative_direction'] = data['total_heavy_vehicles'].groupby(
                                [data['direction'].loc[data['direction'] == 'N']]).astype(int).sum()[0]
                        except IndexError:
                            header['total_heavy_negative_direction'] = 0
                        try:
                            header['total_heavy_positive_direction'] = data['total_heavy_vehicles'].groupby(
                                [data['direction'].loc[data['direction'] == 'P']]).astype(int).sum()[0]
                        except IndexError:
                            header['total_heavy_positive_direction'] = 0
                        try:
                            header['truck_split_negative_direction'] = data['total_heavy_vehicles'].groupby(
                                [data['direction'].loc[data['direction'] == 'N']]).sum()[0] / data['total_heavy_vehicles'].sum()
                        except IndexError:
                            header['truck_split_negative_direction'] = 0
                        try:
                            header['truck_split_positive_direction'] = data['total_heavy_vehicles'].groupby(
                                [data['direction'].loc[data['direction'] == 'P']]).sum()[0] / data['total_heavy_vehicles'].sum()
                        except IndexError:
                            header['truck_split_positive_direction'] = 0

                        header['total_light_vehicles'] = data['total_light_vehicles'].sum(
                        )
                        try:
                            header['total_light_positive_direction'] = data['total_light_vehicles'].groupby(
                                [data['direction'].loc[data['direction'] == 'P']]).sum()[0]
                        except IndexError:
                            header['total_light_positive_direction'] = 0
                        try:
                            header['total_light_negative_direction'] = data['total_light_vehicles'].groupby(
                                [data['direction'].loc[data['direction'] == 'N']]).sum()[0]
                        except IndexError:
                            header['total_light_negative_direction'] = 0

                        header['short_heavy_vehicles'] = data['short_heavy_vehicles'].sum()
                        try:
                            header['short_heavy_positive_direction'] = data['short_heavy_vehicles'].groupby(
                                [data['direction'].loc[data['direction'] == 'P']]).sum()[0]
                        except IndexError:
                            header['short_heavy_positive_direction'] = 0
                        try:
                            header['short_heavy_negative_direction'] = data['short_heavy_vehicles'].groupby(
                                [data['direction'].loc[data['direction'] == 'N']]).sum()[0]
                        except IndexError:
                            header['short_heavy_negative_direction'] = 0

                        header['Medium_heavy_vehicles'] = data['medium_heavy_vehicles'].sum()
                        try:
                            header['Medium_heavy_negative_direction'] = data['medium_heavy_vehicles'].groupby(
                                [data['direction'].loc[data['direction'] == 'N']]).sum()[0]
                        except IndexError:
                            header['Medium_heavy_negative_direction'] = 0
                        try:
                            header['Medium_heavy_positive_direction'] = data['medium_heavy_vehicles'].groupby(
                                [data['direction'].loc[data['direction'] == 'P']]).sum()[0]
                        except IndexError:
                            header['Medium_heavy_positive_direction'] = 0

                        header['long_heavy_vehicles'] = data['long_heavy_vehicles'].sum()
                        try:
                            header['long_heavy_positive_direction'] = data['long_heavy_vehicles'].groupby(
                                [data['direction'].loc[data['direction'] == 'P']]).sum()[0]
                        except IndexError:
                            header['long_heavy_positive_direction'] = 0
                        try:
                            header['long_heavy_negative_direction'] = data['long_heavy_vehicles'].groupby(
                                [data['direction'].loc[data['direction'] == 'N']]).sum()[0]
                        except IndexError:
                            header['long_heavy_negative_direction'] = 0

                        try:
                            header['vehicles_with_rear_to_rear_headway_less_than_2sec_positive_dire'] = data['rear_to_rear_headway_shorter_than_2_seconds'].groupby(
                                [data['direction'].loc[data['direction'] == 'P']]).sum()[0]
                        except IndexError:
                            header['vehicles_with_rear_to_rear_headway_less_than_2sec_positive_dire'] = 0
                        try:
                            header['vehicles_with_rear_to_rear_headway_less_than_2sec_negative_dire'] = data['rear_to_rear_headway_shorter_than_2_seconds'].groupby(
                                [data['direction'].loc[data['direction'] == 'N']]).sum()[0]
                        except IndexError:
                            header['vehicles_with_rear_to_rear_headway_less_than_2sec_negative_dire'] = 0
                        header['vehicles_with_rear_to_rear_headway_less_than_2sec_total'] = data['rear_to_rear_headway_shorter_than_2_seconds'].sum()

                        header['summary_interval_minutes'] = data['duration_min'].mean()

                        try:
                            header['highest_volume_per_hour_positive_direction'] = round(data['total_vehicles'].groupby(
                                [data['start_datetime'].dt.to_period('H'), data['direction'].loc[data['direction'] == 'P']]).sum().max())
                        except ValueError:
                            header['highest_volume_per_hour_positive_direction'] = 0
                        try:
                            header['highest_volume_per_hour_negative_direction'] = round(data['total_vehicles'].groupby(
                                [data['start_datetime'].dt.to_period('H'), data['direction'].loc[data['direction'] == 'N']]).sum().max())
                        except ValueError:
                            header['highest_volume_per_hour_negative_direction'] = 0
                        header['highest_volume_per_hour_total'] = data['total_vehicles'].groupby(
                            [data['start_datetime'].dt.to_period('H')]).sum().max()

                        try:
                            header['15th_highest_volume_per_hour_positive_direction'] = round(data['total_vehicles'].groupby(
                                [data['start_datetime'].dt.to_period('H'), data['direction'].loc[data['direction'] == 'P']]).sum().quantile(q=0.15,  interpolation='linear'))
                        except ValueError:
                            header['15th_highest_volume_per_hour_positive_direction'] = 0
                        try:
                            header['15th_highest_volume_per_hour_negative_direction'] = round(data['total_vehicles'].groupby(
                                [data['start_datetime'].dt.to_period('H'), data['direction'].loc[data['direction'] == 'N']]).sum().quantile(q=0.15,  interpolation='linear'))
                        except ValueError:
                            header['15th_highest_volume_per_hour_negative_direction'] = 0
                        header['15th_highest_volume_per_hour_total'] = round(data['total_vehicles'].groupby(
                            [data['start_datetime'].dt.to_period('H')]).sum().quantile(q=0.15, interpolation='linear'))

                        try:
                            header['30th_highest_volume_per_hour_positive_direction'] = round(data['total_vehicles'].groupby(
                                [data['start_datetime'].dt.to_period('H'), data['direction'].loc[data['direction'] == 'P']]).sum().quantile(q=0.3,  interpolation='linear'))
                        except ValueError:
                            header['30th_highest_volume_per_hour_positive_direction'] = 0
                        try:
                            header['30th_highest_volume_per_hour_negative_direction'] = round(data['total_vehicles'].groupby(
                                [data['start_datetime'].dt.to_period('H'), data['direction'].loc[data['direction'] == 'N']]).sum().quantile(q=0.3, interpolation='linear'))
                        except ValueError:
                            header['30th_highest_volume_per_hour_negative_direction'] = 0
                        header['30th_highest_volume_per_hour_total'] = round(data['total_vehicles'].groupby(
                            [data['start_datetime'].dt.to_period('H')]).sum().quantile(q=0.3, interpolation='linear'))

                        header['average_speed_positive_direction'] = (round((
                            (header['speedbin1'] * data.loc[data['direction'] == 'P', 'speedbin1'].sum()) +
                            (header['speedbin2'] * data.loc[data['direction'] == 'P', 'speedbin2'].sum()) +
                            (header['speedbin3'] * data.loc[data['direction'] == 'P', 'speedbin3'].sum()) +
                            (header['speedbin4'] * data.loc[data['direction'] == 'P', 'speedbin4'].sum()) +
                            (header['speedbin5'] * data.loc[data['direction'] == 'P', 'speedbin5'].sum()) +
                            (header['speedbin6'] * data.loc[data['direction'] == 'P', 'speedbin6'].sum()) +
                            (header['speedbin7'] * data.loc[data['direction'] == 'P', 'speedbin7'].sum()) +
                            (header['speedbin8'] * data.loc[data['direction'] == 'P', 'speedbin8'].sum()) +
                            (header['speedbin9'] * data.loc[data['direction']
                                                            == 'P', 'speedbin9'].sum())
                        ))
                            / data.loc[data['direction'] == 'P', 'sum_of_heavy_vehicle_speeds'].astype(int).sum()
                        )

                        header['average_speed_negative_direction'] = (round((
                            (header['speedbin1'] * data.loc[data['direction'] == 'N', 'speedbin1'].sum()) +
                            (header['speedbin2'] * data.loc[data['direction'] == 'N', 'speedbin2'].sum()) +
                            (header['speedbin3'] * data.loc[data['direction'] == 'N', 'speedbin3'].sum()) +
                            (header['speedbin4'] * data.loc[data['direction'] == 'N', 'speedbin4'].sum()) +
                            (header['speedbin5'] * data.loc[data['direction'] == 'N', 'speedbin5'].sum()) +
                            (header['speedbin6'] * data.loc[data['direction'] == 'N', 'speedbin6'].sum()) +
                            (header['speedbin7'] * data.loc[data['direction'] == 'N', 'speedbin7'].sum()) +
                            (header['speedbin8'] * data.loc[data['direction'] == 'N', 'speedbin8'].sum()) +
                            (header['speedbin9'] * data.loc[data['direction']
                             == 'N', 'speedbin9'].sum())
                        ))
                            / data.loc[data['direction'] == 'N', 'sum_of_heavy_vehicle_speeds'].astype(int).sum()
                        )

                        header['average_speed'] = (round((
                            (header['speedbin1'] * data['speedbin1'].sum()) +
                            (header['speedbin2'] * data['speedbin2'].sum()) +
                            (header['speedbin3'] * data['speedbin3'].sum()) +
                            (header['speedbin4'] * data['speedbin4'].sum()) +
                            (header['speedbin5'] * data['speedbin5'].sum()) +
                            (header['speedbin6'] * data['speedbin6'].sum()) +
                            (header['speedbin7'] * data['speedbin7'].sum()) +
                            (header['speedbin8'] * data['speedbin8'].sum()) +
                            (header['speedbin9'] * data['speedbin9'].sum())
                        ))
                            / data['total_vehicles'].astype(int).sum()
                        )

                        header['average_speed_light_vehicles_positive_direction'] = (round((
                            (header['speedbin1'] * data.loc[data['direction'] == 'P', 'speedbin1'].sum()) +
                            (header['speedbin2'] * data.loc[data['direction'] == 'P', 'speedbin2'].sum()) +
                            (header['speedbin3'] * data.loc[data['direction'] == 'P', 'speedbin3'].sum()) +
                            (header['speedbin4'] * data.loc[data['direction'] == 'P', 'speedbin4'].sum()) +
                            (header['speedbin5'] * data.loc[data['direction'] == 'P', 'speedbin5'].sum()) +
                            (header['speedbin6'] * data.loc[data['direction'] == 'P', 'speedbin6'].sum()) +
                            (header['speedbin7'] * data.loc[data['direction'] == 'P', 'speedbin7'].sum()) +
                            (header['speedbin8'] * data.loc[data['direction'] == 'P', 'speedbin8'].sum()) +
                            (header['speedbin9'] * data.loc[data['direction'] == 'P', 'speedbin9'].sum()) -
                            data.loc[data['direction'] == 'P',
                                     'sum_of_heavy_vehicle_speeds'].sum()
                        ))
                            / data.loc[data['direction'] == 'P', 'total_light_vehicles'].astype(int).sum()
                        )

                        header['average_speed_light_vehicles_negative_direction'] = (round((
                            (header['speedbin1'] * data.loc[data['direction'] == 'N', 'speedbin1'].sum()) +
                            (header['speedbin2'] * data.loc[data['direction'] == 'N', 'speedbin2'].sum()) +
                            (header['speedbin3'] * data.loc[data['direction'] == 'N', 'speedbin3'].sum()) +
                            (header['speedbin4'] * data.loc[data['direction'] == 'N', 'speedbin4'].sum()) +
                            (header['speedbin5'] * data.loc[data['direction'] == 'N', 'speedbin5'].sum()) +
                            (header['speedbin6'] * data.loc[data['direction'] == 'N', 'speedbin6'].sum()) +
                            (header['speedbin7'] * data.loc[data['direction'] == 'N', 'speedbin7'].sum()) +
                            (header['speedbin8'] * data.loc[data['direction'] == 'N', 'speedbin8'].sum()) +
                            (header['speedbin9'] * data.loc[data['direction'] == 'N', 'speedbin9'].sum()) -
                            data.loc[data['direction'] == 'N',
                                     'sum_of_heavy_vehicle_speeds'].sum()
                        ))
                            / data.loc[data['direction'] == 'N', 'total_light_vehicles'].astype(int).sum()
                        )

                        header['average_speed_light_vehicles'] = (round(
                            (header['speedbin1'] * data['speedbin1'].sum()) +
                            (header['speedbin2'] * data['speedbin2'].sum()) +
                            (header['speedbin3'] * data['speedbin3'].sum()) +
                            (header['speedbin4'] * data['speedbin4'].sum()) +
                            (header['speedbin5'] * data['speedbin5'].sum()) +
                            (header['speedbin6'] * data['speedbin6'].sum()) +
                            (header['speedbin7'] * data['speedbin7'].sum()) +
                            (header['speedbin8'] * data['speedbin8'].sum()) +
                            (header['speedbin9'] * data['speedbin9'].sum()) -
                            data['sum_of_heavy_vehicle_speeds'].sum()
                        )
                            / data['total_light_vehicles'].sum()
                        )

                        header['average_speed_heavy_vehicles_positive_direction'] = (int((
                            data.loc[data['direction'] == 'P', 'sum_of_heavy_vehicle_speeds'].sum(
                            ) / data.loc[data['direction'] == 'P', 'total_heavy_vehicles'].sum()
                        )))

                        header['average_speed_heavy_vehicles_negative_direction'] = (int((
                            data.loc[data['direction'] == 'N', 'sum_of_heavy_vehicle_speeds'].sum(
                            ) / data.loc[data['direction'] == 'N', 'total_heavy_vehicles'].sum()
                        )))

                        header['average_speed_heavy_vehicles'] = (int((
                            data['sum_of_heavy_vehicle_speeds'].sum(
                            ) / data['total_heavy_vehicles'].sum()
                        )))

                        try:
                            shv_p = str(round(
                                round(data['short_heavy_vehicles'].groupby([data['direction'].loc[data['direction'] == 'P']]).sum().values[0]) /
                                round(data['total_heavy_vehicles'].groupby([data['direction'].loc[data['direction'] == 'P']]).sum().values[0]*100)))
                        except (ValueError, IndexError, ZeroDivisionError):
                            shv_p = 0
                        try:
                            mhv_p = str(round(
                                round(data['medium_heavy_vehicles'].groupby([data['direction'].loc[data['direction'] == 'P']]).sum().values[0]) /
                                round(data['total_heavy_vehicles'].groupby([data['direction'].loc[data['direction'] == 'P']]).sum()[0]*100)))
                        except (ValueError, IndexError, ZeroDivisionError):
                            mhv_p = 0
                        try:
                            lhv_p = str(round(
                                round(data['long_heavy_vehicles'].groupby([data['direction'].loc[data['direction'] == 'P']]).sum().values[0]) /
                                round(data['total_heavy_vehicles'].groupby([data['direction'].loc[data['direction'] == 'P']]).sum()[0]*100)))
                        except (ValueError, IndexError, ZeroDivisionError):
                            lhv_p = 0
                        header['truck_split_positive_direction'] = str(str(
                            shv_p) + ' : ' + str(mhv_p) + ' : ' + str(lhv_p))

                        try:
                            shv_n = str(round(
                                round(data['short_heavy_vehicles'].groupby([data['direction'].loc[data['direction'] == 'N']]).sum().values[0]) /
                                round(data['total_heavy_vehicles'].groupby([data['direction'].loc[data['direction'] == 'N']]).sum().values[0]*100)))
                        except (ValueError, IndexError, ZeroDivisionError):
                            shv_n = "0"
                        try:
                            mhv_n = str(round(
                                round(data['medium_heavy_vehicles'].groupby([data['direction'].loc[data['direction'] == 'N']]).sum().values[0]) /
                                round(data['total_heavy_vehicles'].groupby([data['direction'].loc[data['direction'] == 'N']]).sum()[0]*100)))
                        except (ValueError, IndexError, ZeroDivisionError):
                            mhv_n = "0"
                        try:
                            lhv_n = str(round(
                                round(data['long_heavy_vehicles'].groupby([data['direction'].loc[data['direction'] == 'N']]).sum().values[0]) /
                                round(data['total_heavy_vehicles'].groupby([data['direction'].loc[data['direction'] == 'N']]).sum()[0]*100)))
                        except (ValueError, IndexError, ZeroDivisionError):
                            lhv_n = "0"
                        header['truck_split_positive_direction'] = str(str(
                            shv_n) + ' : ' + str(mhv_n) + ' : ' + str(lhv_n))

                        try:
                            shv_t = str(round(
                                round(data['short_heavy_vehicles'].sum()) /
                                round(data['total_heavy_vehicles'].sum()*100)))
                        except (ValueError, IndexError, ZeroDivisionError):
                            shv_t = "0"
                        try:
                            mhv_t = str(round(
                                round(data['medium_heavy_vehicles'].sum()) /
                                round(data['total_heavy_vehicles'].sum()*100)))
                        except (ValueError, IndexError, ZeroDivisionError):
                            mhv_t = "0"
                        try:
                            lhv_t = str(round(
                                round(data['long_heavy_vehicles'].sum()) /
                                round(data['total_heavy_vehicles'].sum()*100)))
                        except (ValueError, IndexError, ZeroDivisionError):
                            lhv_t = "0"
                        header['truck_split_total'] = str(str(
                            shv_t) + ' : ' + str(mhv_t) + ' : ' + str(lhv_t))

                        header['percentage_heavy_vehicles_positive_direction'] = float(round(
                            data.loc[data['direction'] == 'P', 'heavy_vehicles_type21'].sum()/data.loc[data['direction'] == 'P', 'total_vehicles'].sum(), 2))
                        header['percentage_heavy_vehicles_negative_direction'] = float(round(
                            data.loc[data['direction'] == 'N', 'heavy_vehicles_type21'].sum()/data.loc[data['direction'] == 'N', 'total_vehicles'].sum(), 2))
                        header['percentage_heavy_vehicles'] = float(round(
                            data['heavy_vehicles_type21'].sum()/data['total_vehicles'].sum(), 2))

                        header['night_adt'] = data.loc[data['start_datetime'].dt.hour >=
                                                       18 and data['start_datetime'].dt.hour <= 6]['total_vehicles'].sum()
                        header['night_adtt'] = data.loc[data['start_datetime'].dt.hour >=
                                                        18 and data['start_datetime'].dt.hour <= 6]['total_heavy_vehicles'].sum()

                        # TODO: add percentile speeds (15th, 30th, 85th)

                    except KeyError:
                        pass
                    try:
                        header["summary_interval_minutes"] = header["summary_interval_minutes"].round(
                        ).astype(int)
                    except (KeyError, pd.errors.IntCastingNaNError):
                        pass

                    return header

                elif type == 30:
                    try:
                        if header['adt_total'].isnull().all():
                            header['adt_total'] = data['total_vehicles'].groupby(
                                [data['start_datetime'].dt.to_period('D')]).sum().mean().astype(int)
                            try:
                                header['adt_positive_direction'] = round(data['total_vehicles'].groupby(
                                    [data['start_datetime'].dt.to_period('D'), data['direction'].loc[data['direction'] == 'P']]).sum().mean())
                            except (ValueError, IndexError):
                                header['adt_positive_direction'] = 0
                            try:
                                header['adt_negative_direction'] = round(data['total_vehicles'].groupby(
                                    [data['start_datetime'].dt.to_period('D'), data['direction'].loc[data['direction'] == 'N']]).sum().mean())
                            except (ValueError, IndexError):
                                header['adt_negative_direction'] = 0
                        else:
                            pass

                        if header['adtt_total'].isnull().all():
                            header['adtt_total'] = data['total_heavy_vehicles'].groupby(
                                [data['start_datetime'].dt.to_period('D')]).sum().mean().astype(int)
                            try:
                                header['adtt_positive_direction'] = round(data['total_vehicles'].groupby(
                                    [data['start_datetime'].dt.to_period('D'), data['direction'].loc[data['direction'] == 'P']]).sum().mean())
                            except (ValueError, IndexError):
                                header['adtt_positive_direction'] = 0
                            try:
                                header['adtt_negative_direction'] = round(data['total_vehicles'].groupby(
                                    [data['start_datetime'].dt.to_period('D'), data['direction'].loc[data['direction'] == 'N']]).sum().mean())
                            except (ValueError, IndexError):
                                header['adtt_negative_direction'] = 0
                        else:
                            pass

                        if header['total_vehicles'].isnull().all():
                            header['total_vehicles'] = data['total_vehicles'].sum()
                            try:
                                header['total_vehicles_positive_direction'] = data['total_vehicles'].groupby(
                                    data['direction'].loc[data['direction'] == 'P']).sum()[0]
                            except (ValueError, IndexError):
                                header['total_vehicles_positive_direction'] = 0
                            try:
                                header['total_vehicles_negative_direction'] = data['total_vehicles'].groupby(
                                    data['direction'].loc[data['direction'] == 'N']).sum()[0]
                            except (ValueError, IndexError):
                                header['total_vehicles_negative_direction'] = 0
                        else:
                            pass

                        if header['total_heavy_vehicles'].isnull().all():
                            header['total_heavy_vehicles'] = data['total_heavy_vehicles'].sum(
                            )
                            try:
                                header['total_heavy_negative_direction'] = data['total_heavy_vehicles'].groupby(
                                    [data['direction'].loc[data['direction'] == 'N']]).sum()[0]
                            except (ValueError, IndexError):
                                header['total_heavy_negative_direction'] = 0
                            try:
                                header['total_heavy_positive_direction'] = data['total_heavy_vehicles'].groupby(
                                    [data['direction'].loc[data['direction'] == 'P']]).sum()[0]
                            except (ValueError, IndexError):
                                header['total_heavy_positive_direction'] = 0
                            try:
                                header['truck_split_negative_direction'] = data['total_heavy_vehicles'].groupby(
                                    [data['direction'].loc[data['direction'] == 'N']]).sum()[0] / data['total_heavy_vehicles'].sum()
                            except (ValueError, IndexError):
                                header['truck_split_negative_direction'] = 0
                            try:
                                header['truck_split_positive_direction'] = data['total_heavy_vehicles'].groupby(
                                    [data['direction'].loc[data['direction'] == 'P']]).sum()[0] / data['total_heavy_vehicles'].sum()
                            except (ValueError, IndexError):
                                header['truck_split_positive_direction'] = 0
                        else:
                            pass

                        if header['total_light_vehicles'].isnull().all():
                            header['total_light_vehicles'] = data['total_light_vehicles'].sum(
                            )
                            try:
                                header['total_light_positive_direction'] = data['total_light_vehicles'].groupby(
                                    [data['direction'].loc[data['direction'] == 'P']]).sum()[0]
                            except (ValueError, IndexError):
                                header['total_light_positive_direction'] = 0
                            try:
                                header['total_light_negative_direction'] = data['total_light_vehicles'].groupby(
                                    [data['direction'].loc[data['direction'] == 'N']]).sum()[0]
                            except (ValueError, IndexError):
                                header['total_light_negative_direction'] = 0
                        else:
                            pass
                    except KeyError:
                        pass

                    try:
                        header['vehicle_classification_scheme'] = header['vehicle_classification_scheme'].round(
                        ).astype(int)
                    except (KeyError, pd.errors.IntCastingNaNError):
                        pass

                    return header

                elif type == 70:
                    header['total_vehicles'] = data[['number_of_error_vehicles',
                                                     'total_free_flowing_light_vehicles',
                                                     'total_following_light_vehicles',
                                                     'total_free_flowing_heavy_vehicles',
                                                     'total_following_heavy_vehicles']].astype(int).sum()
                    header['total_light_vehicles'] = data[[
                        'total_free_flowing_light_vehicles', 'total_following_light_vehicles']].astype(int).sum()
                    header['total_heavy_vehicles'] = data[[
                        'total_free_flowing_heavy_vehicles', 'total_following_heavy_vehicles']].astype(int).sum()
                    try:
                        header['maximum_gap_milliseconds'] = header['maximum_gap_milliseconds'].round(
                        ).astype(int)
                    except (KeyError, pd.errors.IntCastingNaNError):
                        pass

                    return header

                elif type == 60:
                    return header

                else:
                    return header
        except (IndexError, AttributeError):
            return header

    def select_classification_scheme(self, classification_scheme):
        if int(classification_scheme) == 8:
            vc_df = pd.read_sql_query(
                q.SELECT_CLASSIFICAITON_SCHEME_8, config.ENGINE)
        elif int(classification_scheme) == 1:
            vc_df = pd.read_sql_query(
                q.SELECT_CLASSIFICAITON_SCHEME_1, config.ENGINE)
        elif int(classification_scheme) == 5:
            vc_df = pd.read_sql_query(
                q.SELECT_CLASSIFICAITON_SCHEME_5, config.ENGINE)
        elif int(classification_scheme) == 9:
            vc_df = pd.read_sql_query(
                q.SELECT_CLASSIFICAITON_SCHEME_9, config.ENGINE)
        else:
            vc_df = None
        return vc_df


def merge_summary_dataframes(join_this_df: pd.DataFrame, onto_this_df: pd.DataFrame) -> pd.DataFrame:
    """
    It takes two dataframes, joins them together, and then drops any duplicate columns

    :param join_this_df: The dataframe that you want to join onto the other dataframe
    :type join_this_df: pd.DataFrame
    :param onto_this_df: The dataframe that you want to merge the other dataframe onto
    :type onto_this_df: pd.DataFrame
    :return: A dataframe with the columns from the two dataframes merged.
    """
    onto_this_df = pd.concat([onto_this_df, join_this_df], keys=[
                             "site_id", "start_datetime", "lane_number"], ignore_index=False, axis=1)
    onto_this_df = onto_this_df.droplevel(0, axis=1)
    onto_this_df = onto_this_df.loc[:, ~
                                    onto_this_df.T.duplicated(keep='first')]
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


def get_files(files: str) -> List:
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
    files = os.path.expanduser(config.FILES_COMPLETE)
    try:
        files = pd.read_csv(files, header=None)
        files = files[0].tolist()
    except Exception:
        files = []

    files = [i for i in files if i not in files]

    if not os.path.exists(os.path.expanduser(config.PATH)):
        os.makedirs(os.path.expanduser(config.PATH))

    if not os.path.exists(os.path.expanduser(config.PATH)):
        with open(os.path.expanduser(config.FILES_FAILED), "w",) as f:
            pass
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
        with open(os.path.expanduser(config.FILES_FAILED), "a", newline="",) as f:
            write = csv.writer(f)
            write.writerows([[file]])
        gc.collect()
        pass
    else:
        try:
            lanes = TR.lanes
            site_id = TR.site_id
            header_id = TR.header_id
            data_df = TR.data_df

            # pt_df = pd.DataFrame()

            if lanes is None:
                pass
            else:
                try:
                    lanes = lanes[lanes.columns.intersection(lane_cols)]
                    push_to_db(lanes, config.LANES_TBL_NAME)
                except Exception as exc:
                    traceback.print_exc()
                    with open(os.path.expanduser(config.FILES_FAILED), "a", newline="",) as f:
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
                            columns=config.ELECTRONIC_COUNT_DATA_TYPE21_NAME_CHANGE, inplace=True)
                        data = data[data.columns.intersection(t21_cols)]
                        push_to_db(data, config.TYPE_21_TBL_NAME)
                    except Exception as exc:
                        traceback.print_exc()
                        with open(os.path.expanduser(config.FILES_FAILED), "a", newline="",) as f:
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
                    with open(os.path.expanduser(config.FILES_FAILED), "a", newline="",) as f:
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
                    with open(os.path.expanduser(config.FILES_FAILED), "a", newline="",) as f:
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
                        # pt_df = merge_summary_dataframes(data, pt_df)
                    push_to_db(data, config.TYPE_70_TBL_NAME)
                except Exception as exc:
                    traceback.print_exc()
                    with open(os.path.expanduser(config.FILES_FAILED), "a", newline="",) as f:
                        write = csv.writer(f)
                        write.writerows([[file]])
                    gc.collect()

            if head_df.loc[head_df[0] == "10"].empty:
                pass
            elif data_df.loc[(data_df[0] == "10")].reset_index(drop=True)[0].empty:
                pass
            else:
                try:
                    W = wim.Wim(
                        data=data_df,
                        head_df=head_df,
                        header_id=header_id,
                        site_id=site_id,
                        pt_cols=pt_cols)
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
                    raise Exception("Issue with HEADER "+exc) from exc

            pt_df = pt_df.apply(pd.to_numeric, axis=1, errors='ignore')
            pt_df = pt_df[pt_df.columns.intersection(pt_cols)]
            pt_df['site_id'] = site_id
            push_to_db(pt_df, config.MAIN_TBL_NAME)

            print('DONE WITH : '+file)
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
            with open(os.path.expanduser(config.FILES_FAILED), "a", newline="",) as f:
                write = csv.writer(f)
                write.writerows([[file]])
            gc.collect()


# The above code is running the main function in the multiprocessing module.
if __name__ == "__main__":
    PATH = config.PATH

    # this is for local work only - comment this out when running on the server
    # PATH = r"C:\PQ410"
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

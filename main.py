import os
import csv

import pandas as pd
import numpy as np
pd.options.mode.chained_assignment = None
pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", 20)

from psycopg2.errors import UniqueViolation, NotNullViolation, ExclusionViolation
import psycopg2, psycopg2.extensions

from datetime import timedelta, date
import uuid
import gc
from typing import List
import time

import multiprocessing as mp
import traceback
import tqdm

import config
import queries as q
import tools

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

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

def retry(fn):
    @wraps(fn)
    def wrapper(*args, **kw):
        cls = args[0]
        for x in range(cls._reconnectTries):
            print(x, cls._reconnectTries)
            try:
                return fn(*args, **kw)
            except (psycopg2.InterfaceError, psycopg2.OperationalError) as e:
                print ("\nDatabase Connection [InterfaceError or OperationalError]")
                print ("Idle for %s seconds" % (cls._reconnectIdle))
                time.sleep(cls._reconnectIdle)
                cls._connect()
    return wrapper

class Traffic():

    def __init__(self, file) -> None:
        self.file = file
        self.df = self.to_df()
        self.head_df = self.get_head(self.df)
        self.site_id = self.head_df.loc[self.head_df[0]=="S0", 1].iat[0]
        self.lanes = self.get_lanes(self.head_df)
        self.header_df = self.header(self.head_df)
        self.sum_data_df = self.get_sum_data(self.df)
        self.sum_data_df = self.process_data_datetimes(self.sum_data_df)
        self.sum_data_df = self.get_direction(self.sum_data_df)
        self.indv_data_df = self.get_indv_data(self.df)
        self.t21_table = config.TYPE_21_TBL_NAME
        self.t30_table = config.TYPE_30_TBL_NAME
        self.t70_table = config.TYPE_70_TBL_NAME
        self.t60_table = config.TYPE_60_TBL_NAME

    def to_df(self) -> pd.DataFrame:
        try:
            df = pd.read_csv(self.file, header=None)
            df = df[0].str.split("\s+|,\s+|,|;|;\s+", expand=True)
        except (pd.errors.ParserError, AttributeError, ValueError):
            df = pd.read_csv(self.file, header=None, sep="\s+|\n|\t|,", engine='python')
            df = df[0].str.split("\s+|,\s+|,|;|;\s+", expand=True)
        except Exception as exc:
            print(exc)
        return df

    def get_head(self, df) -> pd.DataFrame:
        try:
            df = pd.DataFrame(df.loc[
                (df[0].isin(["H0", "S0", "I0", "S1", "D0", "D1", "D3", "L0", "L1"]))
                | (
                    (df[0].isin(["21", "70", "30", "13", "60"]))
                    & (df.loc[df[2].isin(["21", "70", "30", "13", "60"])][3].astype(int) < 24)
                )
                | (
                    (df[0].isin(["10"]))
                    & (df[1].isin(["1", "8", "5", "9", "01", "08", "05", "09"]))
                )
            ]).dropna(axis=1, how="all").reset_index(drop=True).copy()
        except KeyError:
            print(self.file)
            traceback.print_exc()
        return df

    def get_sum_data(self, df: pd.DataFrame)-> pd.DataFrame:
        df = pd.DataFrame(df.loc[
            (~df[0].isin(["H0", "H9", "S0", "I0", "S1", "D0", "D1", "D3", "L0", "L1"]))
            & (
                (df[0].isin(["21", "22", "70", "30", "31", "13", "60"]))
                & (df[1].isin(["0", "1", "2", "3", "4"]))
                & ((df.loc[df[3].str.len() > 3]).all()[0])
            )
        ]).dropna(axis=1, how="all").reset_index(drop=True).copy()
        
        return df

    def get_indv_data(self, df: pd.DataFrame)-> pd.DataFrame:
        df = pd.DataFrame(df.loc[
            (~df[0].isin(["H0", "H9", "S0", "I0", "S1", "D0", "D1", "D3", "L0", "L1"]))
            & (
                (df[0].isin(["10"]))
                & (~df[1].isin(["1", "8", "5", "9", "01", "08", "05", "09"]))
                & ((df.loc[df[3].str.len() > 3]).all()[0])
            )
        ]).dropna(axis=1, how="all").reset_index(drop=True).copy()
        
        return df

    def get_lanes(self, df: pd.DataFrame) -> pd.DataFrame:
        if not df.empty:
            lanes = df.loc[
                df[0]=="L1"
                ].dropna(axis=1).drop_duplicates().reset_index(drop=True).copy()
            if lanes.shape[1] == 5:
                lanes.rename(columns={
                    1 : "lane_number",
                    2 : "direction_code",
                    3 : "lane_type_code",
                    4 : "traffic_stream_number"
                },inplace = True)
            elif lanes.shape[1] == 11:
                lanes.rename(columns={
                    1 : "lane_number",
                    2 : "direction_code",
                    3 : "lane_type_code",
                    4 : "traffic_stream_number",
                    5 : "traffic_stream_lane_position",
                    6 : "reverse_direction_lane_number",
                    7 : "vehicle_code",
                    8 : "time_code",
                    9 : "length_code",
                    10 : "speed_code"
                },inplace = True)
            elif lanes.shape[1] == 17:
                lanes.rename(columns={
                    1 : "lane_number",
                    2 : "direction_code",
                    3 : "lane_type_code",
                    4 : "traffic_stream_number",
                    5 : "traffic_stream_lane_position",
                    6 : "reverse_direction_lane_number",
                    7 : "vehicle_code",
                    8 : "time_code",
                    9 : "length_code",
                    10 : "speed_code",
                    11 : "occupancy_time_code",
                    12 : "vehicle_following_code",
                    13 : "trailer_code",
                    14 : "axle_code",
                    15 : "mass_code",
                    16 : "tyre_type_code"
                },inplace = True)
            else:
                lanes.rename(columns={
                    1 : "lane_number",
                    2 : "direction_code",
                    3 : "lane_type_code",
                    4 : "traffic_stream_number"
                },inplace = True)
            lanes[lanes.select_dtypes(include=['object']).columns] = lanes[
                lanes.select_dtypes(include=['object']).columns].apply(
                    pd.to_numeric, axis=1, errors='ignore')
            lanes["site_name"] = self.site_id
        else:
            pass
        return lanes

    def process_data_datetimes(self, df: pd.DataFrame) -> pd.DataFrame:
        date_length = len(df[2].at[0])
        duration_min = int(df[4].at[0])

        # df[2] is date
        # df[3] is time

        df[3] = df[3].astype(str)

        df[3] = df[3].str.pad(width=6,side='right',fillchar="0")
        df[3].loc[df[3].str[:2] == '24'] = ('0').zfill(6)

        if date_length == 6:
            decade = int(df[2].at[0][:2])
            if decade < 50:
                century = str(date.today())[:2]
            else:
                century = int(str(date.today())[:2])-1

            df[2] = str(century) + df[2]
            df[2] = df[2].apply(lambda x: pd.to_datetime(x, format="%Y%m%d").date() + timedelta(days=1)
                if x[3] in ['0'.zfill(6),'24'.ljust(6,'0')] else pd.to_datetime(x, format="%Y%m%d").date())
        elif date_length == 8:
            df[2] = df[2].apply(lambda x: pd.to_datetime(x, format="%Y%m%d").date() + timedelta(days=1)
                if x[3] in ['0'.zfill(6),'24'.ljust(6,'0')] else pd.to_datetime(x, format="%Y%m%d").date())
        else:
            raise Exception("DATA Date length abnormal")

        df[3] = df[3].apply(lambda x: pd.to_datetime(x, format="%H%M%S%f").time())

        try:
            df['end_datetime'] = pd.to_datetime((df[2].astype(str)+df[3].astype(str)), 
                format='%Y-%m-%d%H:%M:%S')
        except ValueError:
            df['end_datetime'] = pd.to_datetime((df[2].astype(str)+df[3].astype(str)), 
                format='%Y-%m-%d%H:%M:%S.%f')

        try:
            df['start_datetime'] = pd.to_datetime((df[2].astype(str)+df[3].astype(str)), 
                format='%Y-%m-%d%H:%M:%S') - timedelta(minutes=duration_min)
        except ValueError:
            df['start_datetime'] = pd.to_datetime((df[2].astype(str)+df[3].astype(str)), 
                format='%Y-%m-%d%H:%M:%S%f') - timedelta(minutes=duration_min)

        df.rename(columns={
            2:"end_date",
            3:"end_time"
        },inplace = True)

        return df

    def get_direction(self, df):
        try:
            dir_1 = self.lanes["lane_number"].astype(int).min()
            dir_2 = self.lanes["lane_number"].astype(int).max()
        except (TypeError,ValueError):
            dir_1 = 0
            dir_2 = 4

        df['direction'] = df[5]
        df['compass_heading'] = df[5]
        df['direction_code'] = df[5]
        df['direction'].loc[df[5].isin(list(self.lanes['lane_number'].astype(str).loc[self.lanes['direction_code']==dir_1]))] = 'P'
        df['direction'].loc[df[5].isin(list(self.lanes['lane_number'].astype(str).loc[self.lanes['direction_code']==dir_2]))] = 'N'
        df['compass_heading'].loc[df[5].isin(list(self.lanes['lane_number'].astype(str).loc[self.lanes['direction_code']==dir_1]))] = str(dir_1)
        df['compass_heading'].loc[df[5].isin(list(self.lanes['lane_number'].astype(str).loc[self.lanes['direction_code']==dir_2]))] = str(dir_2)
        df['direction_code'].loc[df[5].isin(list(self.lanes['lane_number'].astype(str).loc[self.lanes['direction_code']==dir_1]))] = str(dir_1)
        df['direction_code'].loc[df[5].isin(list(self.lanes['lane_number'].astype(str).loc[self.lanes['direction_code']==dir_2]))] = str(dir_2)
        
        return df

    def header(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.reset_index(drop=True)
        st_nd = df.loc[df[0].isin(["D1","D3"]), 0:4].reset_index(drop=True).copy()

        # adds century to year if it is not there
        st_nd[1] = st_nd[1].apply(lambda x: str(date.today())[:2] + x if len(x)==6 else x)
        st_nd[3] = st_nd[3].apply(lambda x: str(date.today())[:2] + x if len(x)==6 else x)

        st_nd[1] = st_nd[1].str.pad(width=8, side='right', fillchar="0")
        st_nd[3] = st_nd[3].str.pad(width=8, side='right', fillchar="0")

        # index 2 and 4 are time, this makes the time uniform length
        st_nd[2] = st_nd[2].str.pad(width=7,side='right',fillchar="0")
        st_nd[4] = st_nd[4].str.pad(width=7,side='right',fillchar="0")

        # this filters for time = 24H00m and makes it zero hour
        st_nd[2].loc[st_nd[2].str[:2] == '24'] = ('0').zfill(7)
        st_nd[4].loc[st_nd[4].str[:2] == '24'] = ('0').zfill(7)

        # adds a day if the hour is zero hour and changes string to dtetime.date
        try:
            st_nd[1] = st_nd[1].apply(lambda x: pd.to_datetime(x, format="%Y%m%d").date() + timedelta(days=1)
            if x[2] in ['0'.zfill(7),'24'.zfill(7)] else pd.to_datetime(x, format="%Y%m%d").date())
            st_nd[3] = st_nd[3].apply(lambda x: pd.to_datetime(x, format="%Y%m%d").date() + timedelta(days=1)
            if x[4] in ['0'.zfill(7),'24'.zfill(7)] else pd.to_datetime(x, format="%Y%m%d").date())
        # except ValueError:
        #     st_nd[1] = st_nd[1].apply(lambda x: pd.to_datetime(x[:8], format="%Y%m%d").date() + timedelta(days=1)
        #     if x[2] in ['0'.zfill(7),'24'.zfill(7)] else pd.to_datetime(x[:8], format="%Y%m%d").date())
        #     st_nd[3] = st_nd[3].apply(lambda x: pd.to_datetime(x[:8], format="%Y%m%d").date() + timedelta(days=1)
        #     if x[4] in ['0'.zfill(7),'24'.zfill(7)] else pd.to_datetime(x[:8], format="%Y%m%d").date())
        except:
            pass

    # changes time string into datetime.time
        try:
            st_nd[2] = st_nd[2].apply(lambda x: pd.to_datetime(x, format="%H%M%S%f").time())
            st_nd[4] = st_nd[4].apply(lambda x: pd.to_datetime(x, format="%H%M%S%f").time())
        except ValueError:
            st_nd[2] = st_nd[2].apply(lambda x: pd.to_datetime(x[:7], format="%H%M%S%f").time())
            st_nd[4] = st_nd[4].apply(lambda x: pd.to_datetime(x[:7], format="%H%M%S%f").time())
        except:
            pass

        # creates start_datetime and end_datetime
        try:
            st_nd["start_datetime"] = pd.to_datetime((st_nd[1].astype(str)+st_nd[2].astype(str)), 
                format='%Y-%m-%d%H:%M:%S')
            st_nd["end_datetime"] = pd.to_datetime((st_nd[3].astype(str)+st_nd[4].astype(str)), 
                format='%Y-%m-%d%H:%M:%S')
        except Exception:
            pass


        st_nd = st_nd.iloc[:,1:].drop_duplicates()

        headers = pd.DataFrame()
        try:
            headers['start_datetime'] = st_nd.groupby(st_nd['start_datetime'].dt.year).min()['start_datetime']
            headers['end_datetime'] = st_nd.groupby(st_nd['end_datetime'].dt.year).max()['end_datetime']
        except:
            pass

        headers['site_id'] = df.loc[df[0] == "S0",1].drop_duplicates().reset_index(drop=True)[0]
        headers['site_id'] = headers['site_id'].astype(str)
        headers['document_url'] = self.file

        headers["header_id"] = ""
        headers["header_id"] = headers["header_id"].apply(
                    lambda x: str(uuid.uuid4()))
        
        headers['year'] = headers['start_datetime'].dt.year

        headers["number_of_lanes"] = int(df.loc[df[0] == "L0", 2].drop_duplicates().reset_index(drop=True)[0])
        
        station_name = df.loc[df[0].isin(["S0"]), 3:].reset_index(drop=True).drop_duplicates().dropna(axis=1)
        headers["station_name"] = station_name[station_name.columns].apply(lambda row: ' '.join(row.values.astype(str)), axis=1)

        t21 = df.loc[df[0]=="21"].dropna(axis=1).drop_duplicates().reset_index().copy()
        t21 = t21.iloc[:,(t21.shape[1])-9:].astype(int)
        t21.columns = range(t21.columns.size)
        t21 = t21.rename(columns={
            0:'speedbin1',
            1:'speedbin2',
            2:'speedbin3',
            3:'speedbin4',
            4:'speedbin5',
            5:'speedbin6',
            6:'speedbin7',
            7:'speedbin8',
            8:'speedbin9'}
            ,inplace = True)

        headers = pd.concat([headers,t21],ignore_index=True,axis=0).bfill().ffill().drop_duplicates()
        
        try:
            headers["type_21_count_interval_minutes"] = int(df.loc[df[0]=="21",1].drop_duplicates().reset_index(drop=True)[0])
            headers["type_30_summary_interval_minutes"] = int(df.loc[df[0]=="21",1].drop_duplicates().reset_index(drop=True)[0])
            headers["type_70_summary_interval_minutes"] = int(df.loc[df[0]=="21",1].drop_duplicates().reset_index(drop=True)[0])
        except KeyError:
            pass
        try:
            headers["type_30_summary_interval_minutes"] = int(df.loc[df[0]=="30",1].drop_duplicates().reset_index(drop=True)[0])
            headers["type_21_count_interval_minutes"] = int(df.loc[df[0]=="30",1].drop_duplicates().reset_index(drop=True)[0])
            headers["type_70_summary_interval_minutes"] = int(df.loc[df[0]=="30",1].drop_duplicates().reset_index(drop=True)[0])
            headers["type_30_vehicle_classification_scheme"] = int(df.loc[df[0] == "30", 3].drop_duplicates().reset_index(drop=True)[0])
        except KeyError:
            pass
        try:
            headers["type_30_summary_interval_minutes"] = int(df.loc[df[0]=="70",1].drop_duplicates().reset_index(drop=True)[0])
            headers["type_21_count_interval_minutes"] = int(df.loc[df[0]=="70",1].drop_duplicates().reset_index(drop=True)[0])
            headers["type_70_summary_interval_minutes"] = int(df.loc[df[0]=="70",1].drop_duplicates().reset_index(drop=True)[0])
        except KeyError:
            pass

        try:
            headers['dir1_id'] = int(df[df[0]=="L1"].dropna(axis=1).drop_duplicates().reset_index(drop=True)[2].min())
            headers['dir2_id'] = int(df[df[0]=="L1"].dropna(axis=1).drop_duplicates().reset_index(drop=True)[2].max())
        except (KeyError, ValueError):
            headers['dir1_id'] = 0
            headers['dir2_id'] = 4

        try:
            headers["type_70_vehicle_classification_scheme"] = int(df.loc[
                df[0] == "70", 2].drop_duplicates().reset_index(drop=True)[0])
            headers["type_70_maximum_gap_milliseconds"] = int(df.loc[
                df[0] == "70", 3].drop_duplicates().reset_index(drop=True)[0])
            headers["type_70_maximum_differential_speed"] = int(df.loc[
                df[0] == "70", 4].drop_duplicates().reset_index(drop=True)[0])
            headers["type_70_error_bin_code"] = int(df.loc[
                df[0] == "70", 5].drop_duplicates().reset_index(drop=True)[0])
        except KeyError:
            pass

        headers = headers.reset_index(drop=True)

        m = headers.select_dtypes(np.number)
        headers[m.columns] = m.round().astype('Int32')
        print(headers.head())

        return headers

    def type_21(self) -> pd.DataFrame:
        if self.sum_data_df is None:
            pass
        else:
            data = self.sum_data_df.loc[(self.sum_data_df[0] == "21")].dropna(
                axis=1, how="all"
            ).reset_index(drop=True).copy()

            if (data[1] == "0").any():
                ddf = data
                # ddf = data.iloc[:, 2:]

                ddf.rename(columns = {
                    4 : "duration_min",
                    5 : "lane_number",
                    6 : "speedbin1",
                    7 : "speedbin2",
                    8 : "speedbin3",
                    9 : "speedbin4",
                    10 : "speedbin5",
                    11 : "speedbin6",
                    12 : "speedbin7",
                    13 : "speedbin8",
                    14 : "speedbin9",
                    15 : "speedbin10",
                    16 : "sum_of_heavy_vehicle_speeds",
                    17 : "short_heavy_vehicles",
                    18 : "medium_heavy_vehicles",
                    19 : "long_heavy_vehicles",
                    20 : "rear_to_rear_headway_shorter_than_2_seconds",
                    21 : "rear_to_rear_headways_shorter_than_programmed_time"
                    }, inplace=True)
                ddf["speedbin0"] = 0

            elif (data[1] == "1").any():
                ddf = data
                # ddf = data.iloc[:, 3:]
                
                ddf.rename(columns = {
                    4 : "duration_min",
                    5 : "lane_number",
                    6 : "speedbin0",
                    7 : "speedbin1",
                    8 : "speedbin2",
                    9 : "speedbin3",
                    10 : "speedbin4",
                    11 : "speedbin5",
                    12 : "speedbin6",
                    13 : "speedbin7",
                    14 : "speedbin8",
                    15 : "speedbin9",
                    16 : "speedbin10",
                    17 : "sum_of_heavy_vehicle_speeds",
                    18 : "short_heavy_vehicles",
                    19 : "medium_heavy_vehicles",
                    20 : "long_heavy_vehicles",
                    21 : "rear_to_rear_headway_shorter_than_2_seconds",
                    22 : "rear_to_rear_headways_shorter_than_programmed_time",
                },inplace=True)
            
            ddf = ddf.fillna(0)

            ddf["duration_min"] = ddf["duration_min"].astype(int)
            ddf["lane_number"] = ddf["lane_number"].astype(int)
            ddf["speedbin0"] = ddf["speedbin0"].astype(int)
            ddf["speedbin1"] = ddf["speedbin1"].astype(int)
            ddf["speedbin2"] = ddf["speedbin2"].astype(int)
            ddf["speedbin3"] = ddf["speedbin3"].astype(int)
            ddf["speedbin4"] = ddf["speedbin4"].astype(int)
            ddf["speedbin5"] = ddf["speedbin5"].astype(int)
            ddf["speedbin6"] = ddf["speedbin6"].astype(int)
            ddf["speedbin7"] = ddf["speedbin7"].astype(int)
            ddf["speedbin8"] = ddf["speedbin8"].astype(int)
            ddf["speedbin9"] = ddf["speedbin9"].astype(int)
            ddf["speedbin10"] = ddf["speedbin10"].astype(int)
            ddf["sum_of_heavy_vehicle_speeds"] = ddf[
                "sum_of_heavy_vehicle_speeds"
            ].astype(int)
            ddf["short_heavy_vehicles"] = ddf["short_heavy_vehicles"].astype(int)
            ddf["medium_heavy_vehicles"] = ddf["medium_heavy_vehicles"].astype(int)
            ddf["long_heavy_vehicles"] = ddf["long_heavy_vehicles"].astype(int)
            ddf["rear_to_rear_headway_shorter_than_2_seconds"] = ddf[
                "rear_to_rear_headway_shorter_than_2_seconds"
            ].astype(int)
            ddf["rear_to_rear_headways_shorter_than_programmed_time"] = ddf[
                "rear_to_rear_headways_shorter_than_programmed_time"
            ].astype(int)

            ddf["total_heavy_vehicles_type21"] = (
                ddf["short_heavy_vehicles"]
                + ddf["medium_heavy_vehicles"]
                + ddf["long_heavy_vehicles"]
            )
            ddf["total_heavy_vehicles_type21"] = ddf[
                "total_heavy_vehicles_type21"
            ].astype(int)

            ddf["total_light_vehicles_type21"] = (
                ddf["speedbin1"]
                + ddf["speedbin2"]
                + ddf["speedbin3"]
                + ddf["speedbin4"]
                + ddf["speedbin5"]
                + ddf["speedbin6"]
                + ddf["speedbin7"]
                + ddf["speedbin8"]
                + ddf["speedbin9"]
                + ddf["speedbin10"]
                - ddf["short_heavy_vehicles"]
                - ddf["medium_heavy_vehicles"]
                - ddf["long_heavy_vehicles"]
            )
            ddf["total_light_vehicles_type21"] = ddf[
                "total_light_vehicles_type21"
            ].astype(int)

            ddf["total_vehicles_type21"] = (
                ddf["speedbin1"]
                + ddf["speedbin2"]
                + ddf["speedbin3"]
                + ddf["speedbin4"]
                + ddf["speedbin5"]
                + ddf["speedbin6"]
                + ddf["speedbin7"]
                + ddf["speedbin8"]
                + ddf["speedbin9"]
                + ddf["speedbin10"]
            )
            ddf["total_vehicles_type21"] = ddf["total_vehicles_type21"].astype(int)

            # ddf["start_datetime"] = pd.to_datetime(str(ddf["start_datetime"]), 
            # format="%Y-%m-%d %H:%M:%S")
            try:
                ddf['year'] = ddf['start_datetime'].dt.year
            except AttributeError:
                ddf['year'] = int(ddf['start_datetime'].str[:4][0])

            ddf["site_id"] = self.site_id

            # ddf = ddf.drop_duplicates()

            return ddf

    def type_30(self) -> pd.DataFrame:
        if self.sum_data_df is None:
            pass
        else:
            data = self.sum_data_df.loc[(self.sum_data_df[0] == "30")].dropna(
                        axis=1, how="all"
                    ).reset_index(drop=True).copy()
            header = self.head_df.loc[(self.head_df[0] == "30")].dropna(
                        axis=1, how="all"
                    ).reset_index(drop=True).copy()

            if header.shape[1] > 3:
                classification_scheme = header.iloc[0,3]
                number_of_data_records = header.iloc[0,4]
            else:
                classification_scheme = header.iloc[0,2]
                number_of_data_records = header.iloc[0,3]

            # vc_df = select_classification_scheme(classification_scheme)

            if data[1].isin(["0", "2", 0, 2]).any():
                ddf = data.iloc[:, 1:].dropna(axis=1, how="all").reset_index(drop=True)

                duration_min = int(ddf[4][0])
                max_lanes = self.lanes['lane_number'].astype(int).max()

                ddf[ddf.select_dtypes(include=['object']).columns] = ddf[
                    ddf.select_dtypes(include=['object']).columns].apply(
                    pd.to_numeric, axis=1, errors='ignore')
                    
                ddf['vehicle_classification_scheme'] = int(classification_scheme)

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
                for lane_no in range(1, max_lanes+1):
                    for i in range(6,int(number_of_data_records)+6):
                        join_to_df3 = ddf.loc[ddf['5'].astype(int) == lane_no, ['1', 'start_datetime','end_date', 'end_time', '4', '5',str(i), 'direction', 'compass_heading']]
                        join_to_df3['class_number'] = i-5
                        join_to_df3.rename(columns={
                            '1':"edit_code",
                            '2':"end_date",
                            '3':"end_time",
                            '4':"duration_of_summary",
                            '5':'lane_number', 
                            str(i): 'number_of_vehicles'
                            }, inplace=True)
                        # df3 = pd.concat([df3,join_to_df3],keys=['start_datetime','lane_number','number_of_vehicles','class_number'],ignore_index=True, axis=0)
                        df3 = pd.concat([df3,join_to_df3],keys=['start_datetime','lane_number'],ignore_index=True, axis=0)
                df3 = df3.apply(pd.to_numeric, axis=1, errors="ignore")
                df3['classification_scheme'] = int(classification_scheme)
                df3['site_id'] = self.site_id
                # df3['year'] = int(df3['start_datetime'].at[0].year)
            else:
                pass
            return df3

    def type_70(self) -> pd.DataFrame:
        if self.sum_data_df is None:
            pass
        else:
            data = self.sum_data_df.loc[(self.sum_data_df[0] == "70")].dropna(
                axis=1, how="all"
            ).reset_index(drop=True).copy()

            if data[1].all() == "1":
                ddf = data.iloc[:, 3:]
                ddf = pd.DataFrame(ddf).dropna(axis=1, how="all").reset_index(drop=True)

                ddf.rename(columns = {
                    4 : "duration_min",
                    5 : "lane_number",
                    6 : "number_of_error_vehicles",
                    7 : "total_free_flowing_light_vehicles",
                    8 : "total_following_light_vehicles",
                    9 : "total_free_flowing_heavy_vehicles",
                    10 : "total_following_heavy_vehicles",
                    11 : "sum_of_inverse_of_speeds_for_free_flowing_lights",
                    12 : "sum_of_inverse_of_speeds_for_following_lights",
                    13 : "sum_of_inverse_of_speeds_for_free_flowing_heavies",
                    14 : "sum_of_inverse_of_speeds_for_following_heavies",
                    15 : "sum_of_speeds_for_free_flowing_lights",
                    16 : "sum_of_speeds_for_following_lights",
                    17 : "sum_of_speeds_for_free_flowing_heavies",
                    18 : "sum_of_speeds_for_following_heavies",
                    19 : "sum_of_squared_speeds_of_free_flowing_lights",
                    20 : "sum_of_squared_speeds_for_following_lights",
                    21 : "sum_of_squared_speeds_of_free_flowing_heavies",
                    22 : "sum_of_squared_speeds_for_following_heavies",
                }, inplace = True)

            else:  # data[1].all() == '0':
                ddf = data.iloc[:, 2:]
                ddf = pd.DataFrame(ddf).dropna(axis=1, how="all").reset_index(drop=True)
                ddf.rename(columns = {
                    4 : "duration_min",
                    5 : "lane_number",
                    6 : "total_free_flowing_light_vehicles",
                    7 : "total_following_light_vehicles",
                    8 : "total_free_flowing_heavy_vehicles",
                    9 : "total_following_heavy_vehicles",
                    10 : "sum_of_inverse_of_speeds_for_free_flowing_lights",
                    11 : "sum_of_inverse_of_speeds_for_following_lights",
                    12 : "sum_of_inverse_of_speeds_for_free_flowing_heavies",
                    13 : "sum_of_inverse_of_speeds_for_following_heavies",
                    14 : "sum_of_speeds_for_free_flowing_lights",
                    15 : "sum_of_speeds_for_following_lights",
                    16 : "sum_of_speeds_for_free_flowing_heavies",
                    17 : "sum_of_speeds_for_following_heavies",
                    18 : "sum_of_squared_speeds_of_free_flowing_lights",
                    19 : "sum_of_squared_speeds_for_following_lights",
                    20 : "sum_of_squared_speeds_of_free_flowing_heavies",
                    21 : "sum_of_squared_speeds_for_following_heavies",
                }, inplace = True)
                ddf["number_of_error_vehicles"] = 0
            
            ddf = ddf.fillna(0)

            ddf["duration_min"] = ddf["duration_min"].astype(int)
            ddf["lane_number"] = ddf["lane_number"].astype(int)
            ddf["number_of_error_vehicles"] = ddf["number_of_error_vehicles"].astype(
                int
            )
            ddf["total_free_flowing_light_vehicles"] = ddf[
                "total_free_flowing_light_vehicles"
            ].astype(int)
            ddf["total_following_light_vehicles"] = ddf[
                "total_following_light_vehicles"
            ].astype(int)
            ddf["total_free_flowing_heavy_vehicles"] = ddf[
                "total_free_flowing_heavy_vehicles"
            ].astype(int)
            ddf["total_following_heavy_vehicles"] = ddf[
                "total_following_heavy_vehicles"
            ].astype(int)
            ddf["sum_of_inverse_of_speeds_for_free_flowing_lights"] = ddf[
                "sum_of_inverse_of_speeds_for_free_flowing_lights"
            ].astype(int)
            ddf["sum_of_inverse_of_speeds_for_following_lights"] = ddf[
                "sum_of_inverse_of_speeds_for_following_lights"
            ].astype(int)
            ddf["sum_of_inverse_of_speeds_for_free_flowing_heavies"] = ddf[
                "sum_of_inverse_of_speeds_for_free_flowing_heavies"
            ].astype(int)
            ddf["sum_of_inverse_of_speeds_for_following_heavies"] = ddf[
                "sum_of_inverse_of_speeds_for_following_heavies"
            ].astype(int)
            ddf["sum_of_speeds_for_free_flowing_lights"] = ddf[
                "sum_of_speeds_for_free_flowing_lights"
            ].astype(int)
            ddf["sum_of_speeds_for_following_lights"] = ddf[
                "sum_of_speeds_for_following_lights"
            ].astype(int)
            ddf["sum_of_speeds_for_free_flowing_heavies"] = ddf[
                "sum_of_speeds_for_free_flowing_heavies"
            ].astype(int)
            ddf["sum_of_speeds_for_following_heavies"] = ddf[
                "sum_of_speeds_for_following_heavies"
            ].astype(int)
            ddf["sum_of_squared_speeds_of_free_flowing_lights"] = ddf[
                "sum_of_squared_speeds_of_free_flowing_lights"
            ].astype(int)
            ddf["sum_of_squared_speeds_for_following_lights"] = ddf[
                "sum_of_squared_speeds_for_following_lights"
            ].astype(int)
            ddf["sum_of_squared_speeds_of_free_flowing_heavies"] = ddf[
                "sum_of_squared_speeds_of_free_flowing_heavies"
            ].astype(int)
            ddf["sum_of_squared_speeds_for_following_heavies"] = ddf[
                "sum_of_squared_speeds_for_following_heavies"
            ].astype(int)

            ddf['year'] = ddf['start_datetime'].dt.year
            ddf["site_id"] = self.site_id

            ddf = ddf.drop_duplicates()

            return ddf   

    def type_60(self) -> pd.DataFrame:
        if self.sum_data_df is None:
            pass
        else:
            data = self.sum_data_df.loc[(self.sum_data_df[0] == "60")].dropna(
                axis=1, how="all"
            ).reset_index(drop=True).copy()
            dfh = self.head_df.loc[(self.head_df[0] == "60")].dropna(
                        axis=1, how="all"
                    ).reset_index(drop=True).copy()

            number_of_data_records = dfh.iloc[0,3]

            if data[1].isin(["0", "1", "2", "3", "4"]).any():
                ddf = data.iloc[:, 1:].reset_index(drop=True)
                ddf = pd.DataFrame(ddf).dropna(axis=1, how="all").reset_index(drop=True)

                ddf[ddf.select_dtypes(include=['object']).columns] = ddf[
                    ddf.select_dtypes(include=['object']).columns
                    ].apply(pd.to_numeric, axis = 1, errors='ignore')

                max_lanes = self.lanes['lane_number'].astype(int).max()

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

                for i in range(6,int(number_of_data_records)+6):
                    for lane_no in range(1, max_lanes+1):
                        join_to_df3 = ddf.loc[ddf['5'].astype(int) == lane_no, [
                            '1', # edit_code
                            'start_datetime',
                            'end_date', # end_date
                            'end_time', # end_time
                            '4', # duration_of_summary
                            '5', # lane_number
                            str(i), # "number_of_vehicles"
                            'direction', 
                            'compass_heading'
                            ]]
                        join_to_df3['bin_number'] = str(i-5)
                        join_to_df3['bin_boundary_length_cm'] = int(dfh[i-2][0])
                        join_to_df3.rename(columns={
                            '1':"edit_code",
                            '4':"duration_of_summary",
                            '5':"lane_number",
                            str(i): "number_of_vehicles"
                            }, inplace=True)
                        df3 = pd.concat([df3,join_to_df3], axis=0, ignore_index=True)
                df3 = df3.apply(pd.to_numeric, axis = 1, errors="ignore")
                df3['site_id'] = self.site_id
                # df3['year'] = int(df3['start_datetime'].at[0].year)
            else:
                pass
            return df3

    def type_10(self) -> pd.DataFrame:
        if self.indv_data_df is None:
            pass
        else:
            data = self.indv_data_df
            # data = self.indv_data_df.loc[(self.indv_data_df[0] == "10")].dropna(
            #     axis=1, how="all"
            # ).reset_index(drop=True).copy()

            num_of_fields = int(data.iloc[:,1].unique()[0])
            ddf = data.iloc[:,: 2 + num_of_fields]

            cols = ['index']
            for i in range(ddf.shape[1]-1):
                cols.append(config.TYPE10_DATA_COLUMN_NAMES[i])
            ddf = pd.DataFrame(ddf.values, columns=cols)
            ddf["data_id"] = ddf.apply(lambda x: uuid.uuid4(), axis=1)

            if data.shape[1] > ddf.shape[1]:
                sub_data_df = pd.DataFrame(columns=['index','sub_data_type_code','offset_sensor_detection_code','mass_measurement_resolution_kg', 'number','value'])
                for index, row in data.iterrows():
                    col = int(row[1]) + 2
                    while col < len(row) and row[col] != None:
                        sub_data_type = row[col]
                        col += 1
                        NoOfType = int(row[col])        
                        col +=1
                        if sub_data_type[0].lower() in ['w','a','g']:
                            odc = row[col]
                            col += 1
                            mmr = row[col]
                            col +=1
                            for i in range(0,NoOfType):
                                tempdf = pd.DataFrame([[index,
                                sub_data_type,
                                odc,
                                mmr,
                                i + 1,
                                row[col]]
                                ], columns = ['index',
                                'sub_data_type_code',
                                'offset_sensor_detection_code',
                                'mass_measurement_resolution_kg',
                                'number',
                                'value'
                                ])
                                sub_data_df = pd.concat([sub_data_df, tempdf])
                                col += 1
                        elif sub_data_type[0].lower() in ['s','t','c']:
                            for i in range(0,NoOfType):
                                tempdf = pd.DataFrame([[index, 
                                sub_data_type,
                                i + 1,
                                row[col]]], columns = ['index' ,
                                'sub_data_type_code',
                                'number',
                                'value'])
                                sub_data_df = pd.concat([sub_data_df, tempdf])
                                col += 1
                        elif sub_data_type[0].lower() in ['v']:
                            odc = row[col]
                            col += 1
                            for i in range(0,NoOfType):
                                tempdf = pd.DataFrame([[index,
                                sub_data_type,
                                odc,
                                i + 1,
                                row[col]]
                                ], columns = ['index',
                                'sub_data_type_code',
                                'offset_sensor_detection_code',
                                'number',
                                'value'
                                ])
                                sub_data_df = pd.concat([sub_data_df, tempdf])
                                col += 1
            else:
                sub_data_df = pd.DataFrame(columns=['index','sub_data_type_code','offset_sensor_detection_code','mass_measurement_resolution_kg', 'number','value'])

            sub_data_df = sub_data_df.merge(ddf[['index', 'data_id']], how='left', on='index')
            
            ddf = ddf.fillna(0)
            ddf["assigned_lane_number"] = ddf["assigned_lane_number"].astype(int)
            ddf["lane_number"] = ddf["physical_lane_number"].astype(int)
            ddf["physical_lane_number"] = ddf["physical_lane_number"].astype(int)
            
            max_lanes = self.lanes['lane_number'].max()

            try:
                dir_1 = self.lanes["lane_number"].astype(int).min()
                dir_2 = self.lanes["lane_number"].astype(int).max()
            except (TypeError,ValueError):
                dir_1 = 0
                dir_2 = 4

            ddf['direction'] = ddf[5]
            ddf['compass_heading'] = ddf[5]
            ddf['direction_code'] = ddf[5]
            ddf['direction'].loc[ddf[5].isin(list(self.lanes['lane_number'].astype(str).loc[self.lanes['direction_code']==dir_1]))] = 'P'
            ddf['direction'].loc[ddf[5].isin(list(self.lanes['lane_number'].astype(str).loc[self.lanes['direction_code']==dir_2]))] = 'N'
            ddf['compass_heading'].loc[ddf[5].isin(list(self.lanes['lane_number'].astype(str).loc[self.lanes['direction_code']==dir_1]))] = str(dir_1)
            ddf['compass_heading'].loc[ddf[5].isin(list(self.lanes['lane_number'].astype(str).loc[self.lanes['direction_code']==dir_2]))] = str(dir_2)
            ddf['direction_code'].loc[ddf[5].isin(list(self.lanes['lane_number'].astype(str).loc[self.lanes['direction_code']==dir_1]))] = str(dir_1)
            ddf['direction_code'].loc[ddf[5].isin(list(self.lanes['lane_number'].astype(str).loc[self.lanes['direction_code']==dir_2]))] = str(dir_2)

            if ddf["departure_date"].map(len).isin([8]).all():
                ddf["start_datetime"] = pd.to_datetime(
                    ddf["departure_date"] + ddf["departure_time"],
                    format="%Y%m%d%H%M%S%f",
                )
            elif ddf["departure_date"].map(len).isin([6]).all():
                ddf["start_datetime"] = pd.to_datetime(
                    ddf["departure_date"] + ddf["departure_time"],
                    format="%y%m%d%H%M%S%f",
                )
            ddf['year'] = ddf['start_datetime'].dt.year
            ddf["site_id"] = self.site_id
            ddf["site_id"] = ddf["site_id"].astype(str)

            ddf['departure_time'] = pd.to_datetime(ddf['departure_time'], format='%H%M%S%f')

            ddf = ddf.drop_duplicates()
            ddf["start_datetime"] = ddf["start_datetime"].astype("datetime64[ns]")

            ddf = ddf.replace(r'^\s*$', np.NaN, regex=True)
            sub_data_df = sub_data_df.replace(r'^\s*$', np.NaN, regex=True)
            sub_data_df = sub_data_df.drop("index", axis=1)

            scols = ddf.select_dtypes('object').columns
            ddf[scols] = ddf[scols].apply(pd.to_numeric, axis=1, errors='ignore')

            ddf = ddf[ddf.columns.intersection(config.TYPE10_DATA_TABLE_COL_LIST)]

            return ddf, sub_data_df

    def header_calcs(self, header: pd.DataFrame, data: pd.DataFrame, type: int) -> pd.DataFrame:
        if (header is None) or (data is None):
            pass
        else:
            try:
                speed_limit_qry = f"select max_speed from trafc.countstation where tcname = '{data['site_id'].iloc[0]}' ;"
                speed_limit = pd.read_sql_query(speed_limit_qry,config.ENGINE).reset_index(drop=True)
                try:
                    speed_limit = speed_limit['max_speed'].iloc[0]
                except IndexError:
                    speed_limit = 60
                data = data.fillna(0)
                if type == 21:
                    try:
                        print(header.head())
                        header['adt_total'] = data['total_vehicles_type21'].groupby([data['start_datetime'].dt.to_period('D')]).sum().mean().round().astype(int)
                        header['adt_positive_direction'] = data['total_vehicles_type21'].groupby([data['start_datetime'].dt.to_period('D'), data['direction'].loc[data['direction'] == 'P']]).sum().mean()
                        header['adt_negative_direction'] = data['total_vehicles_type21'].groupby([data['start_datetime'].dt.to_period('D'), data['direction'].loc[data['direction'] == 'N']]).sum().mean()

                        header['adtt_total'] = data['total_heavy_vehicles_type21'].groupby([data['start_datetime'].dt.to_period('D')]).sum().mean().round().astype(int)
                        header['adtt_positive_direction'] = data['total_vehicles_type21'].groupby([data['start_datetime'].dt.to_period('D'), data['direction'].loc[data['direction'] == 'P']]).sum().mean()
                        header['adtt_negative_direction'] = data['total_vehicles_type21'].groupby([data['start_datetime'].dt.to_period('D'), data['direction'].loc[data['direction'] == 'N']]).sum().mean()

                        header['total_vehicles'] = data['total_vehicles_type21'].sum()
                        header['total_vehicles_positive_direction'] = data['total_vehicles_type21'].groupby(data['direction'].loc[data['direction'] == 'P']).sum()
                        header['total_vehicles_negative_direction'] = data['total_vehicles_type21'].groupby(data['direction'].loc[data['direction'] == 'N']).sum()

                        header['total_heavy_vehicles'] = data['total_heavy_vehicles_type21'].sum()
                        header['total_heavy_negative_direction'] = data['total_heavy_vehicles_type21'].groupby([data['direction'].loc[data['direction'] == 'N']]).sum()
                        header['total_heavy_positive_direction'] = data['total_heavy_vehicles_type21'].groupby([data['direction'].loc[data['direction'] == 'P']]).sum()
                        header['truck_split_negative_direction'] = data['total_heavy_vehicles_type21'].groupby([data['direction'].loc[data['direction'] == 'N']]).sum() / data['total_heavy_vehicles_type21'].sum()
                        header['truck_split_positive_direction'] = data['total_heavy_vehicles_type21'].groupby([data['direction'].loc[data['direction'] == 'P']]).sum() / data['total_heavy_vehicles_type21'].sum()

                        header['total_light_vehicles'] = data['total_light_vehicles_type21'].sum()
                        header['total_light_positive_direction'] = data['total_light_vehicles_type21'].groupby([data['direction'].loc[data['direction'] == 'P']]).sum()
                        header['total_light_negative_direction'] = data['total_light_vehicles_type21'].groupby([data['direction'].loc[data['direction'] == 'N']]).sum()

                        header['short_heavy_vehicles'] = data['short_heavy_vehicles'].sum()
                        header['short_heavy_positive_direction'] = data['short_heavy_vehicles'].groupby([data['direction'].loc[data['direction'] == 'P']]).sum()
                        header['short_heavy_negative_direction'] = data['short_heavy_vehicles'].groupby([data['direction'].loc[data['direction'] == 'N']]).sum()

                        header['Medium_heavy_vehicles'] = data['medium_heavy_vehicles'].sum()
                        header['Medium_heavy_negative_direction'] = data['medium_heavy_vehicles'].groupby([data['direction'].loc[data['direction'] == 'N']]).sum()
                        header['Medium_heavy_positive_direction'] = data['medium_heavy_vehicles'].groupby([data['direction'].loc[data['direction'] == 'P']]).sum()

                        header['long_heavy_vehicles'] = data['long_heavy_vehicles'].sum()
                        header['long_heavy_positive_direction'] = data['long_heavy_vehicles'].groupby([data['direction'].loc[data['direction'] == 'P']]).sum()
                        header['long_heavy_negative_direction'] = data['long_heavy_vehicles'].groupby([data['direction'].loc[data['direction'] == 'N']]).sum()

                        header['vehicles_with_rear_to_rear_headway_less_than_2sec_positive_dire'] = data['rear_to_rear_headway_shorter_than_2_seconds'].groupby([data['direction'].loc[data['direction'] == 'P']]).sum()
                        header['vehicles_with_rear_to_rear_headway_less_than_2sec_negative_dire'] = data['rear_to_rear_headway_shorter_than_2_seconds'].groupby([data['direction'].loc[data['direction'] == 'N']]).sum()
                        header['vehicles_with_rear_to_rear_headway_less_than_2sec_total'] = data['rear_to_rear_headway_shorter_than_2_seconds'].sum()
                    
                        header['type_21_count_interval_minutes'] = data['duration_min'].mean()

                        header['highest_volume_per_hour_positive_direction'] = data['total_vehicles_type21'].groupby([data['start_datetime'].dt.to_period('H'), data['direction'].loc[data['direction'] == 'P']]).sum().max()
                        header['highest_volume_per_hour_negative_direction'] = data['total_vehicles_type21'].groupby([data['start_datetime'].dt.to_period('H'), data['direction'].loc[data['direction'] == 'N']]).sum().max()
                        header['highest_volume_per_hour_total'] = data['total_vehicles_type21'].groupby([data['start_datetime'].dt.to_period('H')]).sum().max()

                        header['15th_highest_volume_per_hour_positive_direction'] = data['total_vehicles_type21'].groupby([data['start_datetime'].dt.to_period('H'), data['direction'].loc[data['direction'] == 'P']]).sum().quantile(q=0.15,  interpolation='linear')
                        header['15th_highest_volume_per_hour_negative_direction'] = data['total_vehicles_type21'].groupby([data['start_datetime'].dt.to_period('H'), data['direction'].loc[data['direction'] == 'N']]).sum().quantile(q=0.15,  interpolation='linear')
                        header['15th_highest_volume_per_hour_total'] = data['total_vehicles_type21'].groupby([data['start_datetime'].dt.to_period('H')]).sum().quantile(q=0.15, interpolation='linear')
                        
                        header['30th_highest_volume_per_hour_positive_direction'] = data['total_vehicles_type21'].groupby([data['start_datetime'].dt.to_period('H'), data['direction'].loc[data['direction'] == 'P']]).sum().quantile(q=0.3,  interpolation='linear')
                        header['30th_highest_volume_per_hour_negative_direction'] = data['total_vehicles_type21'].groupby([data['start_datetime'].dt.to_period('H'), data['direction'].loc[data['direction'] == 'N']]).sum().quantile(q=0.3, interpolation='linear')
                        header['30th_highest_volume_per_hour_total'] = data['total_vehicles_type21'].groupby([data['start_datetime'].dt.to_period('H')]).sum().quantile(q=0.3, interpolation='linear')

                        # header['average_speed_positive_direction'] = 
                        # header['average_speed_negative_direction'] = 
                        try:
                            header['average_speed'] = (int((
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
                            / data['sum_of_heavy_vehicle_speeds'].astype(int).sum()
                            )
                        except TypeError:
                            header['average_speed'] = (((
                            (header['speedbin1'] * data['speedbin1'].astype(int).sum()) +
                            (header['speedbin2'] * data['speedbin2'].astype(int).sum()) +
                            (header['speedbin3'] * data['speedbin3'].astype(int).sum()) +
                            (header['speedbin4'] * data['speedbin4'].astype(int).sum()) +
                            (header['speedbin5'] * data['speedbin5'].astype(int).sum()) +
                            (header['speedbin6'] * data['speedbin6'].astype(int).sum()) +
                            (header['speedbin7'] * data['speedbin7'].astype(int).sum()) +
                            (header['speedbin8'] * data['speedbin8'].astype(int).sum()) +
                            (header['speedbin9'] * data['speedbin9'].astype(int).sum()) 
                            ))
                            / data['sum_of_heavy_vehicle_speeds'].astype(int).sum()
                            )
                        # header['average_speed_light_vehicles_positive_direction'] = 
                        # header['average_speed_light_vehicles_negative_direction'] = 
                        header['average_speed_light_vehicles'] = ((
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
                            / data['sum_of_heavy_vehicle_speeds'].sum()
                            )
                        
                        # header['average_speed_heavy_vehicles_positive_direction'] = 
                        # header['average_speed_heavy_vehicles_negative_direction'] = 
                        # header['average_speed_heavy_vehicles'] = 
                        
                        try:
                            header['truck_split_positive_direction'] = (str(round(data['short_heavy_vehicles'].groupby([data['direction'].loc[data['direction'] == 'P']]).sum() / 
                            data['total_heavy_vehicles_type21'].groupby([data['direction'].loc[data['direction'] == 'P']]).sum()*100)) + ' : ' +
                            str(round(data['medium_heavy_vehicles'].groupby([data['direction'].loc[data['direction'] == 'P']]).sum() / 
                            data['total_heavy_vehicles_type21'].groupby([data['direction'].loc[data['direction'] == 'P']]).sum()*100)) + ' : ' +
                            str(round(data['long_heavy_vehicles'].groupby([data['direction'].loc[data['direction'] == 'P']]).sum() / 
                            data['total_heavy_vehicles_type21'].groupby([data['direction'].loc[data['direction'] == 'P']]).sum()*100))
                            )
                        except ValueError:
                            pass
                        
                        try:
                            header['truck_split_negative_direction'] = (str(round(data['short_heavy_vehicles'].groupby([data['direction'].loc[data['direction'] == 'N']]).sum() / 
                            data['total_heavy_vehicles_type21'].groupby([data['direction'].loc[data['direction'] == 'N']]).sum()*100)) + ' : ' +
                            str(round(data['medium_heavy_vehicles'].groupby([data['direction'].loc[data['direction'] == 'N']]).sum() / 
                            data['total_heavy_vehicles_type21'].groupby([data['direction'].loc[data['direction'] == 'N']]).sum()*100)) + ' : ' +
                            str(round(data['long_heavy_vehicles'].groupby([data['direction'].loc[data['direction'] == 'N']]).sum() / 
                            data['total_heavy_vehicles_type21'].groupby([data['direction'].loc[data['direction'] == 'N']]).sum()*100))
                            )
                        except ValueError:
                            pass
                        try:
                            header['truck_split_total'] = (str(round(data['short_heavy_vehicles'].sum() / 
                            data['total_heavy_vehicles_type21'].sum()*100)) + ' : ' +
                            str(round(data['medium_heavy_vehicles'].sum() / 
                            data['total_heavy_vehicles_type21'].sum()*100)) + ' : ' +
                            str(round(data['long_heavy_vehicles'].sum() / 
                            data['total_heavy_vehicles_type21'].sum()*100))
                            )
                        except ValueError:
                            pass

                    except KeyError:
                        pass
                    try:
                        header["type_21_count_interval_minutes"] = header["type_21_count_interval_minutes"].round().astype(int)
                    except (KeyError, pd.errors.IntCastingNaNError):
                        pass

                    return header
                
                elif type == 30:
                    try:
                        if header['adt_total'].isnull().all():
                            header['adt_total'] = data['total_vehicles_type30'].groupby([data['start_datetime'].dt.to_period('D')]).sum().mean()
                            header['adt_positive_direction'] = data['total_vehicles_type30'].groupby([data['start_datetime'].dt.to_period('D'), data['direction'].loc[data['direction'] == 'P']]).sum().mean()
                            header['adt_negative_direction'] = data['total_vehicles_type30'].groupby([data['start_datetime'].dt.to_period('D'), data['direction'].loc[data['direction'] == 'N']]).sum().mean()
                        else:
                            pass

                        if header['adtt_total'].isnull().all():
                            header['adtt_total'] = data['total_heavy_vehicles_type30'].groupby([data['start_datetime'].dt.to_period('D')]).sum().mean()
                            header['adtt_positive_direction'] = data['total_vehicles_type30'].groupby([data['start_datetime'].dt.to_period('D'), data['direction'].loc[data['direction'] == 'P']]).sum().mean()
                            header['adtt_negative_direction'] = data['total_vehicles_type30'].groupby([data['start_datetime'].dt.to_period('D'), data['direction'].loc[data['direction'] == 'N']]).sum().mean()
                        else:
                            pass

                        if header['total_vehicles'].isnull().all():
                            header['total_vehicles'] = data['total_vehicles_type30'].sum()
                            header['total_vehicles_positive_direction'] = data['total_vehicles_type30'].groupby(data['direction'].loc[data['direction'] == 'P']).sum()
                            header['total_vehicles_negative_direction'] = data['total_vehicles_type30'].groupby(data['direction'].loc[data['direction'] == 'N']).sum()
                        else:
                            pass
                        
                        if header['total_heavy_vehicles'].isnull().all():
                            header['total_heavy_vehicles'] = data['total_heavy_vehicles_type21'].sum()
                            header['total_heavy_negative_direction'] = data['total_heavy_vehicles_type21'].groupby([data['direction'].loc[data['direction'] == 'N']]).sum()
                            header['total_heavy_positive_direction'] = data['total_heavy_vehicles_type21'].groupby([data['direction'].loc[data['direction'] == 'P']]).sum()
                            header['truck_split_negative_direction'] = data['total_heavy_vehicles_type21'].groupby([data['direction'].loc[data['direction'] == 'N']]).sum() / data['total_heavy_vehicles_type21'].sum()
                            header['truck_split_positive_direction'] = data['total_heavy_vehicles_type21'].groupby([data['direction'].loc[data['direction'] == 'P']]).sum() / data['total_heavy_vehicles_type21'].sum()
                        else:
                            pass

                        if header['total_light_vehicles'].isnull().all():
                            header['total_light_vehicles'] = data['total_light_vehicles_type30'].sum()
                            header['total_light_positive_direction'] = data['total_light_vehicles_type30'].groupby([data['direction'].loc[data['direction'] == 'P']]).sum()
                            header['total_light_negative_direction'] = data['total_light_vehicles_type30'].groupby([data['direction'].loc[data['direction'] == 'N']]).sum()
                        else:
                            pass
                    except KeyError:
                        pass
                        
                    try:
                        header['type_30_vehicle_classification_scheme'] = header['type_30_vehicle_classification_scheme'].round().astype(int)
                    except (KeyError, pd.errors.IntCastingNaNError):
                        pass

                    return header

                elif type == 70:

                    try:
                        header['type_70_maximum_gap_milliseconds'] = header['type_70_maximum_gap_milliseconds'].round().astype(int)
                    except (KeyError, pd.errors.IntCastingNaNError):
                        pass
                    
                    return header
                
                elif type == 10:
                    header['total_light_negative_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme']<=1)&(data['direction']=='N')].count()[0].round().astype(int)
                    header['total_light_positive_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme']<=1)&(data['direction']=='P')].count()[0].round().astype(int)
                    header['total_light_vehicles'] = data.loc[data['vehicle_class_code_secondary_scheme']<=1].count()[0].round().astype(int)
                    header['total_heavy_negative_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='N')].count()[0].round().astype(int)
                    header['total_heavy_positive_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='P')].count()[0].round().astype(int)
                    header['total_heavy_vehicles'] = data.loc[data['vehicle_class_code_secondary_scheme']>1].count()[0].round().astype(int)
                    header['total_short_heavy_negative_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='N')].count()[0].round().astype(int)
                    header['total_short_heavy_positive_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='P')].count()[0].round().astype(int)
                    header['total_short_heavy_vehicles'] = data.loc[data['vehicle_class_code_secondary_scheme']==2].count()[0].round().astype(int)
                    header['total_medium_heavy_negative_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='N')].count()[0].round().astype(int)
                    header['total_medium_heavy_positive_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='P')].count()[0].round().astype(int)
                    header['total_medium_heavy_vehicles'] = data.loc[data['vehicle_class_code_secondary_scheme']==3].count()[0].round().astype(int)
                    header['total_long_heavy_negative_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='N')].count()[0].round().astype(int)
                    header['total_long_heavy_positive_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='P')].count()[0].round().astype(int)
                    header['total_long_heavy_vehicles'] = data.loc[data['vehicle_class_code_secondary_scheme']==4].count()[0].round().astype(int)
                    header['total_vehicles_negative_direction'] = data.loc[data['direction']=='N'].count()[0].round().astype(int)
                    header['total_vehicles_positive_direction'] = data.loc[data['direction']=='P'].count()[0].round().astype(int)
                    header['total_vehicles'] = data.count()[0].round().astype(int)
                    header['average_speed_negative_direction'] = data.loc[data['direction']=='N']['vehicle_speed'].mean().round(2)
                    header['average_speed_positive_direction'] = data.loc[data['direction']=='P']['vehicle_speed'].mean().round(2)
                    header['average_speed'] = data['vehicle_speed'].mean().round(2)
                    header['average_speed_light_vehicles_negative_direction'] = data['vehicle_speed'].loc[(data['vehicle_class_code_secondary_scheme']<=1)&(data['direction']=='N')].mean().round(2)
                    header['average_speed_light_vehicles_positive_direction'] = data['vehicle_speed'].loc[(data['vehicle_class_code_secondary_scheme']<=1)&(data['direction']=='P')].mean().round(2)
                    header['average_speed_light_vehicles'] = data['vehicle_speed'].loc[data['vehicle_class_code_secondary_scheme']<=1].mean().round(2)
                    header['average_speed_heavy_vehicles_negative_direction'] = data['vehicle_speed'].loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='N')].mean().round(2)
                    header['average_speed_heavy_vehicles_positive_direction'] = data['vehicle_speed'].loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='P')].mean().round(2)
                    header['average_speed_heavy_vehicles'] = data['vehicle_speed'].loc[data['vehicle_class_code_secondary_scheme']>1].mean().round(2)
                    header['truck_split_negative_direction'] = {str((((data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='N')].count()/data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='N')].count())[0])*100).round().astype(int)) +":"+ str((((data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='N')].count()/data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='N')].count())[0])*100).round().astype(int)) +":"+ str((((data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='N')].count()/data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='N')].count())[0])*100).round().astype(int))}
                    header['truck_split_positive_direction'] = {str((((data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='P')].count()/data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='P')].count())[0])*100).round().astype(int)) +":"+ str((((data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='P')].count()/data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='P')].count())[0])*100).round().astype(int)) +":"+ str((((data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='P')].count()/data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='P')].count())[0])*100).round().astype(int))}
                    header['truck_split_total'] = {str((((data.loc[data['vehicle_class_code_secondary_scheme']==2].count()/data.loc[data['vehicle_class_code_secondary_scheme']>1].count())[0])*100).round().astype(int)) +":"+ str((((data.loc[data['vehicle_class_code_secondary_scheme']==3].count()/data.loc[data['vehicle_class_code_secondary_scheme']>1].count())[0])*100).round().astype(int)) +":"+ str((((data.loc[data['vehicle_class_code_secondary_scheme']==4].count()/data.loc[data['vehicle_class_code_secondary_scheme']>1].count())[0])*100).round().astype(int))}
                    header['estimated_axles_per_truck_negative_direction'] = ((data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='N')].count()[0]*2+data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='N')].count()[0]*5+data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='N')].count()[0]*7)/(data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='N')].count()[0]+data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='N')].count()[0]+data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='N')].count()[0])).round(2)
                    header['estimated_axles_per_truck_positive_direction'] = ((data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='P')].count()[0]*2+data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='P')].count()[0]*5+data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='P')].count()[0]*7)/(data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='P')].count()[0]+data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='P')].count()[0]+data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='P')].count()[0])).round(2)
                    header['estimated_axles_per_truck_total'] = ((data.loc[data['vehicle_class_code_secondary_scheme']==2].count()[0]*2+data.loc[data['vehicle_class_code_secondary_scheme']==3].count()[0]*5+data.loc[data['vehicle_class_code_secondary_scheme']==4].count()[0]*7)/(data.loc[data['vehicle_class_code_secondary_scheme']==2].count()[0]+data.loc[data['vehicle_class_code_secondary_scheme']==3].count()[0]+data.loc[data['vehicle_class_code_secondary_scheme']==4].count()[0])).round(2)
                    header['percentage_speeding_positive_direction'] = ((data.loc[(data['vehicle_speed']>speed_limit)&(data['direction']=='P')].count()[0]/data.loc[data['direction'=='P']].count()[0])*100).round(2)
                    header['percentage_speeding_negative_direction'] = ((data.loc[(data['vehicle_speed']>speed_limit)&(data['direction']=='N')].count()[0]/data.loc[data['direction'=='N']].count()[0])*100).round(2)
                    header['percentage_speeding_total'] = ((data.loc[data['vehicle_speed']>speed_limit].count()[0]/data.count()[0])*100).round(2)
                    header['vehicles_with_rear_to_rear_headway_less_than_2sec_negative_dire'] = data.loc[(data['vehicle_following_code']==2)&data['direction']=='N'].count()[0]
                    header['vehicles_with_rear_to_rear_headway_less_than_2sec_positive_dire'] = data.loc[(data['vehicle_following_code']==2)&data['direction']=='P'].count()[0]
                    header['vehicles_with_rear_to_rear_headway_less_than_2sec_total'] = data.loc[data['vehicle_following_code']==2].count()[0]
                    header['estimated_e80_negative_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='N')].count()[0]*0.6+data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='N')].count()[0]*2.5+data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='N')].count()[0]*2.1
                    header['estimated_e80_positive_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='P')].count()[0]*0.6+data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='P')].count()[0]*2.5+data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='P')].count()[0]*2.1
                    header['estimated_e80_on_road'] = data.loc[data['vehicle_class_code_secondary_scheme']==2].count()[0]*0.6+data.loc[data['vehicle_class_code_secondary_scheme']==3].count()[0]*2.5+data.loc[data['vehicle_class_code_secondary_scheme']==4].count()[0]*2.1
                    header['adt_negative_direction'] = data.loc[data['direction']=='N'].groupby(pd.Grouper(key='start_datetime',freq='D')).count().mean()[0].round().astype(int)
                    header['adt_positive_direction'] = data.loc[data['direction']=='P'].groupby(pd.Grouper(key='start_datetime',freq='D')).count().mean()[0].round().astype(int)
                    header['adt_total'] = data.groupby(pd.Grouper(key='start_datetime',freq='D')).count().mean()[0].round().astype(int)
                    header['adtt_negative_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='N')].groupby(pd.Grouper(key='start_datetime',freq='D')).count().mean()[0].round().astype(int)
                    header['adtt_positive_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='P')].groupby(pd.Grouper(key='start_datetime',freq='D')).count().mean()[0].round().astype(int)
                    header['adtt_total'] = data.loc[data['vehicle_class_code_secondary_scheme']>1].groupby(pd.Grouper(key='start_datetime',freq='D')).count().mean()[0].round().astype(int)
                    header['highest_volume_per_hour_negative_direction'] = data.loc[data['direction']=='N'].groupby(pd.Grouper(key='start_datetime',freq='H')).count().max()[0]
                    header['highest_volume_per_hour_positive_direction'] = data.loc[data['direction']=='P'].groupby(pd.Grouper(key='start_datetime',freq='H')).count().max()[0]
                    header['highest_volume_per_hour_total'] = data.groupby(pd.Grouper(key='start_datetime',freq='H')).count().max()[0]
                    header["15th_highest_volume_per_hour_negative_direction"] = data.loc[data['direction']=='N'].groupby(pd.Grouper(key='start_datetime',freq='D')).count().quantile(0.15)[0].round().astype(int)
                    header["15th_highest_volume_per_hour_positive_direction"] = data.loc[data['direction']=='P'].groupby(pd.Grouper(key='start_datetime',freq='D')).count().quantile(0.15)[0].round().astype(int)
                    header["15th_highest_volume_per_hour_total"] = data.groupby(pd.Grouper(key='start_datetime',freq='D')).count().quantile(0.15)[0].round().astype(int)
                    header["30th_highest_volume_per_hour_negative_direction"] = data.loc[data['direction']=='N'].groupby(pd.Grouper(key='start_datetime',freq='D')).count().quantile(0.30)[0].round().astype(int)
                    header["30th_highest_volume_per_hour_positive_direction"] = data.loc[data['direction']=='P'].groupby(pd.Grouper(key='start_datetime',freq='D')).count().quantile(0.30)[0].round().astype(int)
                    header["30th_highest_volume_per_hour_total"] = data.groupby(pd.Grouper(key='start_datetime',freq='D')).count().quantile(0.30)[0].round().astype(int)
                    header["15th_percentile_speed_negative_direction"] = data.loc[data['direction']=='N']['vehicle_speed'].quantile(0.15).round(2)
                    header["15th_percentile_speed_positive_direction"] = data.loc[data['direction']=='P']['vehicle_speed'].quantile(0.15).round(2)
                    header["15th_percentile_speed_total"] = data['vehicle_speed'].quantile(0.15).round(2)
                    header["85th_percentile_speed_negative_direction"] = data.loc[data['direction']=='N']['vehicle_speed'].quantile(0.85).round(2)
                    header["85th_percentile_speed_positive_direction"] = data.loc[data['direction']=='P']['vehicle_speed'].quantile(0.85).round(2)
                    header["85th_percentile_speed_total"] = data['vehicle_speed'].quantile(0.85).round(2)
                    header['avg_weekday_traffic'] = data.groupby(pd.Grouper(key='start_datetime',freq='B')).count().mean()[0].round().astype(int)
                    header['number_of_days_counted'] = data.groupby([data['start_datetime'].dt.to_period('D')]).count().count()[0]
                    header['duration_hours'] = data.groupby([data['start_datetime'].dt.to_period('H')]).count().count()[0]

                    return header

                elif type == 60:
                    
                    return header
                else:
                    return header
            except IndexError:
                return header

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

class Upsert():
    def __init__(self) -> None:
        self.header_ids = self.get_headers_to_update()

    def get_headers_to_update(self):
        print('fetching headers to update')
        header_ids = pd.read_sql_query(q.GET_HSWIM_HEADER_IDS, config.ENGINE)
        return header_ids

    def main(self):
        for header_id in list(self.header_ids['header_id'].astype(str)):
            SELECT_TYPE10_QRY, AXLE_SPACING_SELECT_QRY, WHEEL_MASS_SELECT_QRY = wim_header_upsert_func1(header_id)
            self.df, self.df2, self.df3 = wim_header_upsert_func2(SELECT_TYPE10_QRY, AXLE_SPACING_SELECT_QRY, WHEEL_MASS_SELECT_QRY)
            if self.df2 is None or self.df3 is None:
                pass
            else:
                print(f'working on {self.header_id}')
                self.self.insert_string = wim_header_upsert(self.header_id, self.df, self.df2, self.df3)
                with config.ENGINE.connect() as conn:
                    print(f'upserting {self.header_id}')
                    conn.execute(self.insert_string)
                    print('COMPLETE')

def merge_summary_dataframes(join_this_df: pd.DataFrame, onto_this_df: pd.DataFrame) -> pd.DataFrame:
    onto_this_df = pd.concat([onto_this_df, join_this_df], keys=["site_id", "start_datetime", "lane_number"], ignore_index=False, axis=1)
    onto_this_df = onto_this_df.droplevel(0, axis=1)
    onto_this_df = onto_this_df.T.drop_duplicates().T
    onto_this_df = onto_this_df.loc[:,~onto_this_df.T.duplicated(keep='first')]
    return onto_this_df
    
def push_to_db(df: pd.DataFrame, table_name: str):
    try:
        df.to_sql(
            table_name,
            con=config.ENGINE,
            schema=config.TRAFFIC_SCHEMA,
            if_exists="append",
            index=False,
            method=tools.psql_insert_copy,
        )
    except (UniqueViolation, NotNullViolation, ExclusionViolation):
        pass

def wim_header_upsert_func1(header_id):
    SELECT_TYPE10_QRY = f"""SELECT * FROM trafc.electronic_count_data_type_10 t10
        left join traf_lu.vehicle_classes_scheme08 c on c.id = t10.vehicle_class_code_primary_scheme
        where t10.header_id = '{header_id}'
        """
    AXLE_SPACING_SELECT_QRY = f"""SELECT 
        t10.id,
        t10.header_id, 
        t10.start_datetime,
        t10.edit_code,
        t10.vehicle_class_code_primary_scheme, 
        t10.vehicle_class_code_secondary_scheme,
        t10.direction,
        t10.axle_count,
        axs.axle_spacing_number,
        axs.axle_spacing_cm
        FROM trafc.electronic_count_data_type_10 t10
        inner join trafc.traffic_e_type10_axle_spacing axs ON axs.type10_id = t10.data_id
        where t10.header_id = '{header_id}'
        """
    WHEEL_MASS_SELECT_QRY = f"""SELECT 
        t10.id,
        t10.header_id, 
        t10.start_datetime,
        t10.edit_code,
        t10.vehicle_class_code_primary_scheme, 
        t10.vehicle_class_code_secondary_scheme,
        t10.direction,
        t10.axle_count,
        wm.wheel_mass_number,
        wm.wheel_mass,
        vm.kg as vehicle_mass_limit_kg,
        sum(wm.wheel_mass*2) over(partition by t10.id) as gross_mass
        FROM trafc.electronic_count_data_type_10 t10
        inner join trafc.traffic_e_type10_wheel_mass wm ON wm.type10_id = t10.data_id
        inner join traf_lu.gross_vehicle_mass_limits vm on vm.number_of_axles = t10.axle_count
        where t10.header_id = '{header_id}'
        """
    return SELECT_TYPE10_QRY, AXLE_SPACING_SELECT_QRY, WHEEL_MASS_SELECT_QRY
    
def wim_header_upsert_func2(SELECT_TYPE10_QRY, AXLE_SPACING_SELECT_QRY, WHEEL_MASS_SELECT_QRY):
    df = pd.read_sql_query(SELECT_TYPE10_QRY,config.ENGINE)
    df = df.fillna(0)
    df2 = pd.read_sql_query(AXLE_SPACING_SELECT_QRY,config.ENGINE)
    df2 = df2.fillna(0)
    df3 = pd.read_sql_query(WHEEL_MASS_SELECT_QRY,config.ENGINE)
    df3 = df3.fillna(0)
    return df, df2, df3
    
def wim_header_upsert(header_id, df, df2, df3):
    try:
        egrl_percent = round((((df.loc[df['edit_code']==2].count()[0])/(df.count()[0]))*100),0) 
    except:
        egrl_percent = 0
    try:
        egrl_percent_positive_direction = round(((df.loc[(df['edit_code']==2)&(df['direction']=='P')].count()[0]/df.loc[df['direction']=='P'].count()[0])*100),0) 
    except:
        egrl_percent_positive_direction = 0
    try:
        egrl_percent_negative_direction = round(((df.loc[(df['edit_code']==2)&(df['direction']=='P')].count()[0]/df.loc[df['direction']=='N'].count()[0])*100),0)  
    except:
        egrl_percent_negative_direction = 0
    try:
        egrw_percent = round((((df2.loc[df2['edit_code']==2].count()[0]+df3.loc[df3['edit_code']==2].count()[0])/df.count()[0])*100),0)   
    except:
        egrw_percent = 0
    try:
        egrw_percent_positive_direction = round((((df2.loc[(df2['edit_code']==2)&(df2['direction']=='P')].count()[0]+df3.loc[(df3['edit_code']==2)&(df3['direction']=='P')].count()[0])/df.loc[df['direction']=='P'].count()[0])*100),0)  
    except:
        egrw_percent_positive_direction = 0
    try:
        egrw_percent_negative_direction = round((((df2.loc[(df2['edit_code']==2)&(df2['direction']=='N')].count()[0]+df3.loc[(df3['edit_code']==2)&(df3['direction']=='N')].count()[0])/df.loc[df['direction']=='N'].count()[0])*100),0)   
    except:
        egrw_percent_negative_direction = 0
    num_weighed = df3.groupby(pd.Grouper(key='id')).count().count()[0] or 0 
    num_weighed_positive_direction = df3.loc[df3['direction']=='P'].groupby(pd.Grouper(key='id')).count().count()[0] or 0  
    num_weighed_negative_direction = df3.loc[df3['direction']=='N'].groupby(pd.Grouper(key='id')).count().count()[0] or 0  
    mean_equivalent_axle_mass = round((df3.groupby(pd.Grouper(key='id'))['wheel_mass'].mean()*2).mean(),2) or 0
    mean_equivalent_axle_mass_positive_direction = round((df3.loc[df3['direction']=='P'].groupby(pd.Grouper(key='id'))['wheel_mass'].mean()*2).mean(),2) or 0
    mean_equivalent_axle_mass_negative_direction = round((df3.loc[df3['direction']=='N'].groupby(pd.Grouper(key='id'))['wheel_mass'].mean()*2).mean(),2) or 0
    mean_axle_spacing = round((df2.groupby(pd.Grouper(key='id')).mean()).mean()['axle_spacing_number'],0) or 0
    mean_axle_spacing_positive_direction = round((df2.loc[df2['direction']=='P'].groupby(pd.Grouper(key='id')).mean()).mean()['axle_spacing_number'],0) or 0
    mean_axle_spacing_negative_direction = round((df2.loc[df2['direction']=='N'].groupby(pd.Grouper(key='id')).mean()).mean()['axle_spacing_number'],0) or 0
    e80_per_axle = ((df3['wheel_mass']*2/8200)**4.2).sum() or 0
    e80_per_axle_positive_direction = ((df3.loc[df3['direction']=='P']['wheel_mass']*2/8200)**4.2).sum() or 0
    e80_per_axle_negative_direction = ((df3.loc[df3['direction']=='N']['wheel_mass']*2/8200)**4.2).sum() or 0
    olhv = len(df3.loc[df3['gross_mass']>df3['vehicle_mass_limit_kg']]['id'].unique()) or 0
    olhv_positive_direction = len(df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])&(df3['direction']=='P')]['id'].unique()) or 0
    olhv_negative_direction = len(df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])&(df3['direction']=='N')]['id'].unique()) or 0
    try:
        olhv_percent = round(((len(df3.loc[df3['gross_mass']>df3['vehicle_mass_limit_kg']]['id'].unique())/len(df3['id'].unique()))*100),2)
    except ZeroDivisionError:
        olhv_percent = 0
    try:
        olhv_percent_positive_direction = round(((len(df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])&(df3['direction']=='P')]['id'].unique())/len(df3.loc[df3['direction']=='P']['id'].unique()))*100),2)
    except ZeroDivisionError:
        olhv_percent_positive_direction = 0
    try:
        olhv_percent_negative_direction = round(((len(df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])&(df3['direction']=='N')]['id'].unique())/len(df3.loc[df3['direction']=='N']['id'].unique()))*100),2)
    except ZeroDivisionError:
        olhv_percent_negative_direction = 0
    tonnage_generated = ((df3['wheel_mass']*2).sum()/1000).round().astype(int) or 0
    tonnage_generated_positive_direction = ((df3.loc[df3['direction']=='P']['wheel_mass']*2).sum()/1000).round().astype(int) or 0
    tonnage_generated_negative_direction = ((df3.loc[df3['direction']=='N']['wheel_mass']*2).sum()/1000).round().astype(int) or 0
    olton = (df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])]['wheel_mass']*2).sum().astype(int) or 0
    olton_positive_direction = (df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])&(df3['direction']=='P')]['wheel_mass']*2).sum().astype(int) or 0
    olton_negative_direction = (df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])&(df3['direction']=='N')]['wheel_mass']*2).sum().astype(int) or 0
    try:
        olton_percent = round(((df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])]['wheel_mass']*2).sum().round(2)/(df3['wheel_mass']*2).sum().round(2))*100,2)
    except ZeroDivisionError:
        olton_percent = 0
    try:
        olton_percent_positive_direction = round(((df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])&(df3['direction']=='P')]['wheel_mass']*2).sum().round(2)/(df3.loc[df3['direction']=='P']['wheel_mass']*2).sum().round(2))*100,2)
    except ZeroDivisionError:
        olton_percent_positive_direction = 0
    try:
        olton_percent_negative_direction = round(((df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])&(df3['direction']=='N')]['wheel_mass']*2).sum().round(2)/(df3.loc[df3['direction']=='N']['wheel_mass']*2).sum().round(2))*100,2)
    except ZeroDivisionError:
        olton_percent_negative_direction = 0
    ole80 = round(((df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])]['wheel_mass']*2/8200)**4.2).sum(),0) or 0
    ole80_positive_direction = round(((df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])&(df3['direction']=='P')]['wheel_mass']*2/8200)**4.2).sum(),0) or 0
    ole80_negative_direction = round(((df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])&(df3['direction']=='N')]['wheel_mass']*2/8200)**4.2).sum(),0) or 0
    try:
        ole80_percent = round((round(((df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])]['wheel_mass']*2/8200)**4.2).sum(),0)/round(round(((df3['wheel_mass']*2/8200)**4.2).sum(),0)))*100,2)
    except ZeroDivisionError:
        ole80_percent = 0
    try:
        ole80_percent_positive_direction = ((((df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])&(df3['direction']=='P')]['wheel_mass']*2/8200)**4.2).sum().round()/((df3.loc[df3['direction']=='P']['wheel_mass']*2/8200)**4.2).sum().round())*100).round(2)
    except ZeroDivisionError:
        ole80_percent_positive_direction = 0
    try:
        ole80_percent_negative_direction = ((((df3.loc[(df3['gross_mass']>df3['vehicle_mass_limit_kg'])&(df3['direction']=='N')]['wheel_mass']*2/8200)**4.2).sum().round()/((df3.loc[df3['direction']=='N']['wheel_mass']*2/8200)**4.2).sum().round())*100).round(2)
    except ZeroDivisionError:
        ole80_percent_negative_direction = 0
    xe80 = round(((df3['wheel_mass']*2/8200)**4.2).sum()-(((df3['wheel_mass']*2*0.05)/8200)**4.2).sum(), 2) or 0
    xe80_positive_direction = round(((df3.loc[df3['direction']=='P']['wheel_mass']*2/8200)**4.2).sum()-(((df3.loc[df3['direction']=='P']['wheel_mass']*2*0.05)/8200)**4.2).sum(),2) or 0
    xe80_negative_direction = round(((df3.loc[df3['direction']=='N']['wheel_mass']*2/8200)**4.2).sum()-(((df3.loc[df3['direction']=='N']['wheel_mass']*2*0.05)/8200)**4.2).sum(),2) or 0
    try:
        xe80_percent = (((((df3['wheel_mass']*2/8200)**4.2).sum()-(((df3['wheel_mass']*2*0.05)/8200)**4.2).sum())/((df3['wheel_mass']*2/8200)**4.2).sum())*100).round()
    except ZeroDivisionError:
        xe80_percent = 0
    try:
        xe80_percent_positive_direction = (((((df3.loc[df3['direction']=='P']['wheel_mass']*2/8200)**4.2).sum()-(((df3.loc[df3['direction']=='P']['wheel_mass']*2*0.05)/8200)**4.2).sum())/((df3.loc[df3['direction']=='P']['wheel_mass']*2/8200)**4.2).sum())*100).round()
    except ZeroDivisionError:
        xe80_percent_positive_direction = 0
    try:
        xe80_percent_negative_direction = (((((df3.loc[df3['direction']=='N']['wheel_mass']*2/8200)**4.2).sum()-(((df3.loc[df3['direction']=='N']['wheel_mass']*2*0.05)/8200)**4.2).sum())/((df3.loc[df3['direction']=='N']['wheel_mass']*2/8200)**4.2).sum())*100).round()
    except ZeroDivisionError:
        xe80_percent_negative_direction = 0
    try:
        e80_per_day = ((((df3['wheel_mass']*2/8200)**4.2).sum().round()/df3.groupby(pd.Grouper(key='start_datetime',freq='D')).count().count()[0])*100).round(2)
    except ZeroDivisionError:
        e80_per_day = 0
    try:
        e80_per_day_positive_direction = ((((df3.loc[df3['direction']=='P']['wheel_mass']*2/8200)**4.2).sum().round()/df3.loc[df3['direction']=='P'].groupby(pd.Grouper(key='start_datetime',freq='D')).count().count()[0])*100).round(2)
    except ZeroDivisionError:
        e80_per_day_positive_direction = 0
    try:
        e80_per_day_negative_direction = ((((df3.loc[df3['direction']=='N']['wheel_mass']*2/8200)**4.2).sum().round()/df3.loc[df3['direction']=='N'].groupby(pd.Grouper(key='start_datetime',freq='D')).count().count()[0])*100).round(2)
    except ZeroDivisionError:
        e80_per_day_negative_direction = 0
    try:
        e80_per_heavy_vehicle = ((((df3.loc[df3['vehicle_class_code_primary_scheme']>3]['wheel_mass']*2/8200)**4.2).sum().round()/df3.loc[df3['vehicle_class_code_primary_scheme']>3].count()[0])*100).round(2)
    except ZeroDivisionError:
        e80_per_heavy_vehicle = 0
    try:
        e80_per_heavy_vehicle_positive_direction = ((((df3.loc[(df3['vehicle_class_code_primary_scheme']>3)&(df3['direction']=='P')]['wheel_mass']*2/8200)**4.2).sum().round()/df3.loc[(df3['vehicle_class_code_primary_scheme']>3)&(df3['direction']=='P')].count()[0])*100).round(2)
    except ZeroDivisionError:
        e80_per_heavy_vehicle_positive_direction = 0
    try:
        e80_per_heavy_vehicle_negative_direction = ((((df3.loc[(df3['vehicle_class_code_primary_scheme']>3)&(df3['direction']=='N')]['wheel_mass']*2/8200)**4.2).sum().round()/df3.loc[(df3['vehicle_class_code_primary_scheme']>3)&(df3['direction']=='N')].count()[0])*100).round(2)
    except ZeroDivisionError:
        e80_per_heavy_vehicle_negative_direction = 0
    worst_steering_single_axle_cnt = 0
    worst_steering_single_axle_olhv_perc = 0
    worst_steering_single_axle_tonperhv = 0
    worst_steering_double_axle_cnt = 0
    worst_steering_double_axle_olhv_perc = 0
    worst_steering_double_axle_tonperhv = 0
    worst_non_steering_single_axle_cnt = 0
    worst_non_steering_single_axle_olhv_perc = 0
    worst_non_steering_single_axle_tonperhv = 0
    worst_non_steering_double_axle_cnt = 0
    worst_non_steering_double_axle_olhv_perc = 0
    worst_non_steering_double_axle_tonperhv = 0
    worst_triple_axle_cnt = 0
    worst_triple_axle_olhv_perc = 0
    worst_triple_axle_tonperhv = 0
    bridge_formula_cnt = round((18000 + 2.1 * (df2.loc[df2['axle_spacing_number']>1].groupby('id')['axle_spacing_cm'].sum().mean())),2) or 0
    bridge_formula_olhv_perc = 0
    bridge_formula_tonperhv = 0
    gross_formula_cnt = 0
    gross_formula_olhv_perc = 0
    gross_formula_tonperhv = 0
    total_avg_cnt = df.loc[df['group']=='Heavy'].count()[0]
    total_avg_olhv_perc = 0
    total_avg_tonperhv = round(((df3['wheel_mass']*2).sum()/1000)/df.loc[df['group']=='Heavy'].count()[0],2)
    worst_steering_single_axle_cnt_positive_direciton = 0
    worst_steering_single_axle_olhv_perc_positive_direciton = 0
    worst_steering_single_axle_tonperhv_positive_direciton = 0
    worst_steering_double_axle_cnt_positive_direciton = 0
    worst_steering_double_axle_olhv_perc_positive_direciton = 0
    worst_steering_double_axle_tonperhv_positive_direciton = 0
    worst_non_steering_single_axle_cnt_positive_direciton = 0
    worst_non_steering_single_axle_olhv_perc_positive_direciton = 0
    worst_non_steering_single_axle_tonperhv_positive_direciton = 0
    worst_non_steering_double_axle_cnt_positive_direciton = 0
    worst_non_steering_double_axle_olhv_perc_positive_direciton = 0
    worst_non_steering_double_axle_tonperhv_positive_direciton = 0
    worst_triple_axle_cnt_positive_direciton = 0
    worst_triple_axle_olhv_perc_positive_direciton = 0
    worst_triple_axle_tonperhv_positive_direciton = 0
    bridge_formula_cnt_positive_direciton = round((18000 + 2.1 * (df2.loc[(df2['axle_spacing_number']>1)&(df2['direction']=='P')].groupby('id')['axle_spacing_cm'].sum().mean())),2) or 0
    bridge_formula_olhv_perc_positive_direciton = 0
    bridge_formula_tonperhv_positive_direciton = 0
    gross_formula_cnt_positive_direciton = 0
    gross_formula_olhv_perc_positive_direciton = 0
    gross_formula_tonperhv_positive_direciton = 0
    total_avg_cnt_positive_direciton = df.loc[(df['group']=='Heavy')&(df['direction']=='P')].count()[0]
    total_avg_olhv_perc_positive_direciton = 0
    total_avg_tonperhv_positive_direciton = round(((df3.loc[df3['direction']=='P']['wheel_mass']*2).sum()/1000)/df.loc[df['group']=='Heavy'].count()[0],2)
    worst_steering_single_axle_cnt_negative_direciton = 0
    worst_steering_single_axle_olhv_perc_negative_direciton = 0
    worst_steering_single_axle_tonperhv_negative_direciton = 0
    worst_steering_double_axle_cnt_negative_direciton = 0
    worst_steering_double_axle_olhv_perc_negative_direciton = 0
    worst_steering_double_axle_tonperhv_negative_direciton = 0
    worst_non_steering_single_axle_cnt_negative_direciton = 0
    worst_non_steering_single_axle_olhv_perc_negative_direciton = 0
    worst_non_steering_single_axle_tonperhv_negative_direciton = 0
    worst_non_steering_double_axle_cnt_negative_direciton = 0
    worst_non_steering_double_axle_olhv_perc_negative_direciton = 0
    worst_non_steering_double_axle_tonperhv_negative_direciton = 0
    worst_triple_axle_cnt_negative_direciton = 0
    worst_triple_axle_olhv_perc_negative_direciton = 0
    worst_triple_axle_tonperhv_negative_direciton = 0
    bridge_formula_cnt_negative_direciton = round((18000 + 2.1 * (df2.loc[(df2['axle_spacing_number']>1)&(df2['direction']=='P')].groupby('id')['axle_spacing_cm'].sum().mean())),2) or 0
    bridge_formula_olhv_perc_negative_direciton = 0
    bridge_formula_tonperhv_negative_direciton = 0
    gross_formula_cnt_negative_direciton = 0
    gross_formula_olhv_perc_negative_direciton = 0
    gross_formula_tonperhv_negative_direciton = 0
    total_avg_cnt_negative_direciton = df.loc[(df['group']=='Heavy')&(df['direction']=='N')].count()[0]
    total_avg_olhv_perc_negative_direciton = 0
    total_avg_tonperhv_negative_direciton = round(((df3.loc[df3['direction']=='N']['wheel_mass']*2).sum()/1000)/df.loc[df['group']=='Heavy'].count()[0],2)
    wst_2_axle_busses_cnt_pos_dir = 0
    wst_2_axle_6_tyre_single_units_cnt_pos_dir = 0
    wst_busses_with_3_or_4_axles_cnt_pos_dir = 0
    wst_2_axle_6tyre_su_with_trailer_4axles_max_cnt_pos_dir = 0
    wst_3_axle_su_incl_single_axle_trailer_cnt_pos_dir = 0
    wst_4_or_less_axle_incl_a_single_trailer_cnt_pos_dir = 0
    wst_busses_with_5_or_more_axles_cnt_pos_dir = 0
    wst_3_axle_su_and_trailer_more_than_4_axles_cnt_pos_dir = 0
    wst_5_axle_single_trailer_cnt_pos_dir = 0
    wst_6_axle_single_trailer_cnt_pos_dir = 0
    wst_5_or_less_axle_multi_trailer_cnt_pos_dir = 0
    wst_6_axle_multi_trailer_cnt_pos_dir = 0
    wst_7_axle_multi_trailer_cnt_pos_dir = 0
    wst_8_or_more_axle_multi_trailer_cnt_pos_dir = 0
    wst_2_axle_busses_olhv_perc_pos_dir = 0
    wst_2_axle_6_tyre_single_units_olhv_perc_pos_dir = 0
    wst_busses_with_3_or_4_axles_olhv_perc_pos_dir = 0
    wst_2_axle_6tyre_su_with_trailer_4axles_max_olhv_perc_pos_dir = 0
    wst_3_axle_su_incl_single_axle_trailer_olhv_perc_pos_dir = 0
    wst_4_or_less_axle_incl_a_single_trailer_olhv_perc_pos_dir = 0
    wst_busses_with_5_or_more_axles_olhv_perc_pos_dir = 0
    wst_3_axle_su_and_trailer_more_than_4_axles_olhv_perc_pos_dir = 0
    wst_5_axle_single_trailer_olhv_perc_pos_dir = 0
    wst_6_axle_single_trailer_olhv_perc_pos_dir = 0
    wst_5_or_less_axle_multi_trailer_olhv_perc_pos_dir = 0
    wst_6_axle_multi_trailer_olhv_perc_pos_dir = 0
    wst_7_axle_multi_trailer_olhv_perc_pos_dir = 0
    wst_8_or_more_axle_multi_trailer_olhv_perc_pos_dir = 0
    wst_2_axle_busses_tonperhv_pos_dir = 0
    wst_2_axle_6_tyre_single_units_tonperhv_pos_dir = 0
    wst_busses_with_3_or_4_axles_tonperhv_pos_dir = 0
    wst_2_axle_6tyre_su_with_trailer_4axles_max_tonperhv_pos_dir = 0
    wst_3_axle_su_incl_single_axle_trailer_tonperhv_pos_dir = 0
    wst_4_or_less_axle_incl_a_single_trailer_tonperhv_pos_dir = 0
    wst_busses_with_5_or_more_axles_tonperhv_pos_dir = 0
    wst_3_axle_su_and_trailer_more_than_4_axles_tonperhv_pos_dir = 0
    wst_5_axle_single_trailer_tonperhv_pos_dir = 0
    wst_6_axle_single_trailer_tonperhv_pos_dir = 0
    wst_5_or_less_axle_multi_trailer_tonperhv_pos_dir = 0
    wst_6_axle_multi_trailer_tonperhv_pos_dir = 0
    wst_7_axle_multi_trailer_tonperhv_pos_dir = 0
    wst_8_or_more_axle_multi_trailer_tonperhv_pos_dir = 0
    wst_2_axle_busses_cnt_neg_dir = 0
    wst_2_axle_6_tyre_single_units_cnt_neg_dir = 0
    wst_busses_with_3_or_4_axles_cnt_neg_dir = 0
    wst_2_axle_6tyre_su_with_trailer_4axles_max_cnt_neg_dir = 0
    wst_3_axle_su_incl_single_axle_trailer_cnt_neg_dir = 0
    wst_4_or_less_axle_incl_a_single_trailer_cnt_neg_dir = 0
    wst_busses_with_5_or_more_axles_cnt_neg_dir = 0
    wst_3_axle_su_and_trailer_more_than_4_axles_cnt_neg_dir = 0
    wst_5_axle_single_trailer_cnt_neg_dir = 0
    wst_6_axle_single_trailer_cnt_neg_dir = 0
    wst_5_or_less_axle_multi_trailer_cnt_neg_dir = 0
    wst_6_axle_multi_trailer_cnt_neg_dir = 0
    wst_7_axle_multi_trailer_cnt_neg_dir = 0
    wst_8_or_more_axle_multi_trailer_cnt_neg_dir = 0
    wst_2_axle_busses_olhv_perc_neg_dir = 0
    wst_2_axle_6_tyre_single_units_olhv_perc_neg_dir = 0
    wst_busses_with_3_or_4_axles_olhv_perc_neg_dir = 0
    wst_2_axle_6tyre_su_with_trailer_4axles_max_olhv_perc_neg_dir = 0
    wst_3_axle_su_incl_single_axle_trailer_olhv_perc_neg_dir = 0
    wst_4_or_less_axle_incl_a_single_trailer_olhv_perc_neg_dir = 0
    wst_busses_with_5_or_more_axles_olhv_perc_neg_dir = 0
    wst_3_axle_su_and_trailer_more_than_4_axles_olhv_perc_neg_dir = 0
    wst_5_axle_single_trailer_olhv_perc_neg_dir = 0
    wst_6_axle_single_trailer_olhv_perc_neg_dir = 0
    wst_5_or_less_axle_multi_trailer_olhv_perc_neg_dir = 0
    wst_6_axle_multi_trailer_olhv_perc_neg_dir = 0
    wst_7_axle_multi_trailer_olhv_perc_neg_dir = 0
    wst_8_or_more_axle_multi_trailer_olhv_perc_neg_dir = 0
    wst_2_axle_busses_tonperhv_neg_dir = 0
    wst_2_axle_6_tyre_single_units_tonperhv_neg_dir = 0
    wst_busses_with_3_or_4_axles_tonperhv_neg_dir = 0
    wst_2_axle_6tyre_su_with_trailer_4axles_max_tonperhv_neg_dir = 0
    wst_3_axle_su_incl_single_axle_trailer_tonperhv_neg_dir = 0
    wst_4_or_less_axle_incl_a_single_trailer_tonperhv_neg_dir = 0
    wst_busses_with_5_or_more_axles_tonperhv_neg_dir = 0
    wst_3_axle_su_and_trailer_more_than_4_axles_tonperhv_neg_dir = 0
    wst_5_axle_single_trailer_tonperhv_neg_dir = 0
    wst_6_axle_single_trailer_tonperhv_neg_dir = 0
    wst_5_or_less_axle_multi_trailer_tonperhv_neg_dir = 0
    wst_6_axle_multi_trailer_tonperhv_neg_dir = 0
    wst_7_axle_multi_trailer_tonperhv_neg_dir = 0
    wst_8_or_more_axle_multi_trailer_tonperhv_neg_dir = 0
    wst_2_axle_busses_cnt = 0
    wst_2_axle_6_tyre_single_units_cnt = 0
    wst_busses_with_3_or_4_axles_cnt = 0
    wst_2_axle_6tyre_su_with_trailer_4axles_max_cnt = 0
    wst_3_axle_su_incl_single_axle_trailer_cnt = 0
    wst_4_or_less_axle_incl_a_single_trailer_cnt = 0
    wst_busses_with_5_or_more_axles_cnt = 0
    wst_3_axle_su_and_trailer_more_than_4_axles_cnt = 0
    wst_5_axle_single_trailer_cnt = 0
    wst_6_axle_single_trailer_cnt = 0
    wst_5_or_less_axle_multi_trailer_cnt = 0
    wst_6_axle_multi_trailer_cnt = 0
    wst_7_axle_multi_trailer_cnt = 0
    wst_8_or_more_axle_multi_trailer_cnt = 0
    wst_2_axle_busses_olhv_perc = 0
    wst_2_axle_6_tyre_single_units_olhv_perc = 0
    wst_busses_with_3_or_4_axles_olhv_perc = 0
    wst_2_axle_6tyre_su_with_trailer_4axles_max_olhv_perc = 0
    wst_3_axle_su_incl_single_axle_trailer_olhv_perc = 0
    wst_4_or_less_axle_incl_a_single_trailer_olhv_perc = 0
    wst_busses_with_5_or_more_axles_olhv_perc = 0
    wst_3_axle_su_and_trailer_more_than_4_axles_olhv_perc = 0
    wst_5_axle_single_trailer_olhv_perc = 0
    wst_6_axle_single_trailer_olhv_perc = 0
    wst_5_or_less_axle_multi_trailer_olhv_perc = 0
    wst_6_axle_multi_trailer_olhv_perc = 0
    wst_7_axle_multi_trailer_olhv_perc = 0
    wst_8_or_more_axle_multi_trailer_olhv_perc = 0
    wst_2_axle_busses_tonperhv = 0
    wst_2_axle_6_tyre_single_units_tonperhv = 0
    wst_busses_with_3_or_4_axles_tonperhv = 0
    wst_2_axle_6tyre_su_with_trailer_4axles_max_tonperhv = 0
    wst_3_axle_su_incl_single_axle_trailer_tonperhv = 0
    wst_4_or_less_axle_incl_a_single_trailer_tonperhv = 0
    wst_busses_with_5_or_more_axles_tonperhv = 0
    wst_3_axle_su_and_trailer_more_than_4_axles_tonperhv = 0
    wst_5_axle_single_trailer_tonperhv = 0
    wst_6_axle_single_trailer_tonperhv = 0
    wst_5_or_less_axle_multi_trailer_tonperhv = 0
    wst_6_axle_multi_trailer_tonperhv = 0
    wst_7_axle_multi_trailer_tonperhv = 0
    wst_8_or_more_axle_multi_trailer_tonperhv = 0

    UPSERT_STRING = F"""INSERT INTO 
            Trafc.electronic_count_header_hswim (
        header_id,
        egrl_percent,
        egrw_percent,
        mean_equivalent_axle_mass,
        mean_equivalent_axle_mass_positive_direction,
        mean_equivalent_axle_mass_negative_direction,
        mean_axle_spacing,
        mean_axle_spacing_positive_direction,
        mean_axle_spacing_negative_direction,
        e80_per_axle,
        e80_per_axle_positive_direction,
        e80_per_axle_negative_direction,
        olhv,
        olhv_positive_direction,
        olhv_negative_direction,
        olhv_percent,
        olhv_percent_positive_direction,
        olhv_percent_negative_direction,
        tonnage_generated,
        tonnage_generated_positive_direction,
        tonnage_generated_negative_direction,
        olton,
        olton_positive_direction,
        olton_negative_direction,
        olton_percent,
        olton_percent_positive_direction,
        olton_percent_negative_direction,
        ole80,
        ole80_positive_direction,
        ole80_negative_direction,
        ole80_percent,
        ole80_percent_positive_direction,
        ole80_percent_negative_direction,
        xe80,
        xe80_positive_direction,
        xe80_negative_direction,
        xe80_percent,
        xe80_percent_positive_direction,
        xe80_percent_negative_direction,
        e80_per_day,
        e80_per_day_positive_direction,
        e80_per_day_negative_direction,
        e80_per_heavy_vehicle,
        e80_per_heavy_vehicle_positive_direction,
        e80_per_heavy_vehicle_negative_direction,
        worst_steering_single_axle_cnt,
        worst_steering_single_axle_olhv_perc,
        worst_steering_single_axle_tonperhv,
        worst_steering_double_axle_cnt,
        worst_steering_double_axle_olhv_perc,
        worst_steering_double_axle_tonperhv,
        worst_non_steering_single_axle_cnt,
        worst_non_steering_single_axle_olhv_perc,
        worst_non_steering_single_axle_tonperhv,
        worst_non_steering_double_axle_cnt,
        worst_non_steering_double_axle_olhv_perc,
        worst_non_steering_double_axle_tonperhv,
        worst_triple_axle_cnt,
        worst_triple_axle_olhv_perc,
        worst_triple_axle_tonperhv,
        bridge_formula_cnt,
        bridge_formula_olhv_perc,
        bridge_formula_tonperhv,
        gross_formula_cnt,
        gross_formula_olhv_perc,
        gross_formula_tonperhv,
        total_avg_cnt,
        total_avg_olhv_perc,
        total_avg_tonperhv,
        worst_steering_single_axle_cnt_positive_direciton,
        worst_steering_single_axle_olhv_perc_positive_direciton,
        worst_steering_single_axle_tonperhv_positive_direciton,
        worst_steering_double_axle_cnt_positive_direciton,
        worst_steering_double_axle_olhv_perc_positive_direciton,
        worst_steering_double_axle_tonperhv_positive_direciton,
        worst_non_steering_single_axle_cnt_positive_direciton,
        worst_non_steering_single_axle_olhv_perc_positive_direciton,
        worst_non_steering_single_axle_tonperhv_positive_direciton,
        worst_non_steering_double_axle_cnt_positive_direciton,
        worst_non_steering_double_axle_olhv_perc_positive_direciton,
        worst_non_steering_double_axle_tonperhv_positive_direciton,
        worst_triple_axle_cnt_positive_direciton,
        worst_triple_axle_olhv_perc_positive_direciton,
        worst_triple_axle_tonperhv_positive_direciton,
        bridge_formula_cnt_positive_direciton,
        bridge_formula_olhv_perc_positive_direciton,
        bridge_formula_tonperhv_positive_direciton,
        gross_formula_cnt_positive_direciton,
        gross_formula_olhv_perc_positive_direciton,
        gross_formula_tonperhv_positive_direciton,
        total_avg_cnt_positive_direciton,
        total_avg_olhv_perc_positive_direciton,
        total_avg_tonperhv_positive_direciton,
        worst_steering_single_axle_cnt_negative_direciton,
        worst_steering_single_axle_olhv_perc_negative_direciton,
        worst_steering_single_axle_tonperhv_negative_direciton,
        worst_steering_double_axle_cnt_negative_direciton,
        worst_steering_double_axle_olhv_perc_negative_direciton,
        worst_steering_double_axle_tonperhv_negative_direciton,
        worst_non_steering_single_axle_cnt_negative_direciton,
        worst_non_steering_single_axle_olhv_perc_negative_direciton,
        worst_non_steering_single_axle_tonperhv_negative_direciton,
        worst_non_steering_double_axle_cnt_negative_direciton,
        worst_non_steering_double_axle_olhv_perc_negative_direciton,
        worst_non_steering_double_axle_tonperhv_negative_direciton,
        worst_triple_axle_cnt_negative_direciton,
        worst_triple_axle_olhv_perc_negative_direciton,
        worst_triple_axle_tonperhv_negative_direciton,
        bridge_formula_cnt_negative_direciton,
        bridge_formula_olhv_perc_negative_direciton,
        bridge_formula_tonperhv_negative_direciton,
        gross_formula_cnt_negative_direciton,
        gross_formula_olhv_perc_negative_direciton,
        gross_formula_tonperhv_negative_direciton,
        total_avg_cnt_negative_direciton,
        total_avg_olhv_perc_negative_direciton,
        total_avg_tonperhv_negative_direciton,
        egrl_percent_positive_direction,
        egrl_percent_negative_direction,
        egrw_percent_positive_direction,
        egrw_percent_negative_direction,
        num_weighed,
        num_weighed_positive_direction,
        num_weighed_negative_direction,
        wst_2_axle_busses_cnt_pos_dir,
        wst_2_axle_6_tyre_single_units_cnt_pos_dir,
        wst_busses_with_3_or_4_axles_cnt_pos_dir,
        wst_2_axle_6tyre_su_with_trailer_4axles_max_cnt_pos_dir,
        wst_3_axle_su_incl_single_axle_trailer_cnt_pos_dir,
        wst_4_or_less_axle_incl_a_single_trailer_cnt_pos_dir,
        wst_busses_with_5_or_more_axles_cnt_pos_dir,
        wst_3_axle_su_and_trailer_more_than_4_axles_cnt_pos_dir,
        wst_5_axle_single_trailer_cnt_pos_dir,
        wst_6_axle_single_trailer_cnt_pos_dir,
        wst_5_or_less_axle_multi_trailer_cnt_pos_dir,
        wst_6_axle_multi_trailer_cnt_pos_dir,
        wst_7_axle_multi_trailer_cnt_pos_dir,
        wst_8_or_more_axle_multi_trailer_cnt_pos_dir,
        wst_2_axle_busses_olhv_perc_pos_dir,
        wst_2_axle_6_tyre_single_units_olhv_perc_pos_dir,
        wst_busses_with_3_or_4_axles_olhv_perc_pos_dir,
        wst_2_axle_6tyre_su_with_trailer_4axles_max_olhv_perc_pos_dir,
        wst_3_axle_su_incl_single_axle_trailer_olhv_perc_pos_dir,
        wst_4_or_less_axle_incl_a_single_trailer_olhv_perc_pos_dir,
        wst_busses_with_5_or_more_axles_olhv_perc_pos_dir,
        wst_3_axle_su_and_trailer_more_than_4_axles_olhv_perc_pos_dir,
        wst_5_axle_single_trailer_olhv_perc_pos_dir,
        wst_6_axle_single_trailer_olhv_perc_pos_dir,
        wst_5_or_less_axle_multi_trailer_olhv_perc_pos_dir,
        wst_6_axle_multi_trailer_olhv_perc_pos_dir,
        wst_7_axle_multi_trailer_olhv_perc_pos_dir,
        wst_8_or_more_axle_multi_trailer_olhv_perc_pos_dir,
        wst_2_axle_busses_tonperhv_pos_dir,
        wst_2_axle_6_tyre_single_units_tonperhv_pos_dir,
        wst_busses_with_3_or_4_axles_tonperhv_pos_dir,
        wst_2_axle_6tyre_su_with_trailer_4axles_max_tonperhv_pos_dir,
        wst_3_axle_su_incl_single_axle_trailer_tonperhv_pos_dir,
        wst_4_or_less_axle_incl_a_single_trailer_tonperhv_pos_dir,
        wst_busses_with_5_or_more_axles_tonperhv_pos_dir,
        wst_3_axle_su_and_trailer_more_than_4_axles_tonperhv_pos_dir,
        wst_5_axle_single_trailer_tonperhv_pos_dir,
        wst_6_axle_single_trailer_tonperhv_pos_dir,
        wst_5_or_less_axle_multi_trailer_tonperhv_pos_dir,
        wst_6_axle_multi_trailer_tonperhv_pos_dir,
        wst_7_axle_multi_trailer_tonperhv_pos_dir,
        wst_8_or_more_axle_multi_trailer_tonperhv_pos_dir,
        wst_2_axle_busses_cnt_neg_dir,
        wst_2_axle_6_tyre_single_units_cnt_neg_dir,
        wst_busses_with_3_or_4_axles_cnt_neg_dir,
        wst_2_axle_6tyre_su_with_trailer_4axles_max_cnt_neg_dir,
        wst_3_axle_su_incl_single_axle_trailer_cnt_neg_dir,
        wst_4_or_less_axle_incl_a_single_trailer_cnt_neg_dir,
        wst_busses_with_5_or_more_axles_cnt_neg_dir,
        wst_3_axle_su_and_trailer_more_than_4_axles_cnt_neg_dir,
        wst_5_axle_single_trailer_cnt_neg_dir,
        wst_6_axle_single_trailer_cnt_neg_dir,
        wst_5_or_less_axle_multi_trailer_cnt_neg_dir,
        wst_6_axle_multi_trailer_cnt_neg_dir,
        wst_7_axle_multi_trailer_cnt_neg_dir,
        wst_8_or_more_axle_multi_trailer_cnt_neg_dir,
        wst_2_axle_busses_olhv_perc_neg_dir,
        wst_2_axle_6_tyre_single_units_olhv_perc_neg_dir,
        wst_busses_with_3_or_4_axles_olhv_perc_neg_dir,
        wst_2_axle_6tyre_su_with_trailer_4axles_max_olhv_perc_neg_dir,
        wst_3_axle_su_incl_single_axle_trailer_olhv_perc_neg_dir,
        wst_4_or_less_axle_incl_a_single_trailer_olhv_perc_neg_dir,
        wst_busses_with_5_or_more_axles_olhv_perc_neg_dir,
        wst_3_axle_su_and_trailer_more_than_4_axles_olhv_perc_neg_dir,
        wst_5_axle_single_trailer_olhv_perc_neg_dir,
        wst_6_axle_single_trailer_olhv_perc_neg_dir,
        wst_5_or_less_axle_multi_trailer_olhv_perc_neg_dir,
        wst_6_axle_multi_trailer_olhv_perc_neg_dir,
        wst_7_axle_multi_trailer_olhv_perc_neg_dir,
        wst_8_or_more_axle_multi_trailer_olhv_perc_neg_dir,
        wst_2_axle_busses_tonperhv_neg_dir,
        wst_2_axle_6_tyre_single_units_tonperhv_neg_dir,
        wst_busses_with_3_or_4_axles_tonperhv_neg_dir,
        wst_2_axle_6tyre_su_with_trailer_4axles_max_tonperhv_neg_dir,
        wst_3_axle_su_incl_single_axle_trailer_tonperhv_neg_dir,
        wst_4_or_less_axle_incl_a_single_trailer_tonperhv_neg_dir,
        wst_busses_with_5_or_more_axles_tonperhv_neg_dir,
        wst_3_axle_su_and_trailer_more_than_4_axles_tonperhv_neg_dir,
        wst_5_axle_single_trailer_tonperhv_neg_dir,
        wst_6_axle_single_trailer_tonperhv_neg_dir,
        wst_5_or_less_axle_multi_trailer_tonperhv_neg_dir,
        wst_6_axle_multi_trailer_tonperhv_neg_dir,
        wst_7_axle_multi_trailer_tonperhv_neg_dir,
        wst_8_or_more_axle_multi_trailer_tonperhv_neg_dir,
        wst_2_axle_busses_cnt,
        wst_2_axle_6_tyre_single_units_cnt,
        wst_busses_with_3_or_4_axles_cnt,
        wst_2_axle_6tyre_su_with_trailer_4axles_max_cnt,
        wst_3_axle_su_incl_single_axle_trailer_cnt,
        wst_4_or_less_axle_incl_a_single_trailer_cnt,
        wst_busses_with_5_or_more_axles_cnt,
        wst_3_axle_su_and_trailer_more_than_4_axles_cnt,
        wst_5_axle_single_trailer_cnt,
        wst_6_axle_single_trailer_cnt,
        wst_5_or_less_axle_multi_trailer_cnt,
        wst_6_axle_multi_trailer_cnt,
        wst_7_axle_multi_trailer_cnt,
        wst_8_or_more_axle_multi_trailer_cnt,
        wst_2_axle_busses_olhv_perc,
        wst_2_axle_6_tyre_single_units_olhv_perc,
        wst_busses_with_3_or_4_axles_olhv_perc,
        wst_2_axle_6tyre_su_with_trailer_4axles_max_olhv_perc,
        wst_3_axle_su_incl_single_axle_trailer_olhv_perc,
        wst_4_or_less_axle_incl_a_single_trailer_olhv_perc,
        wst_busses_with_5_or_more_axles_olhv_perc,
        wst_3_axle_su_and_trailer_more_than_4_axles_olhv_perc,
        wst_5_axle_single_trailer_olhv_perc,
        wst_6_axle_single_trailer_olhv_perc,
        wst_5_or_less_axle_multi_trailer_olhv_perc,
        wst_6_axle_multi_trailer_olhv_perc,
        wst_7_axle_multi_trailer_olhv_perc,
        wst_8_or_more_axle_multi_trailer_olhv_perc,
        wst_2_axle_busses_tonperhv,
        wst_2_axle_6_tyre_single_units_tonperhv,
        wst_busses_with_3_or_4_axles_tonperhv,
        wst_2_axle_6tyre_su_with_trailer_4axles_max_tonperhv,
        wst_3_axle_su_incl_single_axle_trailer_tonperhv,
        wst_4_or_less_axle_incl_a_single_trailer_tonperhv,
        wst_busses_with_5_or_more_axles_tonperhv,
        wst_3_axle_su_and_trailer_more_than_4_axles_tonperhv,
        wst_5_axle_single_trailer_tonperhv,
        wst_6_axle_single_trailer_tonperhv,
        wst_5_or_less_axle_multi_trailer_tonperhv,
        wst_6_axle_multi_trailer_tonperhv,
        wst_7_axle_multi_trailer_tonperhv,
        wst_8_or_more_axle_multi_trailer_tonperhv)
    VALUES(
    '{header_id}',
    {egrl_percent},
    {egrw_percent},
    {mean_equivalent_axle_mass},
    {mean_equivalent_axle_mass_positive_direction},
    {mean_equivalent_axle_mass_negative_direction},
    {mean_axle_spacing},
    {mean_axle_spacing_positive_direction},
    {mean_axle_spacing_negative_direction},
    {e80_per_axle},
    {e80_per_axle_positive_direction},
    {e80_per_axle_negative_direction},
    {olhv},
    {olhv_positive_direction},
    {olhv_negative_direction},
    {olhv_percent},
    {olhv_percent_positive_direction},
    {olhv_percent_negative_direction},
    {tonnage_generated},
    {tonnage_generated_positive_direction},
    {tonnage_generated_negative_direction},
    {olton},
    {olton_positive_direction},
    {olton_negative_direction},
    {olton_percent},
    {olton_percent_positive_direction},
    {olton_percent_negative_direction},
    {ole80},
    {ole80_positive_direction},
    {ole80_negative_direction},
    {ole80_percent},
    {ole80_percent_positive_direction},
    {ole80_percent_negative_direction},
    {xe80},
    {xe80_positive_direction},
    {xe80_negative_direction},
    {xe80_percent},
    {xe80_percent_positive_direction},
    {xe80_percent_negative_direction},
    {e80_per_day},
    {e80_per_day_positive_direction},
    {e80_per_day_negative_direction},
    {e80_per_heavy_vehicle},
    {e80_per_heavy_vehicle_positive_direction},
    {e80_per_heavy_vehicle_negative_direction},
    {worst_steering_single_axle_cnt},
    {worst_steering_single_axle_olhv_perc},
    {worst_steering_single_axle_tonperhv},
    {worst_steering_double_axle_cnt},
    {worst_steering_double_axle_olhv_perc},
    {worst_steering_double_axle_tonperhv},
    {worst_non_steering_single_axle_cnt},
    {worst_non_steering_single_axle_olhv_perc},
    {worst_non_steering_single_axle_tonperhv},
    {worst_non_steering_double_axle_cnt},
    {worst_non_steering_double_axle_olhv_perc},
    {worst_non_steering_double_axle_tonperhv},
    {worst_triple_axle_cnt},
    {worst_triple_axle_olhv_perc},
    {worst_triple_axle_tonperhv},
    {bridge_formula_cnt},
    {bridge_formula_olhv_perc},
    {bridge_formula_tonperhv},
    {gross_formula_cnt},
    {gross_formula_olhv_perc},
    {gross_formula_tonperhv},
    {total_avg_cnt},
    {total_avg_olhv_perc},
    {total_avg_tonperhv},
    {worst_steering_single_axle_cnt_positive_direciton},
    {worst_steering_single_axle_olhv_perc_positive_direciton},
    {worst_steering_single_axle_tonperhv_positive_direciton},
    {worst_steering_double_axle_cnt_positive_direciton},
    {worst_steering_double_axle_olhv_perc_positive_direciton},
    {worst_steering_double_axle_tonperhv_positive_direciton},
    {worst_non_steering_single_axle_cnt_positive_direciton},
    {worst_non_steering_single_axle_olhv_perc_positive_direciton},
    {worst_non_steering_single_axle_tonperhv_positive_direciton},
    {worst_non_steering_double_axle_cnt_positive_direciton},
    {worst_non_steering_double_axle_olhv_perc_positive_direciton},
    {worst_non_steering_double_axle_tonperhv_positive_direciton},
    {worst_triple_axle_cnt_positive_direciton},
    {worst_triple_axle_olhv_perc_positive_direciton},
    {worst_triple_axle_tonperhv_positive_direciton},
    {bridge_formula_cnt_positive_direciton},
    {bridge_formula_olhv_perc_positive_direciton},
    {bridge_formula_tonperhv_positive_direciton},
    {gross_formula_cnt_positive_direciton},
    {gross_formula_olhv_perc_positive_direciton},
    {gross_formula_tonperhv_positive_direciton},
    {total_avg_cnt_positive_direciton},
    {total_avg_olhv_perc_positive_direciton},
    {total_avg_tonperhv_positive_direciton},
    {worst_steering_single_axle_cnt_negative_direciton},
    {worst_steering_single_axle_olhv_perc_negative_direciton},
    {worst_steering_single_axle_tonperhv_negative_direciton},
    {worst_steering_double_axle_cnt_negative_direciton},
    {worst_steering_double_axle_olhv_perc_negative_direciton},
    {worst_steering_double_axle_tonperhv_negative_direciton},
    {worst_non_steering_single_axle_cnt_negative_direciton},
    {worst_non_steering_single_axle_olhv_perc_negative_direciton},
    {worst_non_steering_single_axle_tonperhv_negative_direciton},
    {worst_non_steering_double_axle_cnt_negative_direciton},
    {worst_non_steering_double_axle_olhv_perc_negative_direciton},
    {worst_non_steering_double_axle_tonperhv_negative_direciton},
    {worst_triple_axle_cnt_negative_direciton},
    {worst_triple_axle_olhv_perc_negative_direciton},
    {worst_triple_axle_tonperhv_negative_direciton},
    {bridge_formula_cnt_negative_direciton},
    {bridge_formula_olhv_perc_negative_direciton},
    {bridge_formula_tonperhv_negative_direciton},
    {gross_formula_cnt_negative_direciton},
    {gross_formula_olhv_perc_negative_direciton},
    {gross_formula_tonperhv_negative_direciton},
    {total_avg_cnt_negative_direciton},
    {total_avg_olhv_perc_negative_direciton},
    {total_avg_tonperhv_negative_direciton},
    {egrl_percent_positive_direction},
    {egrl_percent_negative_direction},
    {egrw_percent_positive_direction},
    {egrw_percent_negative_direction},
    {num_weighed},
    {num_weighed_positive_direction},
    {num_weighed_negative_direction},
    {wst_2_axle_busses_cnt_pos_dir},
    {wst_2_axle_6_tyre_single_units_cnt_pos_dir},
    {wst_busses_with_3_or_4_axles_cnt_pos_dir},
    {wst_2_axle_6tyre_su_with_trailer_4axles_max_cnt_pos_dir},
    {wst_3_axle_su_incl_single_axle_trailer_cnt_pos_dir},
    {wst_4_or_less_axle_incl_a_single_trailer_cnt_pos_dir},
    {wst_busses_with_5_or_more_axles_cnt_pos_dir},
    {wst_3_axle_su_and_trailer_more_than_4_axles_cnt_pos_dir},
    {wst_5_axle_single_trailer_cnt_pos_dir},
    {wst_6_axle_single_trailer_cnt_pos_dir},
    {wst_5_or_less_axle_multi_trailer_cnt_pos_dir},
    {wst_6_axle_multi_trailer_cnt_pos_dir},
    {wst_7_axle_multi_trailer_cnt_pos_dir},
    {wst_8_or_more_axle_multi_trailer_cnt_pos_dir},
    {wst_2_axle_busses_olhv_perc_pos_dir},
    {wst_2_axle_6_tyre_single_units_olhv_perc_pos_dir},
    {wst_busses_with_3_or_4_axles_olhv_perc_pos_dir},
    {wst_2_axle_6tyre_su_with_trailer_4axles_max_olhv_perc_pos_dir},
    {wst_3_axle_su_incl_single_axle_trailer_olhv_perc_pos_dir},
    {wst_4_or_less_axle_incl_a_single_trailer_olhv_perc_pos_dir},
    {wst_busses_with_5_or_more_axles_olhv_perc_pos_dir},
    {wst_3_axle_su_and_trailer_more_than_4_axles_olhv_perc_pos_dir},
    {wst_5_axle_single_trailer_olhv_perc_pos_dir},
    {wst_6_axle_single_trailer_olhv_perc_pos_dir},
    {wst_5_or_less_axle_multi_trailer_olhv_perc_pos_dir},
    {wst_6_axle_multi_trailer_olhv_perc_pos_dir},
    {wst_7_axle_multi_trailer_olhv_perc_pos_dir},
    {wst_8_or_more_axle_multi_trailer_olhv_perc_pos_dir},
    {wst_2_axle_busses_tonperhv_pos_dir},
    {wst_2_axle_6_tyre_single_units_tonperhv_pos_dir},
    {wst_busses_with_3_or_4_axles_tonperhv_pos_dir},
    {wst_2_axle_6tyre_su_with_trailer_4axles_max_tonperhv_pos_dir},
    {wst_3_axle_su_incl_single_axle_trailer_tonperhv_pos_dir},
    {wst_4_or_less_axle_incl_a_single_trailer_tonperhv_pos_dir},
    {wst_busses_with_5_or_more_axles_tonperhv_pos_dir},
    {wst_3_axle_su_and_trailer_more_than_4_axles_tonperhv_pos_dir},
    {wst_5_axle_single_trailer_tonperhv_pos_dir},
    {wst_6_axle_single_trailer_tonperhv_pos_dir},
    {wst_5_or_less_axle_multi_trailer_tonperhv_pos_dir},
    {wst_6_axle_multi_trailer_tonperhv_pos_dir},
    {wst_7_axle_multi_trailer_tonperhv_pos_dir},
    {wst_8_or_more_axle_multi_trailer_tonperhv_pos_dir},
    {wst_2_axle_busses_cnt_neg_dir},
    {wst_2_axle_6_tyre_single_units_cnt_neg_dir},
    {wst_busses_with_3_or_4_axles_cnt_neg_dir},
    {wst_2_axle_6tyre_su_with_trailer_4axles_max_cnt_neg_dir},
    {wst_3_axle_su_incl_single_axle_trailer_cnt_neg_dir},
    {wst_4_or_less_axle_incl_a_single_trailer_cnt_neg_dir},
    {wst_busses_with_5_or_more_axles_cnt_neg_dir},
    {wst_3_axle_su_and_trailer_more_than_4_axles_cnt_neg_dir},
    {wst_5_axle_single_trailer_cnt_neg_dir},
    {wst_6_axle_single_trailer_cnt_neg_dir},
    {wst_5_or_less_axle_multi_trailer_cnt_neg_dir},
    {wst_6_axle_multi_trailer_cnt_neg_dir},
    {wst_7_axle_multi_trailer_cnt_neg_dir},
    {wst_8_or_more_axle_multi_trailer_cnt_neg_dir},
    {wst_2_axle_busses_olhv_perc_neg_dir},
    {wst_2_axle_6_tyre_single_units_olhv_perc_neg_dir},
    {wst_busses_with_3_or_4_axles_olhv_perc_neg_dir},
    {wst_2_axle_6tyre_su_with_trailer_4axles_max_olhv_perc_neg_dir},
    {wst_3_axle_su_incl_single_axle_trailer_olhv_perc_neg_dir},
    {wst_4_or_less_axle_incl_a_single_trailer_olhv_perc_neg_dir},
    {wst_busses_with_5_or_more_axles_olhv_perc_neg_dir},
    {wst_3_axle_su_and_trailer_more_than_4_axles_olhv_perc_neg_dir},
    {wst_5_axle_single_trailer_olhv_perc_neg_dir},
    {wst_6_axle_single_trailer_olhv_perc_neg_dir},
    {wst_5_or_less_axle_multi_trailer_olhv_perc_neg_dir},
    {wst_6_axle_multi_trailer_olhv_perc_neg_dir},
    {wst_7_axle_multi_trailer_olhv_perc_neg_dir},
    {wst_8_or_more_axle_multi_trailer_olhv_perc_neg_dir},
    {wst_2_axle_busses_tonperhv_neg_dir},
    {wst_2_axle_6_tyre_single_units_tonperhv_neg_dir},
    {wst_busses_with_3_or_4_axles_tonperhv_neg_dir},
    {wst_2_axle_6tyre_su_with_trailer_4axles_max_tonperhv_neg_dir},
    {wst_3_axle_su_incl_single_axle_trailer_tonperhv_neg_dir},
    {wst_4_or_less_axle_incl_a_single_trailer_tonperhv_neg_dir},
    {wst_busses_with_5_or_more_axles_tonperhv_neg_dir},
    {wst_3_axle_su_and_trailer_more_than_4_axles_tonperhv_neg_dir},
    {wst_5_axle_single_trailer_tonperhv_neg_dir},
    {wst_6_axle_single_trailer_tonperhv_neg_dir},
    {wst_5_or_less_axle_multi_trailer_tonperhv_neg_dir},
    {wst_6_axle_multi_trailer_tonperhv_neg_dir},
    {wst_7_axle_multi_trailer_tonperhv_neg_dir},
    {wst_8_or_more_axle_multi_trailer_tonperhv_neg_dir},
    {wst_2_axle_busses_cnt},
    {wst_2_axle_6_tyre_single_units_cnt},
    {wst_busses_with_3_or_4_axles_cnt},
    {wst_2_axle_6tyre_su_with_trailer_4axles_max_cnt},
    {wst_3_axle_su_incl_single_axle_trailer_cnt},
    {wst_4_or_less_axle_incl_a_single_trailer_cnt},
    {wst_busses_with_5_or_more_axles_cnt},
    {wst_3_axle_su_and_trailer_more_than_4_axles_cnt},
    {wst_5_axle_single_trailer_cnt},
    {wst_6_axle_single_trailer_cnt},
    {wst_5_or_less_axle_multi_trailer_cnt},
    {wst_6_axle_multi_trailer_cnt},
    {wst_7_axle_multi_trailer_cnt},
    {wst_8_or_more_axle_multi_trailer_cnt},
    {wst_2_axle_busses_olhv_perc},
    {wst_2_axle_6_tyre_single_units_olhv_perc},
    {wst_busses_with_3_or_4_axles_olhv_perc},
    {wst_2_axle_6tyre_su_with_trailer_4axles_max_olhv_perc},
    {wst_3_axle_su_incl_single_axle_trailer_olhv_perc},
    {wst_4_or_less_axle_incl_a_single_trailer_olhv_perc},
    {wst_busses_with_5_or_more_axles_olhv_perc},
    {wst_3_axle_su_and_trailer_more_than_4_axles_olhv_perc},
    {wst_5_axle_single_trailer_olhv_perc},
    {wst_6_axle_single_trailer_olhv_perc},
    {wst_5_or_less_axle_multi_trailer_olhv_perc},
    {wst_6_axle_multi_trailer_olhv_perc},
    {wst_7_axle_multi_trailer_olhv_perc},
    {wst_8_or_more_axle_multi_trailer_olhv_perc},
    {wst_2_axle_busses_tonperhv},
    {wst_2_axle_6_tyre_single_units_tonperhv},
    {wst_busses_with_3_or_4_axles_tonperhv},
    {wst_2_axle_6tyre_su_with_trailer_4axles_max_tonperhv},
    {wst_3_axle_su_incl_single_axle_trailer_tonperhv},
    {wst_4_or_less_axle_incl_a_single_trailer_tonperhv},
    {wst_busses_with_5_or_more_axles_tonperhv},
    {wst_3_axle_su_and_trailer_more_than_4_axles_tonperhv},
    {wst_5_axle_single_trailer_tonperhv},
    {wst_6_axle_single_trailer_tonperhv},
    {wst_5_or_less_axle_multi_trailer_tonperhv},
    {wst_6_axle_multi_trailer_tonperhv},
    {wst_7_axle_multi_trailer_tonperhv},
    {wst_8_or_more_axle_multi_trailer_tonperhv})
    ON CONFLICT ON CONSTRAINT electronic_count_header_hswim_pkey DO UPDATE SET 
    egrl_percent = COALESCE(EXCLUDED.egrl_percent,egrl_percent),
    egrw_percent = COALESCE(EXCLUDED.egrw_percent,egrw_percent),
    mean_equivalent_axle_mass = COALESCE(EXCLUDED.mean_equivalent_axle_mass,mean_equivalent_axle_mass),
    mean_equivalent_axle_mass_positive_direction = COALESCE(EXCLUDED.mean_equivalent_axle_mass_positive_direction,mean_equivalent_axle_mass_positive_direction),
    mean_equivalent_axle_mass_negative_direction = COALESCE(EXCLUDED.mean_equivalent_axle_mass_negative_direction,mean_equivalent_axle_mass_negative_direction),
    mean_axle_spacing = COALESCE(EXCLUDED.mean_axle_spacing,mean_axle_spacing),
    mean_axle_spacing_positive_direction = COALESCE(EXCLUDED.mean_axle_spacing_positive_direction,mean_axle_spacing_positive_direction),
    mean_axle_spacing_negative_direction = COALESCE(EXCLUDED.mean_axle_spacing_negative_direction,mean_axle_spacing_negative_direction),
    e80_per_axle = COALESCE(EXCLUDED.e80_per_axle,e80_per_axle),
    e80_per_axle_positive_direction = COALESCE(EXCLUDED.e80_per_axle_positive_direction,e80_per_axle_positive_direction),
    e80_per_axle_negative_direction = COALESCE(EXCLUDED.e80_per_axle_negative_direction,e80_per_axle_negative_direction),
    olhv = COALESCE(EXCLUDED.olhv,olhv),
    olhv_positive_direction = COALESCE(EXCLUDED.olhv_positive_direction,olhv_positive_direction),
    olhv_negative_direction = COALESCE(EXCLUDED.olhv_negative_direction,olhv_negative_direction),
    olhv_percent = COALESCE(EXCLUDED.olhv_percent,olhv_percent),
    olhv_percent_positive_direction = COALESCE(EXCLUDED.olhv_percent_positive_direction,olhv_percent_positive_direction),
    olhv_percent_negative_direction = COALESCE(EXCLUDED.olhv_percent_negative_direction,olhv_percent_negative_direction),
    tonnage_generated = COALESCE(EXCLUDED.tonnage_generated,tonnage_generated),
    tonnage_generated_positive_direction = COALESCE(EXCLUDED.tonnage_generated_positive_direction,tonnage_generated_positive_direction),
    tonnage_generated_negative_direction = COALESCE(EXCLUDED.tonnage_generated_negative_direction,tonnage_generated_negative_direction),
    olton = COALESCE(EXCLUDED.olton,olton),
    olton_positive_direction = COALESCE(EXCLUDED.olton_positive_direction,olton_positive_direction),
    olton_negative_direction = COALESCE(EXCLUDED.olton_negative_direction,olton_negative_direction),
    olton_percent = COALESCE(EXCLUDED.olton_percent,olton_percent),
    olton_percent_positive_direction = COALESCE(EXCLUDED.olton_percent_positive_direction,olton_percent_positive_direction),
    olton_percent_negative_direction = COALESCE(EXCLUDED.olton_percent_negative_direction,olton_percent_negative_direction),
    ole8 = COALESCE(EXCLUDED.ole8,ole8),
    ole80_positive_direction = COALESCE(EXCLUDED.ole80_positive_direction,ole80_positive_direction),
    ole80_negative_direction = COALESCE(EXCLUDED.ole80_negative_direction,ole80_negative_direction),
    ole80_percent = COALESCE(EXCLUDED.ole80_percent,ole80_percent),
    ole80_percent_positive_direction = COALESCE(EXCLUDED.ole80_percent_positive_direction,ole80_percent_positive_direction),
    ole80_percent_negative_direction = COALESCE(EXCLUDED.ole80_percent_negative_direction,ole80_percent_negative_direction),
    xe8 = COALESCE(EXCLUDED.xe8,xe8),
    xe80_positive_direction = COALESCE(EXCLUDED.xe80_positive_direction,xe80_positive_direction),
    xe80_negative_direction = COALESCE(EXCLUDED.xe80_negative_direction,xe80_negative_direction),
    xe80_percent = COALESCE(EXCLUDED.xe80_percent,xe80_percent),
    xe80_percent_positive_direction = COALESCE(EXCLUDED.xe80_percent_positive_direction,xe80_percent_positive_direction),
    xe80_percent_negative_direction = COALESCE(EXCLUDED.xe80_percent_negative_direction,xe80_percent_negative_direction),
    e80_per_day = COALESCE(EXCLUDED.e80_per_day,e80_per_day),
    e80_per_day_positive_direction = COALESCE(EXCLUDED.e80_per_day_positive_direction,e80_per_day_positive_direction),
    e80_per_day_negative_direction = COALESCE(EXCLUDED.e80_per_day_negative_direction,e80_per_day_negative_direction),
    e80_per_heavy_vehicle = COALESCE(EXCLUDED.e80_per_heavy_vehicle,e80_per_heavy_vehicle),
    e80_per_heavy_vehicle_positive_direction = COALESCE(EXCLUDED.e80_per_heavy_vehicle_positive_direction,e80_per_heavy_vehicle_positive_direction),
    e80_per_heavy_vehicle_negative_direction = COALESCE(EXCLUDED.e80_per_heavy_vehicle_negative_direction,e80_per_heavy_vehicle_negative_direction),
    worst_steering_single_axle_cnt = COALESCE(EXCLUDED.worst_steering_single_axle_cnt,worst_steering_single_axle_cnt),
    worst_steering_single_axle_olhv_perc = COALESCE(EXCLUDED.worst_steering_single_axle_olhv_perc,worst_steering_single_axle_olhv_perc),
    worst_steering_single_axle_tonperhv = COALESCE(EXCLUDED.worst_steering_single_axle_tonperhv,worst_steering_single_axle_tonperhv),
    worst_steering_double_axle_cnt = COALESCE(EXCLUDED.worst_steering_double_axle_cnt,worst_steering_double_axle_cnt),
    worst_steering_double_axle_olhv_perc = COALESCE(EXCLUDED.worst_steering_double_axle_olhv_perc,worst_steering_double_axle_olhv_perc),
    worst_steering_double_axle_tonperhv = COALESCE(EXCLUDED.worst_steering_double_axle_tonperhv,worst_steering_double_axle_tonperhv),
    worst_non_steering_single_axle_cnt = COALESCE(EXCLUDED.worst_non_steering_single_axle_cnt,worst_non_steering_single_axle_cnt),
    worst_non_steering_single_axle_olhv_perc = COALESCE(EXCLUDED.worst_non_steering_single_axle_olhv_perc,worst_non_steering_single_axle_olhv_perc),
    worst_non_steering_single_axle_tonperhv = COALESCE(EXCLUDED.worst_non_steering_single_axle_tonperhv,worst_non_steering_single_axle_tonperhv),
    worst_non_steering_double_axle_cnt = COALESCE(EXCLUDED.worst_non_steering_double_axle_cnt,worst_non_steering_double_axle_cnt),
    worst_non_steering_double_axle_olhv_perc = COALESCE(EXCLUDED.worst_non_steering_double_axle_olhv_perc,worst_non_steering_double_axle_olhv_perc),
    worst_non_steering_double_axle_tonperhv = COALESCE(EXCLUDED.worst_non_steering_double_axle_tonperhv,worst_non_steering_double_axle_tonperhv),
    worst_triple_axle_cnt = COALESCE(EXCLUDED.worst_triple_axle_cnt,worst_triple_axle_cnt),
    worst_triple_axle_olhv_perc = COALESCE(EXCLUDED.worst_triple_axle_olhv_perc,worst_triple_axle_olhv_perc),
    worst_triple_axle_tonperhv = COALESCE(EXCLUDED.worst_triple_axle_tonperhv,worst_triple_axle_tonperhv),
    bridge_formula_cnt = COALESCE(EXCLUDED.bridge_formula_cnt,bridge_formula_cnt),
    bridge_formula_olhv_perc = COALESCE(EXCLUDED.bridge_formula_olhv_perc,bridge_formula_olhv_perc),
    bridge_formula_tonperhv = COALESCE(EXCLUDED.bridge_formula_tonperhv,bridge_formula_tonperhv),
    gross_formula_cnt = COALESCE(EXCLUDED.gross_formula_cnt,gross_formula_cnt),
    gross_formula_olhv_perc = COALESCE(EXCLUDED.gross_formula_olhv_perc,gross_formula_olhv_perc),
    gross_formula_tonperhv = COALESCE(EXCLUDED.gross_formula_tonperhv,gross_formula_tonperhv),
    total_avg_cnt = COALESCE(EXCLUDED.total_avg_cnt,total_avg_cnt),
    total_avg_olhv_perc = COALESCE(EXCLUDED.total_avg_olhv_perc,total_avg_olhv_perc),
    total_avg_tonperhv = COALESCE(EXCLUDED.total_avg_tonperhv,total_avg_tonperhv),
    worst_steering_single_axle_cnt_positive_direciton = COALESCE(EXCLUDED.worst_steering_single_axle_cnt_positive_direciton,worst_steering_single_axle_cnt_positive_direciton),
    worst_steering_single_axle_olhv_perc_positive_direciton = COALESCE(EXCLUDED.worst_steering_single_axle_olhv_perc_positive_direciton,worst_steering_single_axle_olhv_perc_positive_direciton),
    worst_steering_single_axle_tonperhv_positive_direciton = COALESCE(EXCLUDED.worst_steering_single_axle_tonperhv_positive_direciton,worst_steering_single_axle_tonperhv_positive_direciton),
    worst_steering_double_axle_cnt_positive_direciton = COALESCE(EXCLUDED.worst_steering_double_axle_cnt_positive_direciton,worst_steering_double_axle_cnt_positive_direciton),
    worst_steering_double_axle_olhv_perc_positive_direciton = COALESCE(EXCLUDED.worst_steering_double_axle_olhv_perc_positive_direciton,worst_steering_double_axle_olhv_perc_positive_direciton),
    worst_steering_double_axle_tonperhv_positive_direciton = COALESCE(EXCLUDED.worst_steering_double_axle_tonperhv_positive_direciton,worst_steering_double_axle_tonperhv_positive_direciton),
    worst_non_steering_single_axle_cnt_positive_direciton = COALESCE(EXCLUDED.worst_non_steering_single_axle_cnt_positive_direciton,worst_non_steering_single_axle_cnt_positive_direciton),
    worst_non_steering_single_axle_olhv_perc_positive_direciton = COALESCE(EXCLUDED.worst_non_steering_single_axle_olhv_perc_positive_direciton,worst_non_steering_single_axle_olhv_perc_positive_direciton),
    worst_non_steering_single_axle_tonperhv_positive_direciton = COALESCE(EXCLUDED.worst_non_steering_single_axle_tonperhv_positive_direciton,worst_non_steering_single_axle_tonperhv_positive_direciton),
    worst_non_steering_double_axle_cnt_positive_direciton = COALESCE(EXCLUDED.worst_non_steering_double_axle_cnt_positive_direciton,worst_non_steering_double_axle_cnt_positive_direciton),
    worst_non_steering_double_axle_olhv_perc_positive_direciton = COALESCE(EXCLUDED.worst_non_steering_double_axle_olhv_perc_positive_direciton,worst_non_steering_double_axle_olhv_perc_positive_direciton),
    worst_non_steering_double_axle_tonperhv_positive_direciton = COALESCE(EXCLUDED.worst_non_steering_double_axle_tonperhv_positive_direciton,worst_non_steering_double_axle_tonperhv_positive_direciton),
    worst_triple_axle_cnt_positive_direciton = COALESCE(EXCLUDED.worst_triple_axle_cnt_positive_direciton,worst_triple_axle_cnt_positive_direciton),
    worst_triple_axle_olhv_perc_positive_direciton = COALESCE(EXCLUDED.worst_triple_axle_olhv_perc_positive_direciton,worst_triple_axle_olhv_perc_positive_direciton),
    worst_triple_axle_tonperhv_positive_direciton = COALESCE(EXCLUDED.worst_triple_axle_tonperhv_positive_direciton,worst_triple_axle_tonperhv_positive_direciton),
    bridge_formula_cnt_positive_direciton = COALESCE(EXCLUDED.bridge_formula_cnt_positive_direciton,bridge_formula_cnt_positive_direciton),
    bridge_formula_olhv_perc_positive_direciton = COALESCE(EXCLUDED.bridge_formula_olhv_perc_positive_direciton,bridge_formula_olhv_perc_positive_direciton),
    bridge_formula_tonperhv_positive_direciton = COALESCE(EXCLUDED.bridge_formula_tonperhv_positive_direciton,bridge_formula_tonperhv_positive_direciton),
    gross_formula_cnt_positive_direciton = COALESCE(EXCLUDED.gross_formula_cnt_positive_direciton,gross_formula_cnt_positive_direciton),
    gross_formula_olhv_perc_positive_direciton = COALESCE(EXCLUDED.gross_formula_olhv_perc_positive_direciton,gross_formula_olhv_perc_positive_direciton),
    gross_formula_tonperhv_positive_direciton = COALESCE(EXCLUDED.gross_formula_tonperhv_positive_direciton,gross_formula_tonperhv_positive_direciton),
    total_avg_cnt_positive_direciton = COALESCE(EXCLUDED.total_avg_cnt_positive_direciton,total_avg_cnt_positive_direciton),
    total_avg_olhv_perc_positive_direciton = COALESCE(EXCLUDED.total_avg_olhv_perc_positive_direciton,total_avg_olhv_perc_positive_direciton),
    total_avg_tonperhv_positive_direciton = COALESCE(EXCLUDED.total_avg_tonperhv_positive_direciton,total_avg_tonperhv_positive_direciton),
    worst_steering_single_axle_cnt_negative_direciton = COALESCE(EXCLUDED.worst_steering_single_axle_cnt_negative_direciton,worst_steering_single_axle_cnt_negative_direciton),
    worst_steering_single_axle_olhv_perc_negative_direciton = COALESCE(EXCLUDED.worst_steering_single_axle_olhv_perc_negative_direciton,worst_steering_single_axle_olhv_perc_negative_direciton),
    worst_steering_single_axle_tonperhv_negative_direciton = COALESCE(EXCLUDED.worst_steering_single_axle_tonperhv_negative_direciton,worst_steering_single_axle_tonperhv_negative_direciton),
    worst_steering_double_axle_cnt_negative_direciton = COALESCE(EXCLUDED.worst_steering_double_axle_cnt_negative_direciton,worst_steering_double_axle_cnt_negative_direciton),
    worst_steering_double_axle_olhv_perc_negative_direciton = COALESCE(EXCLUDED.worst_steering_double_axle_olhv_perc_negative_direciton,worst_steering_double_axle_olhv_perc_negative_direciton),
    worst_steering_double_axle_tonperhv_negative_direciton = COALESCE(EXCLUDED.worst_steering_double_axle_tonperhv_negative_direciton,worst_steering_double_axle_tonperhv_negative_direciton),
    worst_non_steering_single_axle_cnt_negative_direciton = COALESCE(EXCLUDED.worst_non_steering_single_axle_cnt_negative_direciton,worst_non_steering_single_axle_cnt_negative_direciton),
    worst_non_steering_single_axle_olhv_perc_negative_direciton = COALESCE(EXCLUDED.worst_non_steering_single_axle_olhv_perc_negative_direciton,worst_non_steering_single_axle_olhv_perc_negative_direciton),
    worst_non_steering_single_axle_tonperhv_negative_direciton = COALESCE(EXCLUDED.worst_non_steering_single_axle_tonperhv_negative_direciton,worst_non_steering_single_axle_tonperhv_negative_direciton),
    worst_non_steering_double_axle_cnt_negative_direciton = COALESCE(EXCLUDED.worst_non_steering_double_axle_cnt_negative_direciton,worst_non_steering_double_axle_cnt_negative_direciton),
    worst_non_steering_double_axle_olhv_perc_negative_direciton = COALESCE(EXCLUDED.worst_non_steering_double_axle_olhv_perc_negative_direciton,worst_non_steering_double_axle_olhv_perc_negative_direciton),
    worst_non_steering_double_axle_tonperhv_negative_direciton = COALESCE(EXCLUDED.worst_non_steering_double_axle_tonperhv_negative_direciton,worst_non_steering_double_axle_tonperhv_negative_direciton),
    worst_triple_axle_cnt_negative_direciton = COALESCE(EXCLUDED.worst_triple_axle_cnt_negative_direciton,worst_triple_axle_cnt_negative_direciton),
    worst_triple_axle_olhv_perc_negative_direciton = COALESCE(EXCLUDED.worst_triple_axle_olhv_perc_negative_direciton,worst_triple_axle_olhv_perc_negative_direciton),
    worst_triple_axle_tonperhv_negative_direciton = COALESCE(EXCLUDED.worst_triple_axle_tonperhv_negative_direciton,worst_triple_axle_tonperhv_negative_direciton),
    bridge_formula_cnt_negative_direciton = COALESCE(EXCLUDED.bridge_formula_cnt_negative_direciton,bridge_formula_cnt_negative_direciton),
    bridge_formula_olhv_perc_negative_direciton = COALESCE(EXCLUDED.bridge_formula_olhv_perc_negative_direciton,bridge_formula_olhv_perc_negative_direciton),
    bridge_formula_tonperhv_negative_direciton = COALESCE(EXCLUDED.bridge_formula_tonperhv_negative_direciton,bridge_formula_tonperhv_negative_direciton),
    gross_formula_cnt_negative_direciton = COALESCE(EXCLUDED.gross_formula_cnt_negative_direciton,gross_formula_cnt_negative_direciton),
    gross_formula_olhv_perc_negative_direciton = COALESCE(EXCLUDED.gross_formula_olhv_perc_negative_direciton,gross_formula_olhv_perc_negative_direciton),
    gross_formula_tonperhv_negative_direciton = COALESCE(EXCLUDED.gross_formula_tonperhv_negative_direciton,gross_formula_tonperhv_negative_direciton),
    total_avg_cnt_negative_direciton = COALESCE(EXCLUDED.total_avg_cnt_negative_direciton,total_avg_cnt_negative_direciton),
    total_avg_olhv_perc_negative_direciton = COALESCE(EXCLUDED.total_avg_olhv_perc_negative_direciton,total_avg_olhv_perc_negative_direciton),
    total_avg_tonperhv_negative_direciton = COALESCE(EXCLUDED.total_avg_tonperhv_negative_direciton,total_avg_tonperhv_negative_direciton),
    egrl_percent_positive_direction = COALESCE(EXCLUDED.egrl_percent_positive_direction,egrl_percent_positive_direction),
    egrl_percent_negative_direction = COALESCE(EXCLUDED.egrl_percent_negative_direction,egrl_percent_negative_direction),
    egrw_percent_positive_direction = COALESCE(EXCLUDED.egrw_percent_positive_direction,egrw_percent_positive_direction),
    egrw_percent_negative_direction = COALESCE(EXCLUDED.egrw_percent_negative_direction,egrw_percent_negative_direction),
    num_weighed = COALESCE(EXCLUDED.num_weighed,num_weighed),
    num_weighed_positive_direction = COALESCE(EXCLUDED.num_weighed_positive_direction,num_weighed_positive_direction),
    num_weighed_negative_direction = COALESCE(EXCLUDED.num_weighed_negative_direction,num_weighed_negative_direction),
    wst_2_axle_busses_cnt_pos_dir = COALESCE(EXCLUDED.wst_2_axle_busses_cnt_pos_dir,wst_2_axle_busses_cnt_pos_dir),
    wst_2_axle_6_tyre_single_units_cnt_pos_dir = COALESCE(EXCLUDED.wst_2_axle_6_tyre_single_units_cnt_pos_dir,wst_2_axle_6_tyre_single_units_cnt_pos_dir),
    wst_busses_with_3_or_4_axles_cnt_pos_dir = COALESCE(EXCLUDED.wst_busses_with_3_or_4_axles_cnt_pos_dir,wst_busses_with_3_or_4_axles_cnt_pos_dir),
    wst_2_axle_6tyre_su_with_trailer_4axles_max_cnt_pos_dir = COALESCE(EXCLUDED.wst_2_axle_6tyre_su_with_trailer_4axles_max_cnt_pos_dir,wst_2_axle_6tyre_su_with_trailer_4axles_max_cnt_pos_dir),
    wst_3_axle_su_incl_single_axle_trailer_cnt_pos_dir = COALESCE(EXCLUDED.wst_3_axle_su_incl_single_axle_trailer_cnt_pos_dir,wst_3_axle_su_incl_single_axle_trailer_cnt_pos_dir),
    wst_4_or_less_axle_incl_a_single_trailer_cnt_pos_dir = COALESCE(EXCLUDED.wst_4_or_less_axle_incl_a_single_trailer_cnt_pos_dir,wst_4_or_less_axle_incl_a_single_trailer_cnt_pos_dir),
    wst_busses_with_5_or_more_axles_cnt_pos_dir = COALESCE(EXCLUDED.wst_busses_with_5_or_more_axles_cnt_pos_dir,wst_busses_with_5_or_more_axles_cnt_pos_dir),
    wst_3_axle_su_and_trailer_more_than_4_axles_cnt_pos_dir = COALESCE(EXCLUDED.wst_3_axle_su_and_trailer_more_than_4_axles_cnt_pos_dir,wst_3_axle_su_and_trailer_more_than_4_axles_cnt_pos_dir),
    wst_5_axle_single_trailer_cnt_pos_dir = COALESCE(EXCLUDED.wst_5_axle_single_trailer_cnt_pos_dir,wst_5_axle_single_trailer_cnt_pos_dir),
    wst_6_axle_single_trailer_cnt_pos_dir = COALESCE(EXCLUDED.wst_6_axle_single_trailer_cnt_pos_dir,wst_6_axle_single_trailer_cnt_pos_dir),
    wst_5_or_less_axle_multi_trailer_cnt_pos_dir = COALESCE(EXCLUDED.wst_5_or_less_axle_multi_trailer_cnt_pos_dir,wst_5_or_less_axle_multi_trailer_cnt_pos_dir),
    wst_6_axle_multi_trailer_cnt_pos_dir = COALESCE(EXCLUDED.wst_6_axle_multi_trailer_cnt_pos_dir,wst_6_axle_multi_trailer_cnt_pos_dir),
    wst_7_axle_multi_trailer_cnt_pos_dir = COALESCE(EXCLUDED.wst_7_axle_multi_trailer_cnt_pos_dir,wst_7_axle_multi_trailer_cnt_pos_dir),
    wst_8_or_more_axle_multi_trailer_cnt_pos_dir = COALESCE(EXCLUDED.wst_8_or_more_axle_multi_trailer_cnt_pos_dir,wst_8_or_more_axle_multi_trailer_cnt_pos_dir),
    wst_2_axle_busses_olhv_perc_pos_dir = COALESCE(EXCLUDED.wst_2_axle_busses_olhv_perc_pos_dir,wst_2_axle_busses_olhv_perc_pos_dir),
    wst_2_axle_6_tyre_single_units_olhv_perc_pos_dir = COALESCE(EXCLUDED.wst_2_axle_6_tyre_single_units_olhv_perc_pos_dir,wst_2_axle_6_tyre_single_units_olhv_perc_pos_dir),
    wst_busses_with_3_or_4_axles_olhv_perc_pos_dir = COALESCE(EXCLUDED.wst_busses_with_3_or_4_axles_olhv_perc_pos_dir,wst_busses_with_3_or_4_axles_olhv_perc_pos_dir),
    wst_2_axle_6tyre_su_with_trailer_4axles_max_olhv_perc_pos_dir = COALESCE(EXCLUDED.wst_2_axle_6tyre_su_with_trailer_4axles_max_olhv_perc_pos_dir,wst_2_axle_6tyre_su_with_trailer_4axles_max_olhv_perc_pos_dir),
    wst_3_axle_su_incl_single_axle_trailer_olhv_perc_pos_dir = COALESCE(EXCLUDED.wst_3_axle_su_incl_single_axle_trailer_olhv_perc_pos_dir,wst_3_axle_su_incl_single_axle_trailer_olhv_perc_pos_dir),
    wst_4_or_less_axle_incl_a_single_trailer_olhv_perc_pos_dir = COALESCE(EXCLUDED.wst_4_or_less_axle_incl_a_single_trailer_olhv_perc_pos_dir,wst_4_or_less_axle_incl_a_single_trailer_olhv_perc_pos_dir),
    wst_busses_with_5_or_more_axles_olhv_perc_pos_dir = COALESCE(EXCLUDED.wst_busses_with_5_or_more_axles_olhv_perc_pos_dir,wst_busses_with_5_or_more_axles_olhv_perc_pos_dir),
    wst_3_axle_su_and_trailer_more_than_4_axles_olhv_perc_pos_dir = COALESCE(EXCLUDED.wst_3_axle_su_and_trailer_more_than_4_axles_olhv_perc_pos_dir,wst_3_axle_su_and_trailer_more_than_4_axles_olhv_perc_pos_dir),
    wst_5_axle_single_trailer_olhv_perc_pos_dir = COALESCE(EXCLUDED.wst_5_axle_single_trailer_olhv_perc_pos_dir,wst_5_axle_single_trailer_olhv_perc_pos_dir),
    wst_6_axle_single_trailer_olhv_perc_pos_dir = COALESCE(EXCLUDED.wst_6_axle_single_trailer_olhv_perc_pos_dir,wst_6_axle_single_trailer_olhv_perc_pos_dir),
    wst_5_or_less_axle_multi_trailer_olhv_perc_pos_dir = COALESCE(EXCLUDED.wst_5_or_less_axle_multi_trailer_olhv_perc_pos_dir,wst_5_or_less_axle_multi_trailer_olhv_perc_pos_dir),
    wst_6_axle_multi_trailer_olhv_perc_pos_dir = COALESCE(EXCLUDED.wst_6_axle_multi_trailer_olhv_perc_pos_dir,wst_6_axle_multi_trailer_olhv_perc_pos_dir),
    wst_7_axle_multi_trailer_olhv_perc_pos_dir = COALESCE(EXCLUDED.wst_7_axle_multi_trailer_olhv_perc_pos_dir,wst_7_axle_multi_trailer_olhv_perc_pos_dir),
    wst_8_or_more_axle_multi_trailer_olhv_perc_pos_dir = COALESCE(EXCLUDED.wst_8_or_more_axle_multi_trailer_olhv_perc_pos_dir,wst_8_or_more_axle_multi_trailer_olhv_perc_pos_dir),
    wst_2_axle_busses_tonperhv_pos_dir = COALESCE(EXCLUDED.wst_2_axle_busses_tonperhv_pos_dir,wst_2_axle_busses_tonperhv_pos_dir),
    wst_2_axle_6_tyre_single_units_tonperhv_pos_dir = COALESCE(EXCLUDED.wst_2_axle_6_tyre_single_units_tonperhv_pos_dir,wst_2_axle_6_tyre_single_units_tonperhv_pos_dir),
    wst_busses_with_3_or_4_axles_tonperhv_pos_dir = COALESCE(EXCLUDED.wst_busses_with_3_or_4_axles_tonperhv_pos_dir,wst_busses_with_3_or_4_axles_tonperhv_pos_dir),
    wst_2_axle_6tyre_su_with_trailer_4axles_max_tonperhv_pos_dir = COALESCE(EXCLUDED.wst_2_axle_6tyre_su_with_trailer_4axles_max_tonperhv_pos_dir,wst_2_axle_6tyre_su_with_trailer_4axles_max_tonperhv_pos_dir),
    wst_3_axle_su_incl_single_axle_trailer_tonperhv_pos_dir = COALESCE(EXCLUDED.wst_3_axle_su_incl_single_axle_trailer_tonperhv_pos_dir,wst_3_axle_su_incl_single_axle_trailer_tonperhv_pos_dir),
    wst_4_or_less_axle_incl_a_single_trailer_tonperhv_pos_dir = COALESCE(EXCLUDED.wst_4_or_less_axle_incl_a_single_trailer_tonperhv_pos_dir,wst_4_or_less_axle_incl_a_single_trailer_tonperhv_pos_dir),
    wst_busses_with_5_or_more_axles_tonperhv_pos_dir = COALESCE(EXCLUDED.wst_busses_with_5_or_more_axles_tonperhv_pos_dir,wst_busses_with_5_or_more_axles_tonperhv_pos_dir),
    wst_3_axle_su_and_trailer_more_than_4_axles_tonperhv_pos_dir = COALESCE(EXCLUDED.wst_3_axle_su_and_trailer_more_than_4_axles_tonperhv_pos_dir,wst_3_axle_su_and_trailer_more_than_4_axles_tonperhv_pos_dir),
    wst_5_axle_single_trailer_tonperhv_pos_dir = COALESCE(EXCLUDED.wst_5_axle_single_trailer_tonperhv_pos_dir,wst_5_axle_single_trailer_tonperhv_pos_dir),
    wst_6_axle_single_trailer_tonperhv_pos_dir = COALESCE(EXCLUDED.wst_6_axle_single_trailer_tonperhv_pos_dir,wst_6_axle_single_trailer_tonperhv_pos_dir),
    wst_5_or_less_axle_multi_trailer_tonperhv_pos_dir = COALESCE(EXCLUDED.wst_5_or_less_axle_multi_trailer_tonperhv_pos_dir,wst_5_or_less_axle_multi_trailer_tonperhv_pos_dir),
    wst_6_axle_multi_trailer_tonperhv_pos_dir = COALESCE(EXCLUDED.wst_6_axle_multi_trailer_tonperhv_pos_dir,wst_6_axle_multi_trailer_tonperhv_pos_dir),
    wst_7_axle_multi_trailer_tonperhv_pos_dir = COALESCE(EXCLUDED.wst_7_axle_multi_trailer_tonperhv_pos_dir,wst_7_axle_multi_trailer_tonperhv_pos_dir),
    wst_8_or_more_axle_multi_trailer_tonperhv_pos_dir = COALESCE(EXCLUDED.wst_8_or_more_axle_multi_trailer_tonperhv_pos_dir,wst_8_or_more_axle_multi_trailer_tonperhv_pos_dir),
    wst_2_axle_busses_cnt_neg_dir = COALESCE(EXCLUDED.wst_2_axle_busses_cnt_neg_dir,wst_2_axle_busses_cnt_neg_dir),
    wst_2_axle_6_tyre_single_units_cnt_neg_dir = COALESCE(EXCLUDED.wst_2_axle_6_tyre_single_units_cnt_neg_dir,wst_2_axle_6_tyre_single_units_cnt_neg_dir),
    wst_busses_with_3_or_4_axles_cnt_neg_dir = COALESCE(EXCLUDED.wst_busses_with_3_or_4_axles_cnt_neg_dir,wst_busses_with_3_or_4_axles_cnt_neg_dir),
    wst_2_axle_6tyre_su_with_trailer_4axles_max_cnt_neg_dir = COALESCE(EXCLUDED.wst_2_axle_6tyre_su_with_trailer_4axles_max_cnt_neg_dir,wst_2_axle_6tyre_su_with_trailer_4axles_max_cnt_neg_dir),
    wst_3_axle_su_incl_single_axle_trailer_cnt_neg_dir = COALESCE(EXCLUDED.wst_3_axle_su_incl_single_axle_trailer_cnt_neg_dir,wst_3_axle_su_incl_single_axle_trailer_cnt_neg_dir),
    wst_4_or_less_axle_incl_a_single_trailer_cnt_neg_dir = COALESCE(EXCLUDED.wst_4_or_less_axle_incl_a_single_trailer_cnt_neg_dir,wst_4_or_less_axle_incl_a_single_trailer_cnt_neg_dir),
    wst_busses_with_5_or_more_axles_cnt_neg_dir = COALESCE(EXCLUDED.wst_busses_with_5_or_more_axles_cnt_neg_dir,wst_busses_with_5_or_more_axles_cnt_neg_dir),
    wst_3_axle_su_and_trailer_more_than_4_axles_cnt_neg_dir = COALESCE(EXCLUDED.wst_3_axle_su_and_trailer_more_than_4_axles_cnt_neg_dir,wst_3_axle_su_and_trailer_more_than_4_axles_cnt_neg_dir),
    wst_5_axle_single_trailer_cnt_neg_dir = COALESCE(EXCLUDED.wst_5_axle_single_trailer_cnt_neg_dir,wst_5_axle_single_trailer_cnt_neg_dir),
    wst_6_axle_single_trailer_cnt_neg_dir = COALESCE(EXCLUDED.wst_6_axle_single_trailer_cnt_neg_dir,wst_6_axle_single_trailer_cnt_neg_dir),
    wst_5_or_less_axle_multi_trailer_cnt_neg_dir = COALESCE(EXCLUDED.wst_5_or_less_axle_multi_trailer_cnt_neg_dir,wst_5_or_less_axle_multi_trailer_cnt_neg_dir),
    wst_6_axle_multi_trailer_cnt_neg_dir = COALESCE(EXCLUDED.wst_6_axle_multi_trailer_cnt_neg_dir,wst_6_axle_multi_trailer_cnt_neg_dir),
    wst_7_axle_multi_trailer_cnt_neg_dir = COALESCE(EXCLUDED.wst_7_axle_multi_trailer_cnt_neg_dir,wst_7_axle_multi_trailer_cnt_neg_dir),
    wst_8_or_more_axle_multi_trailer_cnt_neg_dir = COALESCE(EXCLUDED.wst_8_or_more_axle_multi_trailer_cnt_neg_dir,wst_8_or_more_axle_multi_trailer_cnt_neg_dir),
    wst_2_axle_busses_olhv_perc_neg_dir = COALESCE(EXCLUDED.wst_2_axle_busses_olhv_perc_neg_dir,wst_2_axle_busses_olhv_perc_neg_dir),
    wst_2_axle_6_tyre_single_units_olhv_perc_neg_dir = COALESCE(EXCLUDED.wst_2_axle_6_tyre_single_units_olhv_perc_neg_dir,wst_2_axle_6_tyre_single_units_olhv_perc_neg_dir),
    wst_busses_with_3_or_4_axles_olhv_perc_neg_dir = COALESCE(EXCLUDED.wst_busses_with_3_or_4_axles_olhv_perc_neg_dir,wst_busses_with_3_or_4_axles_olhv_perc_neg_dir),
    wst_2_axle_6tyre_su_with_trailer_4axles_max_olhv_perc_neg_dir = COALESCE(EXCLUDED.wst_2_axle_6tyre_su_with_trailer_4axles_max_olhv_perc_neg_dir,wst_2_axle_6tyre_su_with_trailer_4axles_max_olhv_perc_neg_dir),
    wst_3_axle_su_incl_single_axle_trailer_olhv_perc_neg_dir = COALESCE(EXCLUDED.wst_3_axle_su_incl_single_axle_trailer_olhv_perc_neg_dir,wst_3_axle_su_incl_single_axle_trailer_olhv_perc_neg_dir),
    wst_4_or_less_axle_incl_a_single_trailer_olhv_perc_neg_dir = COALESCE(EXCLUDED.wst_4_or_less_axle_incl_a_single_trailer_olhv_perc_neg_dir,wst_4_or_less_axle_incl_a_single_trailer_olhv_perc_neg_dir),
    wst_busses_with_5_or_more_axles_olhv_perc_neg_dir = COALESCE(EXCLUDED.wst_busses_with_5_or_more_axles_olhv_perc_neg_dir,wst_busses_with_5_or_more_axles_olhv_perc_neg_dir),
    wst_3_axle_su_and_trailer_more_than_4_axles_olhv_perc_neg_dir = COALESCE(EXCLUDED.wst_3_axle_su_and_trailer_more_than_4_axles_olhv_perc_neg_dir,wst_3_axle_su_and_trailer_more_than_4_axles_olhv_perc_neg_dir),
    wst_5_axle_single_trailer_olhv_perc_neg_dir = COALESCE(EXCLUDED.wst_5_axle_single_trailer_olhv_perc_neg_dir,wst_5_axle_single_trailer_olhv_perc_neg_dir),
    wst_6_axle_single_trailer_olhv_perc_neg_dir = COALESCE(EXCLUDED.wst_6_axle_single_trailer_olhv_perc_neg_dir,wst_6_axle_single_trailer_olhv_perc_neg_dir),
    wst_5_or_less_axle_multi_trailer_olhv_perc_neg_dir = COALESCE(EXCLUDED.wst_5_or_less_axle_multi_trailer_olhv_perc_neg_dir,wst_5_or_less_axle_multi_trailer_olhv_perc_neg_dir),
    wst_6_axle_multi_trailer_olhv_perc_neg_dir = COALESCE(EXCLUDED.wst_6_axle_multi_trailer_olhv_perc_neg_dir,wst_6_axle_multi_trailer_olhv_perc_neg_dir),
    wst_7_axle_multi_trailer_olhv_perc_neg_dir = COALESCE(EXCLUDED.wst_7_axle_multi_trailer_olhv_perc_neg_dir,wst_7_axle_multi_trailer_olhv_perc_neg_dir),
    wst_8_or_more_axle_multi_trailer_olhv_perc_neg_dir = COALESCE(EXCLUDED.wst_8_or_more_axle_multi_trailer_olhv_perc_neg_dir,wst_8_or_more_axle_multi_trailer_olhv_perc_neg_dir),
    wst_2_axle_busses_tonperhv_neg_dir = COALESCE(EXCLUDED.wst_2_axle_busses_tonperhv_neg_dir,wst_2_axle_busses_tonperhv_neg_dir),
    wst_2_axle_6_tyre_single_units_tonperhv_neg_dir = COALESCE(EXCLUDED.wst_2_axle_6_tyre_single_units_tonperhv_neg_dir,wst_2_axle_6_tyre_single_units_tonperhv_neg_dir),
    wst_busses_with_3_or_4_axles_tonperhv_neg_dir = COALESCE(EXCLUDED.wst_busses_with_3_or_4_axles_tonperhv_neg_dir,wst_busses_with_3_or_4_axles_tonperhv_neg_dir),
    wst_2_axle_6tyre_su_with_trailer_4axles_max_tonperhv_neg_dir = COALESCE(EXCLUDED.wst_2_axle_6tyre_su_with_trailer_4axles_max_tonperhv_neg_dir,wst_2_axle_6tyre_su_with_trailer_4axles_max_tonperhv_neg_dir),
    wst_3_axle_su_incl_single_axle_trailer_tonperhv_neg_dir = COALESCE(EXCLUDED.wst_3_axle_su_incl_single_axle_trailer_tonperhv_neg_dir,wst_3_axle_su_incl_single_axle_trailer_tonperhv_neg_dir),
    wst_4_or_less_axle_incl_a_single_trailer_tonperhv_neg_dir = COALESCE(EXCLUDED.wst_4_or_less_axle_incl_a_single_trailer_tonperhv_neg_dir,wst_4_or_less_axle_incl_a_single_trailer_tonperhv_neg_dir),
    wst_busses_with_5_or_more_axles_tonperhv_neg_dir = COALESCE(EXCLUDED.wst_busses_with_5_or_more_axles_tonperhv_neg_dir,wst_busses_with_5_or_more_axles_tonperhv_neg_dir),
    wst_3_axle_su_and_trailer_more_than_4_axles_tonperhv_neg_dir = COALESCE(EXCLUDED.wst_3_axle_su_and_trailer_more_than_4_axles_tonperhv_neg_dir,wst_3_axle_su_and_trailer_more_than_4_axles_tonperhv_neg_dir),
    wst_5_axle_single_trailer_tonperhv_neg_dir = COALESCE(EXCLUDED.wst_5_axle_single_trailer_tonperhv_neg_dir,wst_5_axle_single_trailer_tonperhv_neg_dir),
    wst_6_axle_single_trailer_tonperhv_neg_dir = COALESCE(EXCLUDED.wst_6_axle_single_trailer_tonperhv_neg_dir,wst_6_axle_single_trailer_tonperhv_neg_dir),
    wst_5_or_less_axle_multi_trailer_tonperhv_neg_dir = COALESCE(EXCLUDED.wst_5_or_less_axle_multi_trailer_tonperhv_neg_dir,wst_5_or_less_axle_multi_trailer_tonperhv_neg_dir),
    wst_6_axle_multi_trailer_tonperhv_neg_dir = COALESCE(EXCLUDED.wst_6_axle_multi_trailer_tonperhv_neg_dir,wst_6_axle_multi_trailer_tonperhv_neg_dir),
    wst_7_axle_multi_trailer_tonperhv_neg_dir = COALESCE(EXCLUDED.wst_7_axle_multi_trailer_tonperhv_neg_dir,wst_7_axle_multi_trailer_tonperhv_neg_dir),
    wst_8_or_more_axle_multi_trailer_tonperhv_neg_dir = COALESCE(EXCLUDED.wst_8_or_more_axle_multi_trailer_tonperhv_neg_dir,wst_8_or_more_axle_multi_trailer_tonperhv_neg_dir),
    wst_2_axle_busses_cnt = COALESCE(EXCLUDED.wst_2_axle_busses_cnt,wst_2_axle_busses_cnt),
    wst_2_axle_6_tyre_single_units_cnt = COALESCE(EXCLUDED.wst_2_axle_6_tyre_single_units_cnt,wst_2_axle_6_tyre_single_units_cnt),
    wst_busses_with_3_or_4_axles_cnt = COALESCE(EXCLUDED.wst_busses_with_3_or_4_axles_cnt,wst_busses_with_3_or_4_axles_cnt),
    wst_2_axle_6tyre_su_with_trailer_4axles_max_cnt = COALESCE(EXCLUDED.wst_2_axle_6tyre_su_with_trailer_4axles_max_cnt,wst_2_axle_6tyre_su_with_trailer_4axles_max_cnt),
    wst_3_axle_su_incl_single_axle_trailer_cnt = COALESCE(EXCLUDED.wst_3_axle_su_incl_single_axle_trailer_cnt,wst_3_axle_su_incl_single_axle_trailer_cnt),
    wst_4_or_less_axle_incl_a_single_trailer_cnt = COALESCE(EXCLUDED.wst_4_or_less_axle_incl_a_single_trailer_cnt,wst_4_or_less_axle_incl_a_single_trailer_cnt),
    wst_busses_with_5_or_more_axles_cnt = COALESCE(EXCLUDED.wst_busses_with_5_or_more_axles_cnt,wst_busses_with_5_or_more_axles_cnt),
    wst_3_axle_su_and_trailer_more_than_4_axles_cnt = COALESCE(EXCLUDED.wst_3_axle_su_and_trailer_more_than_4_axles_cnt,wst_3_axle_su_and_trailer_more_than_4_axles_cnt),
    wst_5_axle_single_trailer_cnt = COALESCE(EXCLUDED.wst_5_axle_single_trailer_cnt,wst_5_axle_single_trailer_cnt),
    wst_6_axle_single_trailer_cnt = COALESCE(EXCLUDED.wst_6_axle_single_trailer_cnt,wst_6_axle_single_trailer_cnt),
    wst_5_or_less_axle_multi_trailer_cnt = COALESCE(EXCLUDED.wst_5_or_less_axle_multi_trailer_cnt,wst_5_or_less_axle_multi_trailer_cnt),
    wst_6_axle_multi_trailer_cnt = COALESCE(EXCLUDED.wst_6_axle_multi_trailer_cnt,wst_6_axle_multi_trailer_cnt),
    wst_7_axle_multi_trailer_cnt = COALESCE(EXCLUDED.wst_7_axle_multi_trailer_cnt,wst_7_axle_multi_trailer_cnt),
    wst_8_or_more_axle_multi_trailer_cnt = COALESCE(EXCLUDED.wst_8_or_more_axle_multi_trailer_cnt,wst_8_or_more_axle_multi_trailer_cnt),
    wst_2_axle_busses_olhv_perc = COALESCE(EXCLUDED.wst_2_axle_busses_olhv_perc,wst_2_axle_busses_olhv_perc),
    wst_2_axle_6_tyre_single_units_olhv_perc = COALESCE(EXCLUDED.wst_2_axle_6_tyre_single_units_olhv_perc,wst_2_axle_6_tyre_single_units_olhv_perc),
    wst_busses_with_3_or_4_axles_olhv_perc = COALESCE(EXCLUDED.wst_busses_with_3_or_4_axles_olhv_perc,wst_busses_with_3_or_4_axles_olhv_perc),
    wst_2_axle_6tyre_su_with_trailer_4axles_max_olhv_perc = COALESCE(EXCLUDED.wst_2_axle_6tyre_su_with_trailer_4axles_max_olhv_perc,wst_2_axle_6tyre_su_with_trailer_4axles_max_olhv_perc),
    wst_3_axle_su_incl_single_axle_trailer_olhv_perc = COALESCE(EXCLUDED.wst_3_axle_su_incl_single_axle_trailer_olhv_perc,wst_3_axle_su_incl_single_axle_trailer_olhv_perc),
    wst_4_or_less_axle_incl_a_single_trailer_olhv_perc = COALESCE(EXCLUDED.wst_4_or_less_axle_incl_a_single_trailer_olhv_perc,wst_4_or_less_axle_incl_a_single_trailer_olhv_perc),
    wst_busses_with_5_or_more_axles_olhv_perc = COALESCE(EXCLUDED.wst_busses_with_5_or_more_axles_olhv_perc,wst_busses_with_5_or_more_axles_olhv_perc),
    wst_3_axle_su_and_trailer_more_than_4_axles_olhv_perc = COALESCE(EXCLUDED.wst_3_axle_su_and_trailer_more_than_4_axles_olhv_perc,wst_3_axle_su_and_trailer_more_than_4_axles_olhv_perc),
    wst_5_axle_single_trailer_olhv_perc = COALESCE(EXCLUDED.wst_5_axle_single_trailer_olhv_perc,wst_5_axle_single_trailer_olhv_perc),
    wst_6_axle_single_trailer_olhv_perc = COALESCE(EXCLUDED.wst_6_axle_single_trailer_olhv_perc,wst_6_axle_single_trailer_olhv_perc),
    wst_5_or_less_axle_multi_trailer_olhv_perc = COALESCE(EXCLUDED.wst_5_or_less_axle_multi_trailer_olhv_perc,wst_5_or_less_axle_multi_trailer_olhv_perc),
    wst_6_axle_multi_trailer_olhv_perc = COALESCE(EXCLUDED.wst_6_axle_multi_trailer_olhv_perc,wst_6_axle_multi_trailer_olhv_perc),
    wst_7_axle_multi_trailer_olhv_perc = COALESCE(EXCLUDED.wst_7_axle_multi_trailer_olhv_perc,wst_7_axle_multi_trailer_olhv_perc),
    wst_8_or_more_axle_multi_trailer_olhv_perc = COALESCE(EXCLUDED.wst_8_or_more_axle_multi_trailer_olhv_perc,wst_8_or_more_axle_multi_trailer_olhv_perc),
    wst_2_axle_busses_tonperhv = COALESCE(EXCLUDED.wst_2_axle_busses_tonperhv,wst_2_axle_busses_tonperhv),
    wst_2_axle_6_tyre_single_units_tonperhv = COALESCE(EXCLUDED.wst_2_axle_6_tyre_single_units_tonperhv,wst_2_axle_6_tyre_single_units_tonperhv),
    wst_busses_with_3_or_4_axles_tonperhv = COALESCE(EXCLUDED.wst_busses_with_3_or_4_axles_tonperhv,wst_busses_with_3_or_4_axles_tonperhv),
    wst_2_axle_6tyre_su_with_trailer_4axles_max_tonperhv = COALESCE(EXCLUDED.wst_2_axle_6tyre_su_with_trailer_4axles_max_tonperhv,wst_2_axle_6tyre_su_with_trailer_4axles_max_tonperhv),
    wst_3_axle_su_incl_single_axle_trailer_tonperhv = COALESCE(EXCLUDED.wst_3_axle_su_incl_single_axle_trailer_tonperhv,wst_3_axle_su_incl_single_axle_trailer_tonperhv),
    wst_4_or_less_axle_incl_a_single_trailer_tonperhv = COALESCE(EXCLUDED.wst_4_or_less_axle_incl_a_single_trailer_tonperhv,wst_4_or_less_axle_incl_a_single_trailer_tonperhv),
    wst_busses_with_5_or_more_axles_tonperhv = COALESCE(EXCLUDED.wst_busses_with_5_or_more_axles_tonperhv,wst_busses_with_5_or_more_axles_tonperhv),
    wst_3_axle_su_and_trailer_more_than_4_axles_tonperhv = COALESCE(EXCLUDED.wst_3_axle_su_and_trailer_more_than_4_axles_tonperhv,wst_3_axle_su_and_trailer_more_than_4_axles_tonperhv),
    wst_5_axle_single_trailer_tonperhv = COALESCE(EXCLUDED.wst_5_axle_single_trailer_tonperhv,wst_5_axle_single_trailer_tonperhv),
    wst_6_axle_single_trailer_tonperhv = COALESCE(EXCLUDED.wst_6_axle_single_trailer_tonperhv,wst_6_axle_single_trailer_tonperhv),
    wst_5_or_less_axle_multi_trailer_tonperhv = COALESCE(EXCLUDED.wst_5_or_less_axle_multi_trailer_tonperhv,wst_5_or_less_axle_multi_trailer_tonperhv),
    wst_6_axle_multi_trailer_tonperhv = COALESCE(EXCLUDED.wst_6_axle_multi_trailer_tonperhv,wst_6_axle_multi_trailer_tonperhv),
    wst_7_axle_multi_trailer_tonperhv = COALESCE(EXCLUDED.wst_7_axle_multi_trailer_tonperhv,wst_7_axle_multi_trailer_tonperhv),
    wst_8_or_more_axle_multi_trailer_tonperhv = COALESCE(EXCLUDED.wst_8_or_more_axle_multi_trailer_tonperhv,wst_8_or_more_axle_multi_trailer_tonperhv)
    ;
"""
    return UPSERT_STRING

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
    files = get_files(path)

    for file in files:
        try:
            main(file)
        except Exception as exc:
            traceback.print_exc()
        
def main_type10(df: pd.DataFrame, sub_data_df: pd.DataFrame, file: str):
    try:
        df = df[df.columns.intersection(t10_cols)]
        try:
            tools.push_to_db(df, config.TYPE_10_TBL_NAME)
        except (UniqueViolation, NotNullViolation):
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
            push_to_db(wx_data, config.WX_TABLE)
            
        if ax_data.empty:
            pass
        else:
            push_to_db(ax_data, config.AX_TABLE)
            
        if gx_data.empty:
            pass
        else:
            push_to_db(gx_data, config.GX_TABLE)
            
        if sx_data.empty:
            pass
        else:
            sx_data.rename(columns = {"value":"axle_spacing_cm", "number":"axle_spacing_number", "id":"type10_id"}, inplace=True)
            sx_data = sx_data.drop(["offset_sensor_detection_code","mass_measurement_resolution_kg"], axis=1)
            push_to_db(sx_data, config.SX_TABLE)
            
        if tx_data.empty:
            pass
        else:
            tx_data.rename(columns = {"value":"tyre_code", "number":"tyre_number", "id":"type10_id"}, inplace=True)
            tx_data = tx_data.drop(["offset_sensor_detection_code","mass_measurement_resolution_kg"], axis=1)
            push_to_db(tx_data, config.TX_TABLE)
            
        if cx_data.empty:
            pass
        else:
            push_to_db(cx_data, config.CX_TABLE)
            
        if vx_data.empty:
            pass
        else:
            vx_data.rename(columns = {"value":"group_axle_count", "offset_sensor_detection_code":"vehicle_registration_number" ,"number":"group_axle_number", "id":"type10_id"}, inplace=True)
            vx_data = vx_data.drop(["mass_measurement_resolution_kg"], axis=1)
            push_to_db(vx_data, config.VX_TABLE)
                        
        with open(os.path.expanduser(config.FILES_COMPLETE),"a",newline="",) as f:
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

def main(file: str):
    print("--- Busy With "+file)
    T = Traffic(file)
    head_df = T.head_df
    header = T.header_df

    if header is None:
        pass
    else:
        lanes = T.lanes
        lanes = lanes[lanes.columns.intersection(lane_cols)]

        if lanes is not None:
            push_to_db(lanes, config.LANES_TBL_NAME)
        else:
            pass

        if not head_df.loc[head_df[0] == "21"].empty:
            data = T.type_21()
            header = T.header_calcs(header, data, 21)
            data.rename(
                    columns=config.ELECTRONIC_COUNT_DATA_TYPE21_NAME_CHANGE, inplace=True)
            data = data[data.columns.intersection(t21_cols)]
            push_to_db(data, config.TYPE_21_TBL_NAME)
        else:
            pass
        
        if not head_df.loc[head_df[0] == "30"].empty:
            data = T.type_30()
            header = T.header_calcs(header, data, 30)
            push_to_db(data, config.TYPE_30_TBL_NAME)
        else:
            pass

        if not head_df.loc[head_df[0] == "60"].empty:        
            data = T.type_60()
            header =  T.header_calcs(header, data, 60)
            data = data[data.columns.intersection(t60_cols)]
            push_to_db(data, config.TYPE_70_TBL_NAME)
        else:
            pass
        
        if not head_df.loc[head_df[0] == "70"].empty:        
            data = T.type_70()
            header =  T.header_calcs(header, data, 70)
            data = data[data.columns.intersection(t70_cols)]
            push_to_db(data, config.TYPE_70_TBL_NAME)
        else:
            pass

        # data = T.type_10()
        # if data.empty:
        #     pass
        # else:
        #     try:
        #         data, sub_data = T.type_10()
        #         header = T.header_calcs(header, type_10_data, 10)
        #         type_10_data = main_type10(data, sub_data, file)
        #     except:
        #         pass

        if header is not None:
            try:
                header = header[header.columns.intersection(h_cols)]
                push_to_db(header, config.HEADER_TBL_NAME)
            except AttributeError as exc:
                raise Exception("Issue with HEADER "+exc) from exc
        else:
            pass

        print('DONE WITH '+file)
        with open(
            os.path.expanduser(config.FILES_COMPLETE),
            "a",
            newline="",
        ) as f:
            write = csv.writer(f)
            write.writerows([[file]])

if __name__ == "__main__":
    PATH = config.PATH
    PATH = r"C:\PQ410"
    testfile = r"C:\FTP\Syntell\0087_20220331.RSA"
        
    # run_multiprocess(PATH)

        # time.sleep(10)
        # run_multiprocess(PATH)

    run_individually(PATH)

    # T = Traffic(testfile)
    # head_df = T.head_df
    # d = T.sum_data_df
    # t30 = T.type_30()
    # t10 = T.type_10()
    # print(d.columns)

    # data = d.loc[(d[0] == "30")].dropna(
    #                 axis=1, how="all"
    #             ).reset_index(drop=True).copy()
    # print(data)
    # print(t10)
    # print(t30)
    # print(int(d['start_datetime'].at[0].year))
    
    print("COMPLETE")
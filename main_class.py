import pandas as pd
import numpy as np
pd.options.mode.chained_assignment = None
pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", 20)

from pandasql import sqldf
from sqlalchemy.dialects.postgresql import insert
from datetime import timedelta, date
import uuid
import gc
from typing import List
import time

import config
import queries as q
from rsa_data_summary import get_direction

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

class Traffic():

    def __init__(self, file) -> None:
        self.file = file
        self.df = self.to_df()
        self.header_df = self.get_head(self.df)
        self.site_id = self.header_df.loc[self.header_df[0]=="S0", 1].iat[0]
        self.lanes = self.get_lanes(self.header_df)
        self.header_df = self.header(self.header_df)
        self.data_df = self.get_data(self.df)
        self.data_df = self.process_data_datetimes(self.data_df)
        self.data_df = self.get_direction(self.data_df)
        self.type_21 = None
        self.type_70 = None
        self.type_30 = None
        self.type_60 = None

    def to_df(self) -> pd.DataFrame:
        try:
            df = pd.read_csv(self.file, header=None)
            df = df[0].str.split("\s+|,\s+|,|;|;\s+", expand=True)
        except (pd.errors.ParserError, AttributeError, ValueError):
            df = pd.read_csv(self.file, header=None, sep="\s+|\n|\t|,")
            df = df[0].str.split("\s+|,\s+|,|;|;\s+", expand=True)
        except Exception as exc:
            print(exc)
        return df

    def get_head(self, df) -> pd.DataFrame:
        df = pd.DataFrame(df.loc[
            (df[0].isin(["H0", "S0", "I0", "S1", "D0", "D1", "D3", "L0", "L1"]))
            | (
                (df[0].isin(["21", "70", "30", "13", "60"]))
                & (df.loc[df[3].str.len() < 3]).all()
            )
            | (
                (df[0].isin(["10"]))
                & (df[1].isin(["1", "8", "5", "9", "01", "08", "05", "09"]))
            )
        ]).dropna(axis=1, how="all").reset_index(drop=True).copy()
        return df

    def get_data(self, df: pd.DataFrame)-> pd.DataFrame:
        df = pd.DataFrame(df.loc[
            (~df[0].isin(["H0", "H9", "S0", "I0", "S1", "D0", "D1", "D3", "L0", "L1"]))
            & (
                (df[0].isin(["21", "22", "70", "30", "31", "13", "60"]))
                & (df[1].isin(["0", "1", "2", "3", "4"]))
                & ((df.loc[df[3].str.len() > 3]).all())
            )
            | (
                (df[0].isin(["10"]))
                & (~df[1].isin(["1", "8", "5", "9", "01", "08", "05", "09"]))
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
                    pd.to_numeric, errors='ignore')
            lanes["site_name"] = self.site_id
        else:
            pass
        return lanes

    def process_data_datetimes(self, df: pd.DataFrame) -> pd.DataFrame:
        date_length = len(df[2].at[0])
        duration_min = int(df[4].at[0])

        df[3] = df[3].astype(str)

        df[3] = df[3].str.pad(width=7,side='right',fillchar="0")
        df[3].loc[df[3].str[:2] == '24'] = ('0').zfill(7)

        if date_length == 6:
            decade = int(df[2].at[0][:2])
            if decade < 50:
                century = str(date.today())[:2]
            else:
                century = int(str(date.today())[:2])-1

            df[2] = str(century) + df[2]
            df[2] = df[2].apply(lambda x: pd.to_datetime(x, format="%Y%m%d").date() + timedelta(days=1)
                if x[3] in ['0'.zfill(7),'24'.ljust(7,'0')] else pd.to_datetime(x, format="%Y%m%d").date())
        elif date_length == 8:
            df[2] = df[2].apply(lambda x: pd.to_datetime(x, format="%Y%m%d").date() + timedelta(days=1)
                if x[3] in ['0'.zfill(7),'24'.ljust(7,'0')] else pd.to_datetime(x, format="%Y%m%d").date())
        else:
            raise Exception("DATA Date length abnormal")

        df[3] = df[3].apply(lambda x: pd.to_datetime(x, format="%H%M%S%f").time())

        if (df[2].astype(str)+df[3].astype(str)).map(len).isin([18]).all():
            df['end_datetime'] = pd.to_datetime(df[2].astype(str)+df[3].astype(str), 
                format='%Y-%m-%d%H:%M:%S')
        else:
            df['end_datetime'] = pd.to_datetime((df[2].astype(str)+df[3].astype(str)), 
                format='%Y-%m-%d%H:%M:%S.%f')

        if (df[2].astype(str)+df[3].astype(str)).map(len).isin([18]).all():
            df['start_datetime'] = pd.to_datetime(df[2].astype(str)+df[3].astype(str), 
                format='%Y-%m-%d%H:%M:%S') - timedelta(minutes=duration_min)
        else:
            df['start_datetime'] = pd.to_datetime((df[2].astype(str)+df[3].astype(str)), 
                format='%Y-%m-%d%H:%M:%S.%f') - timedelta(minutes=duration_min)

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

        try:
            headers["type_10_vehicle_classification_scheme_primary"] = int(df.loc[
                df[0] == "10", 1].drop_duplicates().reset_index(drop=True)[0])
            headers["type_10_vehicle_classification_scheme_secondary"] = int(df.loc[
                df[0] == "10", 2].drop_duplicates().reset_index(drop=True)[0])
            headers["type_10_maximum_gap_milliseconds"] = int(df.loc[
                df[0] == "10", 3].drop_duplicates().reset_index(drop=True)[0])
            headers["type_10_maximum_differential_speed"] = int(df.loc[
                df[0] == "10", 4].drop_duplicates().reset_index(drop=True)[0])
        except KeyError:
            pass

        headers = headers.reset_index(drop=True)

        m = headers.select_dtypes(np.number)
        headers[m.columns] = m.round().astype('Int32')

        return headers

    def type_30(self) -> pd.DataFrame:
        data = self.data_df.loc[(self.data_df[0] == "30")].dropna(
                    axis=1, how="all"
                ).reset_index(drop=True).copy()
        header = self.header_df.loc[(self.header_df[0] == "30")].dropna(
                    axis=1, how="all"
                ).reset_index(drop=True).copy()

        if data.empty:
            pass
        else:
            if header.shape[1] > 3:
                classification_scheme = header.iloc[0,3]
                number_of_data_records = header.iloc[0,4]
            else:
                classification_scheme = header.iloc[0,2]
                number_of_data_records = header.iloc[0,3]

            # vc_df = select_classification_scheme(classification_scheme)

            if data[1].isin(["0", "2"]).any():
                ddf = data.iloc[:, 1:].reset_index(drop=True)
                ddf = pd.DataFrame(ddf).dropna(axis=1, how="all").reset_index(drop=True)

                duration_min = int(ddf[4][0])
                max_lanes = self.lanes['lane_number'].astype(int).max()

                ddf = get_direction(df)

                ddf[ddf.select_dtypes(include=['object']).columns] = ddf[
                    ddf.select_dtypes(include=['object']).columns].apply(
                    pd.to_numeric, errors='ignore')
                    
                ddf['vehicle_classification_scheme'] = int(classification_scheme)

                ddf['start_datetime'] = pd.to_datetime(ddf[2].astype(str)+ddf[3].astype(str), 
                    format='%Y-%m-%d%H:%M:%S') - timedelta(minutes=duration_min)
                
                ddf.columns = ddf.columns.astype(str)

                df3 = pd.DataFrame(columns=['edit_code', 'start_datetime', 'end_date', 'end_time', 
                    'duration_of_summary', 'lane_number', 'number_of_vehicles', 'class_number', 'direction', 'compass_heading'])
                for lane_no in range(1, max_lanes+1):
                    for i in range(6,int(number_of_data_records)+6):
                        join_to_df3 = ddf.loc[ddf['5'] == lane_no, ['1', 'start_datetime','2', '3', '4', '5',str(i), 'direction', 'compass_heading']]
                        join_to_df3['class_number'] = i-5
                        join_to_df3.rename(columns={
                            '1':"edit_code",
                            '2':"end_date",
                            '3':"end_time",
                            '4':"duration_of_summary",
                            '5':'lane_number', 
                            str(i): 'number_of_vehicles'
                            }, inplace=True)
                        df3 = pd.concat([df3,join_to_df3],keys=['start_datetime','lane_number','number_of_vehicles','class_number'],ignore_index=True, axis=0)
                df3['classification_scheme'] = int(classification_scheme)
                df3['site_id'] = self.site_id
                df3['year'] = int(df3['start_datetime'][0].year)
            else:
                pass
            return df3    

    def type_21(self) -> pd.DataFrame:
        data = self.data_df.loc[(self.data_df[0] == "21")].dropna(
            axis=1, how="all"
        ).reset_index(drop=True).copy()

        if data.empty:
            pass
        else:
            if (data[1] == "0").any():
                ddf = data.iloc[:, 2:]

                ddf = get_direction(ddf)

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
                

            elif (data[1] == "1").any():
                ddf = data.iloc[:, 3:]
                
                ddf = get_direction(ddf)

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

            ddf['year'] = ddf['start_datetime'].dt.year
            ddf["site_id"] = self.site_id

            ddf = ddf.drop_duplicates()
            ddf["start_datetime"] = ddf["start_datetime"].astype("datetime64[ns]")

            return ddf

    def type_70(self) -> pd.DataFrame:
        data = df.loc[(df[0] == "70")].dropna(
            axis=1, how="all"
        ).reset_index(drop=True).copy()

        if data.empty:
            pass
        else:
            if data[1].all() == "1":
                ddf = data.iloc[:, 3:]
                ddf = pd.DataFrame(ddf).dropna(axis=1, how="all").reset_index(drop=True)

                ddf = get_direction(ddf)
                
                ddf.columns = [
                    "end_datetime",
                    "end_time",
                    "duration_min",
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
                    "sum_of_squared_speeds_for_following_heavies",
                ]

            else:  # data[1].all() == '0':
                ddf = data.iloc[:, 2:]
                ddf = pd.DataFrame(ddf).dropna(axis=1, how="all").reset_index(drop=True)
                ddf.columns = [
                    "end_datetime",
                    "end_time",
                    "duration_min",
                    "lane_number",
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
                    "sum_of_squared_speeds_for_following_heavies",
                ]
            
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

            ddf["lane_number"] = ddf["lane_number"].astype(int)
            max_lanes = ddf["lane_number"].max()
            ddf["direction"] = ddf.apply(
                lambda x: "P" if x["lane_number"] <= (int(max_lanes) / 2) else "N",
                axis=1,
            )
            direction = dfh.loc[dfh[0] == "L1", 1:3].astype(int)
            direction = direction.drop_duplicates()


            ddf['year'] = ddf['start_datetime'].dt.year
            # t1 = dfh.loc[dfh[0] == "S0", 1].unique()
            ddf["site_id"] = str(dfh[dfh[0]=="S0"].reset_index(drop=True).iloc[0,1]).zfill(4)
            ddf["site_id"] = self.site_id

            ddf = ddf.drop_duplicates()
            ddf["start_datetime"] = ddf["start_datetime"].astype("datetime64[ns]")

            ddf['end_time'] = ddf['end_time'].apply(lambda x: '0000' if (x=='2400' and len(x)==4) 
            else '000000' if ((x=='240000' and len(x)==6))
            else x)
            ddf['end_time'] = ddf['end_time'].apply(lambda x: pd.to_datetime(x, format="%H%M").time()
            if len(x)==4 
            else pd.to_datetime(x, format="%H%M%S").time())

            return ddf

file = r"C:\FTP\Syntell\0087_20220331.RSA"

T = Traffic(file)

h = T.header_df
d = T.data_df
print(d)
import pandas as pd
import numpy as np
from datetime import timedelta, date
import uuid

# class Headers(object):
#     def __init__(self, df: pd.DataFrame, file: str):
#         self.df = Headers.get_head(df)
#         self.header_df = Headers.headers(self.df, file)
#         # self.headers_old = Headers.headers_insert_min(self.df, file)
#         self.lanes = Headers.lanes(self.df)

def get_head(df) -> pd.DataFrame:
    df = pd.DataFrame(df.loc[
            (df[0].isin(["H0", "S0", "I0", "S1", "D0", "D1", "D3", "L0", "L1", "L2", "L3","L4" ,"L5","L6","L7","L8","L9","L10","L11","L12"]))
            | (
                (df[0].isin(["21", "70", "30", "13", "60"]))
                & (~df[1].isin(["0", "1", "2", "3", "4"]))
            )
            | (
                (df[0].isin(["10"]))
                & (df[1].isin(["1", "8", "5", "9", "01", "08", "05", "09"]))
            )
        ]
    ).dropna(axis=1, how="all").reset_index(drop=True)
    return df

def header(df: pd.DataFrame, file: str) -> pd.DataFrame:
    header_start_end = df.loc[df[0].isin(["D1","D3"]), 0:4].reset_index(drop=True).copy()

    # adds century to year if it is not there
    header_start_end[1] = header_start_end[1].apply(lambda x: str(date.today())[:2] + x if len(x)==6 else x)
    header_start_end[3] = header_start_end[3].apply(lambda x: str(date.today())[:2] + x if len(x)==6 else x)

    header_start_end[1] = header_start_end[1].str.pad(width=8, side='right', fillchar="0")
    header_start_end[3] = header_start_end[3].str.pad(width=8, side='right', fillchar="0")

    # index 2 and 4 are time, this makes the time uniform length
    header_start_end[2] = header_start_end[2].str.pad(width=7,side='right',fillchar="0")
    header_start_end[4] = header_start_end[4].str.pad(width=7,side='right',fillchar="0")

    # this filters for time = 24H00m and makes it zero hour
    header_start_end[2].loc[header_start_end[2].str[:2] == '24'] = ('0').zfill(7)
    header_start_end[4].loc[header_start_end[4].str[:2] == '24'] = ('0').zfill(7)

    # adds a day if the hour is zero hour and changes string to dtetime.date
    try:
        header_start_end[1] = header_start_end[1].apply(lambda x: pd.to_datetime(x, format="%Y%m%d").date() + timedelta(days=1)
        if x[2] in ['0'.zfill(7),'24'.zfill(7)] else pd.to_datetime(x, format="%Y%m%d").date())
        header_start_end[3] = header_start_end[3].apply(lambda x: pd.to_datetime(x, format="%Y%m%d").date() + timedelta(days=1)
        if x[4] in ['0'.zfill(7),'24'.zfill(7)] else pd.to_datetime(x, format="%Y%m%d").date())
    # except ValueError:
    #     header_start_end[1] = header_start_end[1].apply(lambda x: pd.to_datetime(x[:8], format="%Y%m%d").date() + timedelta(days=1)
    #     if x[2] in ['0'.zfill(7),'24'.zfill(7)] else pd.to_datetime(x[:8], format="%Y%m%d").date())
    #     header_start_end[3] = header_start_end[3].apply(lambda x: pd.to_datetime(x[:8], format="%Y%m%d").date() + timedelta(days=1)
    #     if x[4] in ['0'.zfill(7),'24'.zfill(7)] else pd.to_datetime(x[:8], format="%Y%m%d").date())
    except:
        pass

# changes time string into datetime.time
    try:
        header_start_end[2] = header_start_end[2].apply(lambda x: pd.to_datetime(x, format="%H%M%S%f").time())
        header_start_end[4] = header_start_end[4].apply(lambda x: pd.to_datetime(x, format="%H%M%S%f").time())
    except ValueError:
        header_start_end[2] = header_start_end[2].apply(lambda x: pd.to_datetime(x[:7], format="%H%M%S%f").time())
        header_start_end[4] = header_start_end[4].apply(lambda x: pd.to_datetime(x[:7], format="%H%M%S%f").time())
    except:
        pass

    # creates start_datetime and end_datetime
    try:
        header_start_end["start_datetime"] = pd.to_datetime((header_start_end[1].astype(str)+header_start_end[2].astype(str)), 
            format='%Y-%m-%d%H:%M:%S')
        header_start_end["end_datetime"] = pd.to_datetime((header_start_end[3].astype(str)+header_start_end[4].astype(str)), 
            format='%Y-%m-%d%H:%M:%S')
    except Exception:
        pass


    header_start_end = header_start_end.iloc[:,1:].drop_duplicates()

    headers = pd.DataFrame()
    try:
        headers['start_datetime'] = header_start_end.groupby(header_start_end['start_datetime'].dt.year).min()['start_datetime']
        headers['end_datetime'] = header_start_end.groupby(header_start_end['end_datetime'].dt.year).max()['end_datetime']
    except:
        pass

    headers['site_id'] = df.loc[df[0] == "S0",1].drop_duplicates().reset_index(drop=True)[0]
    headers['site_id'] = headers['site_id'].astype(str)
    headers['document_url'] = file

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
        8:'speedbin9'})

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

def lanes(df: pd.DataFrame) -> pd.DataFrame:
    if not df.empty:
        site_name = df.loc[df[0]=="S0", 1].iat[0]
        df = df.loc[df[0]=="L1"].dropna(axis=1).drop_duplicates().reset_index(drop=True).copy()
        if df.shape[1] == 5:
            df.rename(columns={
                1 : "lane_number",
                2 : "direction_code",
                3 : "lane_type_code",
                4 : "traffic_stream_number"
            })
        elif df.shape[1] == 11:
            df.rename(columns={
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
            })
        elif df.shape[1] == 17:
            df.rename(columns={
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
            })
        else:
            df.rename(columns={
                1 : "lane_number",
                2 : "direction_code",
                3 : "lane_type_code",
                4 : "traffic_stream_number"
            })
        df['site_name'] = site_name
    else:
        pass
    return df

def header_insert(row):
    qry = f"""
    INSERT INTO trafc.electronic_count_header (
        header_id,
        site_id,
        station_name,
        x,
        y,
        start_datetime,
        end_datetime,
        number_of_lanes,
        type_21_count_interval_minutes,
        type_21_programmable_rear_to_rear_headway_bin,
        type_21_program_id,
        speedbin1,
        speedbin2,
        speedbin3,
        speedbin4,
        speedbin5,
        speedbin6,
        speedbin7,
        speedbin8,
        speedbin9,
        speedbin10,
        type_10_vehicle_classification_scheme_primary,
        type_10_vehicle_classification_scheme_secondary,
        type_10_maximum_gap_milliseconds,
        type_10_maximum_differential_speed,
        type_30_summary_interval_minutes,
        type_30_vehicle_classification_scheme,
        type_70_summary_interval_minutes,
        type_70_vehicle_classification_scheme,
        type_70_maximum_gap_milliseconds,
        type_70_maximum_differential_speed,
        type_70_error_bin_code,
        instrumentation_description,
        document_url,
        date_processed,
        growth_rate_use,
        total_light_positive_direction,
        total_light_negative_direction,
        total_light_vehicles,
        total_heavy_positive_direction,
        total_heavy_negative_direction,
        total_heavy_vehicles,
        total_short_heavy_positive_direction,
        total_short_heavy_negative_direction,
        total_short_heavy_vehicles,
        total_medium_heavy_positive_direction,
        total_medium_heavy_negative_direction,
        total_medium_heavy_vehicles,
        total_long_heavy_positive_direction,
        total_long_heavy_negative_direction,
        total_long_heavy_vehicles,
        total_vehicles_positive_direction,
        total_vehicles_negative_direction,
        total_vehicles,
        average_speed_positive_direction,
        average_speed_negative_direction,
        average_speed,
        average_speed_light_vehicles_positive_direction,
        average_speed_light_vehicles_negative_direction,
        average_speed_light_vehicles,
        average_speed_heavy_vehicles_positive_direction,
        average_speed_heavy_vehicles_negative_direction,
        average_speed_heavy_vehicles,
        truck_split_positive_direction,
        truck_split_negative_direction,
        truck_split_total,
        estimated_axles_per_truck_positive_direction,
        estimated_axles_per_truck_negative_direction,
        estimated_axles_per_truck_total,
        percentage_speeding_positive_direction,
        percentage_speeding_negative_direction,
        percentage_speeding_total,
        vehicles_with_rear_to_rear_headway_less_than_2sec_positive_dire,
        vehicles_with_rear_to_rear_headway_less_than_2sec_negative_dire,
        vehicles_with_rear_to_rear_headway_less_than_2sec_total,
        estimated_e80_positive_direction,
        estimated_e80_negative_direction,
        estimated_e80_on_road,
        adt_positive_direction,
        adt_negative_direction,
        adt_total,
        adtt_positive_direction,
        adtt_negative_direction,
        adtt_total,
        highest_volume_per_hour_positive_direction,
        highest_volume_per_hour_negative_direction,
        highest_volume_per_hour_total,
        "15th_highest_volume_per_hour_positive_direction",
        "15th_highest_volume_per_hour_negative_direction",
        "15th_highest_volume_per_hour_total",
        "30th_highest_volume_per_hour_positive_direction",
        "30th_highest_volume_per_hour_negative_direction",
        "30th_highest_volume_per_hour_total",
        "15th_percentile_speed_positive_direction",
        "15th_percentile_speed_negative_direction",
        "15th_percentile_speed_total",
        "85th_percentile_speed_positive_direction",
        "85th_percentile_speed_negative_direction",
        "85th_percentile_speed_total",
        "year",
        positive_direction,
        negative_direction,
        avg_weekday_traffic,
        number_of_days_counted,
        duration_hours
    )
    VALUES (
        '{row['header_id']}',
        '{row['site_id']}',
        '{row['station_name']}',
        '{row['x']}',
        '{row['y']}',
        '{row['start_datetime']}',
        '{row['end_datetime']}',
        {row['number_of_lanes']},
        {row['type_21_count_interval_minutes']},
        {row['type_21_programmable_rear_to_rear_headway_bin']},
        {row['type_21_program_id']},
        {row['speedbin1']},
        {row['speedbin2']},
        {row['speedbin3']},
        {row['speedbin4']},
        {row['speedbin5']},
        {row['speedbin6']},
        {row['speedbin7']},
        {row['speedbin8']},
        {row['speedbin9']},
        {row['speedbin10']},
        {row['type_10_vehicle_classification_scheme_primary']},
        {row['type_10_vehicle_classification_scheme_secondary']},
        {row['type_10_maximum_gap_milliseconds']},
        {row['type_10_maximum_differential_speed']},
        {row['type_30_summary_interval_minutes']},
        {row['type_30_vehicle_classification_scheme']},
        {row['type_70_summary_interval_minutes']},
        {row['type_70_vehicle_classification_scheme']},
        {row['type_70_maximum_gap_milliseconds']},
        {row['type_70_maximum_differential_speed']},
        {row['type_70_error_bin_code']},
        '{row['instrumentation_description']}',
        '{row['document_url']}',
        '{row['date_processed']}',
        '{row['growth_rate_use']}',
        {row['total_light_positive_direction']},
        {row['total_light_negative_direction']},
        {row['total_light_vehicles']},
        {row['total_heavy_positive_direction']},
        {row['total_heavy_negative_direction']},
        {row['total_heavy_vehicles']},
        {row['total_short_heavy_positive_direction']},
        {row['total_short_heavy_negative_direction']},
        {row['total_short_heavy_vehicles']},
        {row['total_medium_heavy_positive_direction']},
        {row['total_medium_heavy_negative_direction']},
        {row['total_medium_heavy_vehicles']},
        {row['total_long_heavy_positive_direction']},
        {row['total_long_heavy_negative_direction']},
        {row['total_long_heavy_vehicles']},
        {row['total_vehicles_positive_direction']},
        {row['total_vehicles_negative_direction']},
        {row['total_vehicles']},
        {row['average_speed_positive_direction']},
        {row['average_speed_negative_direction']},
        {row['average_speed']},
        {row['average_speed_light_vehicles_positive_direction']},
        {row['average_speed_light_vehicles_negative_direction']},
        {row['average_speed_light_vehicles']},
        {row['average_speed_heavy_vehicles_positive_direction']},
        {row['average_speed_heavy_vehicles_negative_direction']},
        {row['average_speed_heavy_vehicles']},
        '{row['truck_split_positive_direction']}',
        '{row['truck_split_negative_direction']}',
        '{row['truck_split_total']}',
        {row['estimated_axles_per_truck_positive_direction']},
        {row['estimated_axles_per_truck_negative_direction']},
        {row['estimated_axles_per_truck_total']},
        {row['percentage_speeding_positive_direction']},
        {row['percentage_speeding_negative_direction']},
        {row['percentage_speeding_total']},
        {row['vehicles_with_rear_to_rear_headway_less_than_2sec_positive_dire']},
        {row['vehicles_with_rear_to_rear_headway_less_than_2sec_negative_dire']},
        {row['vehicles_with_rear_to_rear_headway_less_than_2sec_total']},
        {row['estimated_e80_positive_direction']},
        {row['estimated_e80_negative_direction']},
        {row['estimated_e80_on_road']},
        {row['adt_positive_direction']},
        {row['adt_negative_direction']},
        {row['adt_total']},
        {row['adtt_positive_direction']},
        {row['adtt_negative_direction']},
        {row['adtt_total']},
        {row['highest_volume_per_hour_positive_direction']},
        {row['highest_volume_per_hour_negative_direction']},
        {row['highest_volume_per_hour_total']},
        {row['15th_highest_volume_per_hour_positive_direction']},
        {row['15th_highest_volume_per_hour_negative_direction']},
        {row['15th_highest_volume_per_hour_total']},
        {row['30th_highest_volume_per_hour_positive_direction']},
        {row['30th_highest_volume_per_hour_negative_direction']},
        {row['30th_highest_volume_per_hour_total']},
        {row['15th_percentile_speed_positive_direction']},
        {row['15th_percentile_speed_negative_direction']},
        {row['15th_percentile_speed_total']},
        {row['85th_percentile_speed_positive_direction']},
        {row['85th_percentile_speed_negative_direction']},
        {row['85th_percentile_speed_total']},
        {row['year']},
        '{row['positive_direction']}',
        '{row['negative_direction']}',
        {row['avg_weekday_traffic']},
        {row['number_of_days_counted']},
        {row['duration_hours']}
            )
        """
    return qry

def header_update(row):
    qry = f""" UPDATE trafc.electronic_count_header SET
        x = '{row['x']}',
        y = '{row['y']}',
        number_of_lanes = {row['number_of_lanes']},
        type_21_count_interval_minutes = {row['type_21_count_interval_minutes']},
        type_21_programmable_rear_to_rear_headway_bin = {row['type_21_programmable_rear_to_rear_headway_bin']},
        type_21_program_id = {row['type_21_program_id']},
        speedbin1 = {row['speedbin1']},
        speedbin2 = {row['speedbin2']},
        speedbin3 = {row['speedbin3']},
        speedbin4 = {row['speedbin4']},
        speedbin5 = {row['speedbin5']},
        speedbin6 = {row['speedbin6']},
        speedbin7 = {row['speedbin7']},
        speedbin8 = {row['speedbin8']},
        speedbin9 = {row['speedbin9']},
        speedbin10 = {row['speedbin10']},
        type_10_vehicle_classification_scheme_primary = {row['type_10_vehicle_classification_scheme_primary']},
        type_10_vehicle_classification_scheme_secondary = {row['type_10_vehicle_classification_scheme_secondary']},
        type_10_maximum_gap_milliseconds = {row['type_10_maximum_gap_milliseconds']},
        type_10_maximum_differential_speed = {row['type_10_maximum_differential_speed']},
        type_30_summary_interval_minutes = {row['type_30_summary_interval_minutes']},
        type_30_vehicle_classification_scheme = {row['type_30_vehicle_classification_scheme']},
        type_70_summary_interval_minutes = {row['type_70_summary_interval_minutes']},
        type_70_vehicle_classification_scheme = {row['type_70_vehicle_classification_scheme']},
        type_70_maximum_gap_milliseconds = {row['type_70_maximum_gap_milliseconds']},
        type_70_maximum_differential_speed = {row['type_70_maximum_differential_speed']},
        type_70_error_bin_code = {row['type_70_error_bin_code']},
        instrumentation_description = '{row['instrumentation_description']}',
        document_url = '{row['document_url']}',
        date_processed = '{row['date_processed']}',
        growth_rate_use = '{row['growth_rate_use']}',
        total_light_positive_direction = {row['total_light_positive_direction']},
        total_light_negative_direction = {row['total_light_negative_direction']},
        total_light_vehicles = {row['total_light_vehicles']},
        total_heavy_positive_direction = {row['total_heavy_positive_direction']},
        total_heavy_negative_direction = {row['total_heavy_negative_direction']},
        total_heavy_vehicles = {row['total_heavy_vehicles']},
        total_short_heavy_positive_direction = {row['total_short_heavy_positive_direction']},
        total_short_heavy_negative_direction = {row['total_short_heavy_negative_direction']},
        total_short_heavy_vehicles = {row['total_short_heavy_vehicles']},
        total_medium_heavy_positive_direction = {row['total_medium_heavy_positive_direction']},
        total_medium_heavy_negative_direction = {row['total_medium_heavy_negative_direction']},
        total_medium_heavy_vehicles = {row['total_medium_heavy_vehicles']},
        total_long_heavy_positive_direction = {row['total_long_heavy_positive_direction']},
        total_long_heavy_negative_direction = {row['total_long_heavy_negative_direction']},
        total_long_heavy_vehicles = {row['total_long_heavy_vehicles']},
        total_vehicles_positive_direction = {row['total_vehicles_positive_direction']},
        total_vehicles_negative_direction = {row['total_vehicles_negative_direction']},
        total_vehicles = {row['total_vehicles']},
        average_speed_positive_direction = {row['average_speed_positive_direction']},
        average_speed_negative_direction = {row['average_speed_negative_direction']},
        average_speed = {row['average_speed']},
        average_speed_light_vehicles_positive_direction = {row['average_speed_light_vehicles_positive_direction']},
        average_speed_light_vehicles_negative_direction = {row['average_speed_light_vehicles_negative_direction']},
        average_speed_light_vehicles = {row['average_speed_light_vehicles']},
        average_speed_heavy_vehicles_positive_direction = {row['average_speed_heavy_vehicles_positive_direction']},
        average_speed_heavy_vehicles_negative_direction = {row['average_speed_heavy_vehicles_negative_direction']},
        average_speed_heavy_vehicles = {row['average_speed_heavy_vehicles']},
        truck_split_positive_direction = '{row['truck_split_positive_direction']}',
        truck_split_negative_direction = '{row['truck_split_negative_direction']}',
        truck_split_total = '{row['truck_split_total']}',
        estimated_axles_per_truck_positive_direction = {row['estimated_axles_per_truck_positive_direction']},
        estimated_axles_per_truck_negative_direction = {row['estimated_axles_per_truck_negative_direction']},
        estimated_axles_per_truck_total = {row['estimated_axles_per_truck_total']},
        percentage_speeding_positive_direction = {row['percentage_speeding_positive_direction']},
        percentage_speeding_negative_direction = {row['percentage_speeding_negative_direction']},
        percentage_speeding_total = {row['percentage_speeding_total']},
        vehicles_with_rear_to_rear_headway_less_than_2sec_positive_dire = {row['vehicles_with_rear_to_rear_headway_less_than_2sec_positive_dire']},
        vehicles_with_rear_to_rear_headway_less_than_2sec_negative_dire = {row['vehicles_with_rear_to_rear_headway_less_than_2sec_negative_dire']},
        vehicles_with_rear_to_rear_headway_less_than_2sec_total = {row['vehicles_with_rear_to_rear_headway_less_than_2sec_total']},
        estimated_e80_positive_direction = {row['estimated_e80_positive_direction']},
        estimated_e80_negative_direction = {row['estimated_e80_negative_direction']},
        estimated_e80_on_road = {row['estimated_e80_on_road']},
        adt_positive_direction = {row['adt_positive_direction']},
        adt_negative_direction = {row['adt_negative_direction']},
        adt_total = {row['adt_total']},
        adtt_positive_direction = {row['adtt_positive_direction']},
        adtt_negative_direction = {row['adtt_negative_direction']},
        adtt_total = {row['adtt_total']},
        highest_volume_per_hour_positive_direction = {row['highest_volume_per_hour_positive_direction']},
        highest_volume_per_hour_negative_direction = {row['highest_volume_per_hour_negative_direction']},
        highest_volume_per_hour_total = {row['highest_volume_per_hour_total']},
        "15th_highest_volume_per_hour_positive_direction" = {row['15th_highest_volume_per_hour_positive_direction']},
        "15th_highest_volume_per_hour_negative_direction" = {row['15th_highest_volume_per_hour_negative_direction']},
        "15th_highest_volume_per_hour_total" = {row['15th_highest_volume_per_hour_total']},
        "30th_highest_volume_per_hour_positive_direction" = {row['30th_highest_volume_per_hour_positive_direction']},
        "30th_highest_volume_per_hour_negative_direction" = {row['30th_highest_volume_per_hour_negative_direction']},
        "30th_highest_volume_per_hour_total" = {row['30th_highest_volume_per_hour_total']},
        "15th_percentile_speed_positive_direction" = {row['15th_percentile_speed_positive_direction']},
        "15th_percentile_speed_negative_direction" = {row['15th_percentile_speed_negative_direction']},
        "15th_percentile_speed_total" = {row['15th_percentile_speed_total']},
        "85th_percentile_speed_positive_direction" = {row['85th_percentile_speed_positive_direction']},
        "85th_percentile_speed_negative_direction" = {row['85th_percentile_speed_negative_direction']},
        "85th_percentile_speed_total" = {row['85th_percentile_speed_total']},
        "year" = {row['year']},
        positive_direction = '{row['positive_direction']}',
        negative_direction = '{row['negative_direction']}',
        avg_weekday_traffic = {row['avg_weekday_traffic']},
        number_of_days_counted = {row['number_of_days_counted']},
        duration_hours = {row['duration_hours']}
        WHERE site_id='{row['site_id']}' AND start_datetime='{row['start_datetime']}' AND end_datetime='{row['end_datetime']}'
        on conflict on constraint electronic_count_header_un do update set
    id = coalesce(excluded.id,id),
    header_id = coalesce(excluded.header_id,header_id),
    station_name = coalesce(excluded.station_name,station_name),
    x = coalesce(excluded.x,x),
    y = coalesce(excluded.y,y),
    number_of_lanes = coalesce(excluded.number_of_lanes,number_of_lanes),
    type_21_count_interval_minutes = coalesce(excluded.type_21_count_interval_minutes,type_21_count_interval_minutes),
    type_21_programmable_rear_to_rear_headway_bin = coalesce(excluded.type_21_programmable_rear_to_rear_headway_bin,type_21_programmable_rear_to_rear_headway_bin),
    type_21_program_id = coalesce(excluded.type_21_program_id,type_21_program_id),
    speedbin1 = coalesce(excluded.speedbin1,speedbin1),
    speedbin2 = coalesce(excluded.speedbin2,speedbin2),
    speedbin3 = coalesce(excluded.speedbin3,speedbin3),
    speedbin4 = coalesce(excluded.speedbin4,speedbin4),
    speedbin5 = coalesce(excluded.speedbin5,speedbin5),
    speedbin6 = coalesce(excluded.speedbin6,speedbin6),
    speedbin7 = coalesce(excluded.speedbin7,speedbin7),
    speedbin8 = coalesce(excluded.speedbin8,speedbin8),
    speedbin9 = coalesce(excluded.speedbin9,speedbin9),
    speedbin10 = coalesce(excluded.speedbin10,speedbin10),
    type_10_vehicle_classification_scheme_primary = coalesce(excluded.type_10_vehicle_classification_scheme_primary,type_10_vehicle_classification_scheme_primary),
    type_10_vehicle_classification_scheme_secondary = coalesce(excluded.type_10_vehicle_classification_scheme_secondary,type_10_vehicle_classification_scheme_secondary),
    type_10_maximum_gap_milliseconds = coalesce(excluded.type_10_maximum_gap_milliseconds,type_10_maximum_gap_milliseconds),
    type_10_maximum_differential_speed = coalesce(excluded.type_10_maximum_differential_speed,type_10_maximum_differential_speed),
    type_30_summary_interval_minutes = coalesce(excluded.type_30_summary_interval_minutes,type_30_summary_interval_minutes),
    type_30_vehicle_classification_scheme = coalesce(excluded.type_30_vehicle_classification_scheme,type_30_vehicle_classification_scheme),
    type_70_summary_interval_minutes = coalesce(excluded.type_70_summary_interval_minutes,type_70_summary_interval_minutes),
    type_70_vehicle_classification_scheme = coalesce(excluded.type_70_vehicle_classification_scheme,type_70_vehicle_classification_scheme),
    type_70_maximum_gap_milliseconds = coalesce(excluded.type_70_maximum_gap_milliseconds,type_70_maximum_gap_milliseconds),
    type_70_maximum_differential_speed = coalesce(excluded.type_70_maximum_differential_speed,type_70_maximum_differential_speed),
    type_70_error_bin_code = coalesce(excluded.type_70_error_bin_code,type_70_error_bin_code),
    instrumentation_description = coalesce(excluded.instrumentation_description,instrumentation_description),
    document_url = coalesce(excluded.document_url,document_url),
    date_processed = coalesce(excluded.date_processed,date_processed),
    growth_rate_use = coalesce(excluded.growth_rate_use,growth_rate_use),
    total_light_positive_direction = coalesce(excluded.total_light_positive_direction,total_light_positive_direction),
    total_light_negative_direction = coalesce(excluded.total_light_negative_direction,total_light_negative_direction),
    total_light_vehicles = coalesce(excluded.total_light_vehicles,total_light_vehicles),
    total_heavy_positive_direction = coalesce(excluded.total_heavy_positive_direction,total_heavy_positive_direction),
    total_heavy_negative_direction = coalesce(excluded.total_heavy_negative_direction,total_heavy_negative_direction),
    total_heavy_vehicles = coalesce(excluded.total_heavy_vehicles,total_heavy_vehicles),
    total_short_heavy_positive_direction = coalesce(excluded.total_short_heavy_positive_direction,total_short_heavy_positive_direction),
    total_short_heavy_negative_direction = coalesce(excluded.total_short_heavy_negative_direction,total_short_heavy_negative_direction),
    total_short_heavy_vehicles = coalesce(excluded.total_short_heavy_vehicles,total_short_heavy_vehicles),
    total_medium_heavy_positive_direction = coalesce(excluded.total_medium_heavy_positive_direction,total_medium_heavy_positive_direction),
    total_medium_heavy_negative_direction = coalesce(excluded.total_medium_heavy_negative_direction,total_medium_heavy_negative_direction),
    total_medium_heavy_vehicles = coalesce(excluded.total_medium_heavy_vehicles,total_medium_heavy_vehicles),
    total_long_heavy_positive_direction = coalesce(excluded.total_long_heavy_positive_direction,total_long_heavy_positive_direction),
    total_long_heavy_negative_direction = coalesce(excluded.total_long_heavy_negative_direction,total_long_heavy_negative_direction),
    total_long_heavy_vehicles = coalesce(excluded.total_long_heavy_vehicles,total_long_heavy_vehicles),
    total_vehicles_positive_direction = coalesce(excluded.total_vehicles_positive_direction,total_vehicles_positive_direction),
    total_vehicles_negative_direction = coalesce(excluded.total_vehicles_negative_direction,total_vehicles_negative_direction),
    total_vehicles = coalesce(excluded.total_vehicles,total_vehicles),
    average_speed_positive_direction = coalesce(excluded.average_speed_positive_direction,average_speed_positive_direction),
    average_speed_negative_direction = coalesce(excluded.average_speed_negative_direction,average_speed_negative_direction),
    average_speed = coalesce(excluded.average_speed,average_speed),
    average_speed_light_vehicles_positive_direction = coalesce(excluded.average_speed_light_vehicles_positive_direction,average_speed_light_vehicles_positive_direction),
    average_speed_light_vehicles_negative_direction = coalesce(excluded.average_speed_light_vehicles_negative_direction,average_speed_light_vehicles_negative_direction),
    average_speed_light_vehicles = coalesce(excluded.average_speed_light_vehicles,average_speed_light_vehicles),
    average_speed_heavy_vehicles_positive_direction = coalesce(excluded.average_speed_heavy_vehicles_positive_direction,average_speed_heavy_vehicles_positive_direction),
    average_speed_heavy_vehicles_negative_direction = coalesce(excluded.average_speed_heavy_vehicles_negative_direction,average_speed_heavy_vehicles_negative_direction),
    average_speed_heavy_vehicles = coalesce(excluded.average_speed_heavy_vehicles,average_speed_heavy_vehicles),
    truck_split_positive_direction = coalesce(excluded.truck_split_positive_direction,truck_split_positive_direction),
    truck_split_negative_direction = coalesce(excluded.truck_split_negative_direction,truck_split_negative_direction),
    truck_split_total = coalesce(excluded.truck_split_total,truck_split_total),
    estimated_axles_per_truck_positive_direction = coalesce(excluded.estimated_axles_per_truck_positive_direction,estimated_axles_per_truck_positive_direction),
    estimated_axles_per_truck_negative_direction = coalesce(excluded.estimated_axles_per_truck_negative_direction,estimated_axles_per_truck_negative_direction),
    estimated_axles_per_truck_total = coalesce(excluded.estimated_axles_per_truck_total,estimated_axles_per_truck_total),
    percentage_speeding_positive_direction = coalesce(excluded.percentage_speeding_positive_direction,percentage_speeding_positive_direction),
    percentage_speeding_negative_direction = coalesce(excluded.percentage_speeding_negative_direction,percentage_speeding_negative_direction),
    percentage_speeding_total = coalesce(excluded.percentage_speeding_total,percentage_speeding_total),
    vehicles_with_rear_to_rear_headway_less_than_2sec_positive_dire = coalesce(excluded.vehicles_with_rear_to_rear_headway_less_than_2sec_positive_dire,vehicles_with_rear_to_rear_headway_less_than_2sec_positive_dire),
    vehicles_with_rear_to_rear_headway_less_than_2sec_negative_dire = coalesce(excluded.vehicles_with_rear_to_rear_headway_less_than_2sec_negative_dire,vehicles_with_rear_to_rear_headway_less_than_2sec_negative_dire),
    vehicles_with_rear_to_rear_headway_less_than_2sec_total = coalesce(excluded.vehicles_with_rear_to_rear_headway_less_than_2sec_total,vehicles_with_rear_to_rear_headway_less_than_2sec_total),
    estimated_e80_positive_direction = coalesce(excluded.estimated_e80_positive_direction,estimated_e80_positive_direction),
    estimated_e80_negative_direction = coalesce(excluded.estimated_e80_negative_direction,estimated_e80_negative_direction),
    estimated_e80_on_road = coalesce(excluded.estimated_e80_on_road,estimated_e80_on_road),
    adt_positive_direction = coalesce(excluded.adt_positive_direction,adt_positive_direction),
    adt_negative_direction = coalesce(excluded.adt_negative_direction,adt_negative_direction),
    adt_total = coalesce(excluded.adt_total,adt_total),
    adtt_positive_direction = coalesce(excluded.adtt_positive_direction,adtt_positive_direction),
    adtt_negative_direction = coalesce(excluded.adtt_negative_direction,adtt_negative_direction),
    adtt_total = coalesce(excluded.adtt_total,adtt_total),
    highest_volume_per_hour_positive_direction = coalesce(excluded.highest_volume_per_hour_positive_direction,highest_volume_per_hour_positive_direction),
    highest_volume_per_hour_negative_direction = coalesce(excluded.highest_volume_per_hour_negative_direction,highest_volume_per_hour_negative_direction),
    highest_volume_per_hour_total = coalesce(excluded.highest_volume_per_hour_total,highest_volume_per_hour_total),
    "15th_highest_volume_per_hour_positive_direction" = coalesce(excluded."15th_highest_volume_per_hour_positive_direction","15th_highest_volume_per_hour_positive_direction"),
    "15th_highest_volume_per_hour_negative_direction" = coalesce(excluded."15th_highest_volume_per_hour_negative_direction","15th_highest_volume_per_hour_negative_direction"),
    "15th_highest_volume_per_hour_total" = coalesce(excluded."15th_highest_volume_per_hour_total","15th_highest_volume_per_hour_total"),
    "30th_highest_volume_per_hour_positive_direction" = coalesce(excluded."30th_highest_volume_per_hour_positive_direction","30th_highest_volume_per_hour_positive_direction"),
    "30th_highest_volume_per_hour_negative_direction" = coalesce(excluded."30th_highest_volume_per_hour_negative_direction","30th_highest_volume_per_hour_negative_direction"),
    "30th_highest_volume_per_hour_total" = coalesce(excluded."30th_highest_volume_per_hour_total","30th_highest_volume_per_hour_total"),
    "15th_percentile_speed_positive_direction" = coalesce(excluded."15th_percentile_speed_positive_direction","15th_percentile_speed_positive_direction"),
    "15th_percentile_speed_negative_direction" = coalesce(excluded."15th_percentile_speed_negative_direction","15th_percentile_speed_negative_direction"),
    "15th_percentile_speed_total" = coalesce(excluded."15th_percentile_speed_total","15th_percentile_speed_total"),
    "85th_percentile_speed_positive_direction" = coalesce(excluded."85th_percentile_speed_positive_direction","85th_percentile_speed_positive_direction"),
    "85th_percentile_speed_negative_direction" = coalesce(excluded."85th_percentile_speed_negative_direction","85th_percentile_speed_negative_direction"),
    "85th_percentile_speed_total" = coalesce(excluded."85th_percentile_speed_total","85th_percentile_speed_total"),
    "year" = coalesce(excluded."year","year"),
    positive_direction = coalesce(excluded.positive_direction,positive_direction),
    negative_direction = coalesce(excluded.negative_direction,negative_direction),
    avg_weekday_traffic = coalesce(excluded.avg_weekday_traffic,avg_weekday_traffic),
    number_of_days_counted = coalesce(excluded.number_of_days_counted,number_of_days_counted),
    duration_hours = coalesce(excluded.duration_hours,duration_hours)
    ;
        """
    return qry

def header_update_2(df: pd.DataFrame) -> str:
    qry = f"""update
        trafc.electronic_count_header
    set
        type_21_count_interval_minutes = {df['type_21_count_interval_minutes'][0]},
        type_21_programmable_rear_to_rear_headway_bin = {df['type_21_programmable_rear_to_rear_headway_bin'][0]},
        type_21_program_id = {df['type_21_program_id'][0]},
        speedbin1 = {df['speedbin1'][0]},
        speedbin2 = {df['speedbin2'][0]},
        speedbin3 = {df['speedbin3'][0]},
        speedbin4 = {df['speedbin4'][0]},
        speedbin5 = {df['speedbin5'][0]},
        speedbin6 = {df['speedbin6'][0]},
        speedbin7 = {df['speedbin7'][0]},
        speedbin8 = {df['speedbin8'][0]},
        speedbin9 = {df['speedbin9'][0]},
        total_light_positive_direction = {df['total_light_positive_direction'][0]},
        total_light_negative_direction = {df['total_light_negative_direction'][0]},
        total_light_vehicles = {df['total_light_vehicles'][0]},
        total_heavy_positive_direction = {df['total_heavy_positive_direction'][0]},
        total_heavy_negative_direction = {df['total_heavy_negative_direction'][0]},
        total_heavy_vehicles = {df['total_heavy_vehicles'][0]},
        total_short_heavy_positive_direction = {df['total_short_heavy_positive_direction'][0]},
        total_short_heavy_negative_direction = {df['total_short_heavy_negative_direction'][0]},
        total_short_heavy_vehicles = {df['total_short_heavy_vehicles'][0]},
        total_medium_heavy_positive_direction = {df['total_medium_heavy_positive_direction'][0]},
        total_medium_heavy_negative_direction = {df['total_medium_heavy_negative_direction'][0]},
        total_medium_heavy_vehicles = {df['total_medium_heavy_vehicles'][0]},
        total_long_heavy_positive_direction = {df['total_long_heavy_positive_direction'][0]},
        total_long_heavy_negative_direction = {df['total_long_heavy_negative_direction'][0]},
        total_long_heavy_vehicles = {df['total_long_heavy_vehicles'][0]},
        total_vehicles_positive_direction = {df['total_vehicles_positive_direction'][0]},
        total_vehicles_negative_direction = {df['total_vehicles_negative_direction'][0]},
        total_vehicles = {df['total_vehicles'][0]},
        average_speed_positive_direction = {df['average_speed_positive_direction'][0]},
        average_speed_negative_direction = {df['average_speed_negative_direction'][0]},
        average_speed = {df['average_speed'][0]},
        average_speed_light_vehicles_positive_direction = {df['average_speed_light_vehicles_positive_direction'][0]},
        average_speed_light_vehicles_negative_direction = {df['average_speed_light_vehicles_negative_direction'][0]},
        average_speed_light_vehicles = {df['average_speed_light_vehicles'][0]},
        average_speed_heavy_vehicles_positive_direction = {df['average_speed_heavy_vehicles_positive_direction'][0]},
        average_speed_heavy_vehicles_negative_direction = {df['average_speed_heavy_vehicles_negative_direction'][0]},
        average_speed_heavy_vehicles = {df['average_speed_heavy_vehicles'][0]},
        truck_split_positive_direction = '{df['truck_split_positive_direction'][0]}',
        truck_split_negative_direction = '{df['truck_split_negative_direction'][0]}',
        truck_split_total = '{df['truck_split_total'][0]}',
        estimated_axles_per_truck_positive_direction = {df['estimated_axles_per_truck_positive_direction'][0]},
        estimated_axles_per_truck_negative_direction = {df['estimated_axles_per_truck_negative_direction'][0]},
        estimated_axles_per_truck_total = {df['estimated_axles_per_truck_total'][0]},
        percentage_speeding_positive_direction = {df['percentage_speeding_positive_direction'][0]},
        percentage_speeding_negative_direction = {df['percentage_speeding_negative_direction'][0]},
        percentage_speeding_total = {df['percentage_speeding_total'][0]},
        vehicles_with_rear_to_rear_headway_less_than_2sec_positive_dire = {df['vehicles_with_rear_to_rear_headway_less_than_2sec_positive_dire'][0]},
        vehicles_with_rear_to_rear_headway_less_than_2sec_negative_dire = {df['vehicles_with_rear_to_rear_headway_less_than_2sec_negative_dire'][0]},
        vehicles_with_rear_to_rear_headway_less_than_2sec_total = {df['vehicles_with_rear_to_rear_headway_less_than_2sec_total'][0]},
        estimated_e80_positive_direction = {df['estimated_e80_positive_direction'][0]},
        estimated_e80_negative_direction = {df['estimated_e80_negative_direction'][0]},
        estimated_e80_on_road = {df['estimated_e80_on_road'][0]},
        adt_positive_direction = {df['adt_positive_direction'][0]},
        adt_negative_direction = {df['adt_negative_direction'][0]},
        adt_total = {df['adt_total'][0]},
        adtt_positive_direction = {df['adtt_positive_direction'][0]},
        adtt_negative_direction = {df['adtt_negative_direction'][0]},
        adtt_total = {df['adtt_total'][0]},
        highest_volume_per_hour_positive_direction = {df['highest_volume_per_hour_positive_direction'][0]},
        highest_volume_per_hour_negative_direction = {df['highest_volume_per_hour_negative_direction'][0]},
        highest_volume_per_hour_total = {df['highest_volume_per_hour_total'][0]},
        "15th_highest_volume_per_hour_positive_direction" = {df['15th_highest_volume_per_hour_positive_direction'][0]},
        "15th_highest_volume_per_hour_negative_direction" = {df['15th_highest_volume_per_hour_negative_direction'][0]},
        "15th_highest_volume_per_hour_total" = {df['15th_highest_volume_per_hour_total'][0]},
        "30th_highest_volume_per_hour_positive_direction" = {df['30th_highest_volume_per_hour_positive_direction'][0]},
        "30th_highest_volume_per_hour_negative_direction" = {df['30th_highest_volume_per_hour_negative_direction'][0]},
        "30th_highest_volume_per_hour_total" = {df['30th_highest_volume_per_hour_total'][0]},
        "15th_percentile_speed_positive_direction" = {df['15th_percentile_speed_positive_direction'][0]},
        "15th_percentile_speed_negative_direction" = {df['15th_percentile_speed_negative_direction'][0]},
        "15th_percentile_speed_total" = {df['15th_percentile_speed_total'][0]},
        "85th_percentile_speed_positive_direction" = {df['85th_percentile_speed_positive_direction'][0]},
        "85th_percentile_speed_negative_direction" = {df['85th_percentile_speed_negative_direction'][0]},
        "85th_percentile_speed_total" = {df['85th_percentile_speed_total'][0]},
        "year" = {df['year'][0]},
        avg_weekday_traffic = {df['avg_weekday_traffic'][0]},
        number_of_days_counted = {df['number_of_days_counted'][0]},
        duration_hours = {df['duration_hours'][0]}
    where
        site_id = '{df['site_id'][0]}'
        and start_datetime = '{df['start_datetime'][0]}'
        and end_datetime = '{df['end_datetime'][0]}'
    on conflict on constraint electronic_count_header_un do update set
    id = coalesce(excluded.id,id),
    header_id = coalesce(excluded.header_id,header_id),
    station_name = coalesce(excluded.station_name,station_name),
    x = coalesce(excluded.x,x),
    y = coalesce(excluded.y,y),
    number_of_lanes = coalesce(excluded.number_of_lanes,number_of_lanes),
    type_21_count_interval_minutes = coalesce(excluded.type_21_count_interval_minutes,type_21_count_interval_minutes),
    type_21_programmable_rear_to_rear_headway_bin = coalesce(excluded.type_21_programmable_rear_to_rear_headway_bin,type_21_programmable_rear_to_rear_headway_bin),
    type_21_program_id = coalesce(excluded.type_21_program_id,type_21_program_id),
    speedbin1 = coalesce(excluded.speedbin1,speedbin1),
    speedbin2 = coalesce(excluded.speedbin2,speedbin2),
    speedbin3 = coalesce(excluded.speedbin3,speedbin3),
    speedbin4 = coalesce(excluded.speedbin4,speedbin4),
    speedbin5 = coalesce(excluded.speedbin5,speedbin5),
    speedbin6 = coalesce(excluded.speedbin6,speedbin6),
    speedbin7 = coalesce(excluded.speedbin7,speedbin7),
    speedbin8 = coalesce(excluded.speedbin8,speedbin8),
    speedbin9 = coalesce(excluded.speedbin9,speedbin9),
    speedbin10 = coalesce(excluded.speedbin10,speedbin10),
    type_10_vehicle_classification_scheme_primary = coalesce(excluded.type_10_vehicle_classification_scheme_primary,type_10_vehicle_classification_scheme_primary),
    type_10_vehicle_classification_scheme_secondary = coalesce(excluded.type_10_vehicle_classification_scheme_secondary,type_10_vehicle_classification_scheme_secondary),
    type_10_maximum_gap_milliseconds = coalesce(excluded.type_10_maximum_gap_milliseconds,type_10_maximum_gap_milliseconds),
    type_10_maximum_differential_speed = coalesce(excluded.type_10_maximum_differential_speed,type_10_maximum_differential_speed),
    type_30_summary_interval_minutes = coalesce(excluded.type_30_summary_interval_minutes,type_30_summary_interval_minutes),
    type_30_vehicle_classification_scheme = coalesce(excluded.type_30_vehicle_classification_scheme,type_30_vehicle_classification_scheme),
    type_70_summary_interval_minutes = coalesce(excluded.type_70_summary_interval_minutes,type_70_summary_interval_minutes),
    type_70_vehicle_classification_scheme = coalesce(excluded.type_70_vehicle_classification_scheme,type_70_vehicle_classification_scheme),
    type_70_maximum_gap_milliseconds = coalesce(excluded.type_70_maximum_gap_milliseconds,type_70_maximum_gap_milliseconds),
    type_70_maximum_differential_speed = coalesce(excluded.type_70_maximum_differential_speed,type_70_maximum_differential_speed),
    type_70_error_bin_code = coalesce(excluded.type_70_error_bin_code,type_70_error_bin_code),
    instrumentation_description = coalesce(excluded.instrumentation_description,instrumentation_description),
    document_url = coalesce(excluded.document_url,document_url),
    date_processed = coalesce(excluded.date_processed,date_processed),
    growth_rate_use = coalesce(excluded.growth_rate_use,growth_rate_use),
    total_light_positive_direction = coalesce(excluded.total_light_positive_direction,total_light_positive_direction),
    total_light_negative_direction = coalesce(excluded.total_light_negative_direction,total_light_negative_direction),
    total_light_vehicles = coalesce(excluded.total_light_vehicles,total_light_vehicles),
    total_heavy_positive_direction = coalesce(excluded.total_heavy_positive_direction,total_heavy_positive_direction),
    total_heavy_negative_direction = coalesce(excluded.total_heavy_negative_direction,total_heavy_negative_direction),
    total_heavy_vehicles = coalesce(excluded.total_heavy_vehicles,total_heavy_vehicles),
    total_short_heavy_positive_direction = coalesce(excluded.total_short_heavy_positive_direction,total_short_heavy_positive_direction),
    total_short_heavy_negative_direction = coalesce(excluded.total_short_heavy_negative_direction,total_short_heavy_negative_direction),
    total_short_heavy_vehicles = coalesce(excluded.total_short_heavy_vehicles,total_short_heavy_vehicles),
    total_medium_heavy_positive_direction = coalesce(excluded.total_medium_heavy_positive_direction,total_medium_heavy_positive_direction),
    total_medium_heavy_negative_direction = coalesce(excluded.total_medium_heavy_negative_direction,total_medium_heavy_negative_direction),
    total_medium_heavy_vehicles = coalesce(excluded.total_medium_heavy_vehicles,total_medium_heavy_vehicles),
    total_long_heavy_positive_direction = coalesce(excluded.total_long_heavy_positive_direction,total_long_heavy_positive_direction),
    total_long_heavy_negative_direction = coalesce(excluded.total_long_heavy_negative_direction,total_long_heavy_negative_direction),
    total_long_heavy_vehicles = coalesce(excluded.total_long_heavy_vehicles,total_long_heavy_vehicles),
    total_vehicles_positive_direction = coalesce(excluded.total_vehicles_positive_direction,total_vehicles_positive_direction),
    total_vehicles_negative_direction = coalesce(excluded.total_vehicles_negative_direction,total_vehicles_negative_direction),
    total_vehicles = coalesce(excluded.total_vehicles,total_vehicles),
    average_speed_positive_direction = coalesce(excluded.average_speed_positive_direction,average_speed_positive_direction),
    average_speed_negative_direction = coalesce(excluded.average_speed_negative_direction,average_speed_negative_direction),
    average_speed = coalesce(excluded.average_speed,average_speed),
    average_speed_light_vehicles_positive_direction = coalesce(excluded.average_speed_light_vehicles_positive_direction,average_speed_light_vehicles_positive_direction),
    average_speed_light_vehicles_negative_direction = coalesce(excluded.average_speed_light_vehicles_negative_direction,average_speed_light_vehicles_negative_direction),
    average_speed_light_vehicles = coalesce(excluded.average_speed_light_vehicles,average_speed_light_vehicles),
    average_speed_heavy_vehicles_positive_direction = coalesce(excluded.average_speed_heavy_vehicles_positive_direction,average_speed_heavy_vehicles_positive_direction),
    average_speed_heavy_vehicles_negative_direction = coalesce(excluded.average_speed_heavy_vehicles_negative_direction,average_speed_heavy_vehicles_negative_direction),
    average_speed_heavy_vehicles = coalesce(excluded.average_speed_heavy_vehicles,average_speed_heavy_vehicles),
    truck_split_positive_direction = coalesce(excluded.truck_split_positive_direction,truck_split_positive_direction),
    truck_split_negative_direction = coalesce(excluded.truck_split_negative_direction,truck_split_negative_direction),
    truck_split_total = coalesce(excluded.truck_split_total,truck_split_total),
    estimated_axles_per_truck_positive_direction = coalesce(excluded.estimated_axles_per_truck_positive_direction,estimated_axles_per_truck_positive_direction),
    estimated_axles_per_truck_negative_direction = coalesce(excluded.estimated_axles_per_truck_negative_direction,estimated_axles_per_truck_negative_direction),
    estimated_axles_per_truck_total = coalesce(excluded.estimated_axles_per_truck_total,estimated_axles_per_truck_total),
    percentage_speeding_positive_direction = coalesce(excluded.percentage_speeding_positive_direction,percentage_speeding_positive_direction),
    percentage_speeding_negative_direction = coalesce(excluded.percentage_speeding_negative_direction,percentage_speeding_negative_direction),
    percentage_speeding_total = coalesce(excluded.percentage_speeding_total,percentage_speeding_total),
    vehicles_with_rear_to_rear_headway_less_than_2sec_positive_dire = coalesce(excluded.vehicles_with_rear_to_rear_headway_less_than_2sec_positive_dire,vehicles_with_rear_to_rear_headway_less_than_2sec_positive_dire),
    vehicles_with_rear_to_rear_headway_less_than_2sec_negative_dire = coalesce(excluded.vehicles_with_rear_to_rear_headway_less_than_2sec_negative_dire,vehicles_with_rear_to_rear_headway_less_than_2sec_negative_dire),
    vehicles_with_rear_to_rear_headway_less_than_2sec_total = coalesce(excluded.vehicles_with_rear_to_rear_headway_less_than_2sec_total,vehicles_with_rear_to_rear_headway_less_than_2sec_total),
    estimated_e80_positive_direction = coalesce(excluded.estimated_e80_positive_direction,estimated_e80_positive_direction),
    estimated_e80_negative_direction = coalesce(excluded.estimated_e80_negative_direction,estimated_e80_negative_direction),
    estimated_e80_on_road = coalesce(excluded.estimated_e80_on_road,estimated_e80_on_road),
    adt_positive_direction = coalesce(excluded.adt_positive_direction,adt_positive_direction),
    adt_negative_direction = coalesce(excluded.adt_negative_direction,adt_negative_direction),
    adt_total = coalesce(excluded.adt_total,adt_total),
    adtt_positive_direction = coalesce(excluded.adtt_positive_direction,adtt_positive_direction),
    adtt_negative_direction = coalesce(excluded.adtt_negative_direction,adtt_negative_direction),
    adtt_total = coalesce(excluded.adtt_total,adtt_total),
    highest_volume_per_hour_positive_direction = coalesce(excluded.highest_volume_per_hour_positive_direction,highest_volume_per_hour_positive_direction),
    highest_volume_per_hour_negative_direction = coalesce(excluded.highest_volume_per_hour_negative_direction,highest_volume_per_hour_negative_direction),
    highest_volume_per_hour_total = coalesce(excluded.highest_volume_per_hour_total,highest_volume_per_hour_total),
    "15th_highest_volume_per_hour_positive_direction" = coalesce(excluded."15th_highest_volume_per_hour_positive_direction","15th_highest_volume_per_hour_positive_direction"),
    "15th_highest_volume_per_hour_negative_direction" = coalesce(excluded."15th_highest_volume_per_hour_negative_direction","15th_highest_volume_per_hour_negative_direction"),
    "15th_highest_volume_per_hour_total" = coalesce(excluded."15th_highest_volume_per_hour_total","15th_highest_volume_per_hour_total"),
    "30th_highest_volume_per_hour_positive_direction" = coalesce(excluded."30th_highest_volume_per_hour_positive_direction","30th_highest_volume_per_hour_positive_direction"),
    "30th_highest_volume_per_hour_negative_direction" = coalesce(excluded."30th_highest_volume_per_hour_negative_direction","30th_highest_volume_per_hour_negative_direction"),
    "30th_highest_volume_per_hour_total" = coalesce(excluded."30th_highest_volume_per_hour_total","30th_highest_volume_per_hour_total"),
    "15th_percentile_speed_positive_direction" = coalesce(excluded."15th_percentile_speed_positive_direction","15th_percentile_speed_positive_direction"),
    "15th_percentile_speed_negative_direction" = coalesce(excluded."15th_percentile_speed_negative_direction","15th_percentile_speed_negative_direction"),
    "15th_percentile_speed_total" = coalesce(excluded."15th_percentile_speed_total","15th_percentile_speed_total"),
    "85th_percentile_speed_positive_direction" = coalesce(excluded."85th_percentile_speed_positive_direction","85th_percentile_speed_positive_direction"),
    "85th_percentile_speed_negative_direction" = coalesce(excluded."85th_percentile_speed_negative_direction","85th_percentile_speed_negative_direction"),
    "85th_percentile_speed_total" = coalesce(excluded."85th_percentile_speed_total","85th_percentile_speed_total"),
    "year" = coalesce(excluded."year","year"),
    positive_direction = coalesce(excluded.positive_direction,positive_direction),
    negative_direction = coalesce(excluded.negative_direction,negative_direction),
    avg_weekday_traffic = coalesce(excluded.avg_weekday_traffic,avg_weekday_traffic),
    number_of_days_counted = coalesce(excluded.number_of_days_counted,number_of_days_counted),
    duration_hours = coalesce(excluded.duration_hours,duration_hours)
    ;
    """
    return qry
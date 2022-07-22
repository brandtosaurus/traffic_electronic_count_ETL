import pandas as pd
import numpy as np
import config
import rsa_data_summary as rd

import uuid

class WimData(object):
    def __init__(self, df: pd.DataFrame, header: pd.DataFrame) -> None:
        self.dtype10, self.dtype10_sub_data = WimData.dtype10(df)
        self.dtype10 = rd.Data.join_header_id(self.dtype10, header)
        self.type10_separate_table = WimData.type10_separate_table(df)

    def type_10(self, df: pd.DataFrame) -> pd.DataFrame:
        data = df.loc[(df[0] == "10") & (df[3].isin(["1", "0"]))].dropna(
            axis=1, how="all"
        )
        dfh2 = pd.DataFrame(df.loc[(df[0].isin(["S0", "L1"]))]).dropna(
            axis=1, how="all"
        )

        if data.empty:
            print("wim data df empty")
        else:
            num_of_fields = int(data.iloc[:,1].unique()[0])
            ddf = data.iloc[:,: 2 + num_of_fields]
            ddf.reset_index(inplace=True)

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
            max_lanes = ddf["physical_lane_number"].max()
            try:
                ddf["direction"] = ddf.apply(
                lambda x: "P" if x["physical_lane_number"] <= (int(max_lanes) / 2) else "N",
                axis=1,
            )
                direction = dfh2.loc[dfh2[0] == "L1", 1:3]
                direction = direction.drop_duplicates()
            except:
                pass

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
            t1 = dfh2.loc[dfh2[0] == "S0", 1].unique()
            ddf["site_id"] = str(t1[0])
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

    def header_update_type10(self, data: pd.DataFrame) -> pd.DataFrame:
        speed_limit_qry = f"select max_speed from trafc.countstation where tcname = '{data['site_id'][0]}' ;"
        speed_limit = pd.read_sql_query(speed_limit_qry,config.ENGINE)
        speed_limit = speed_limit['max_speed'][0]

        UPDATE_STRING = f"""update
            trafc.electronic_count_header
            set
            total_light_positive_direction = {data.loc[(data['vehicle_class_code_secondary_scheme']<=1)&(data['direction']=='N')].count()[0].round().astype(int)},
            total_light_negative_direction = {data.loc[(data['vehicle_class_code_secondary_scheme']<=1)&(data['direction']=='P')].count()[0].round().astype(int)},
            total_light_vehicles = {data.loc[data['vehicle_class_code_secondary_scheme']<=1].count()[0].round().astype(int)},
            total_heavy_positive_direction = {data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='N')].count()[0].round().astype(int)},
            total_heavy_negative_direction = {data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='P')].count()[0].round().astype(int)},
            total_heavy_vehicles = {data.loc[data['vehicle_class_code_secondary_scheme']>1].count()[0].round().astype(int)},
            total_short_heavy_positive_direction = {data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='N')].count()[0].round().astype(int)},
            total_short_heavy_negative_direction = {data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='P')].count()[0].round().astype(int)},
            total_short_heavy_vehicles = {data.loc[data['vehicle_class_code_secondary_scheme']==2].count()[0].round().astype(int)},
            total_medium_heavy_positive_direction = {data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='N')].count()[0].round().astype(int)},
            total_medium_heavy_negative_direction = {data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='P')].count()[0].round().astype(int)},
            total_medium_heavy_vehicles = {data.loc[data['vehicle_class_code_secondary_scheme']==3].count()[0].round().astype(int)},
            total_long_heavy_positive_direction = {data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='N')].count()[0].round().astype(int)},
            total_long_heavy_negative_direction = {data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='P')].count()[0].round().astype(int)},
            total_long_heavy_vehicles = {data.loc[data['vehicle_class_code_secondary_scheme']==4].count()[0].round().astype(int)},
            total_vehicles_positive_direction = {data.loc[data['direction']=='N'].count()[0].round().astype(int)},
            total_vehicles_negative_direction = {data.loc[data['direction']=='P'].count()[0].round().astype(int)},
            total_vehicles = {data.count()[0]},
            average_speed_positive_direction = {data.loc[data['direction']=='N']['vehicle_speed'].mean().round(2)},
            average_speed_negative_direction = {data.loc[data['direction']=='P']['vehicle_speed'].mean().round(2)},
            average_speed = {data['vehicle_speed'].mean().round(2)},
            average_speed_light_vehicles_positive_direction = {data['vehicle_speed'].loc[(data['vehicle_class_code_secondary_scheme']<=1)&(data['direction']=='N')].mean().round(2)},
            average_speed_light_vehicles_negative_direction = {data['vehicle_speed'].loc[(data['vehicle_class_code_secondary_scheme']<=1)&(data['direction']=='P')].mean().round(2)},
            average_speed_light_vehicles = {data['vehicle_speed'].loc[data['vehicle_class_code_secondary_scheme']<=1].mean().round(2)},
            average_speed_heavy_vehicles_positive_direction = {data['vehicle_speed'].loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='N')].mean().round(2)},
            average_speed_heavy_vehicles_negative_direction = {data['vehicle_speed'].loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='P')].mean().round(2)},
            average_speed_heavy_vehicles = {data['vehicle_speed'].loc[data['vehicle_class_code_secondary_scheme']>1].mean().round(2)},
            truck_split_positive_direction = '{str((((data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='N')].count()/data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='N')].count())[0])*100).round().astype(int)) +":"+ str((((data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='P')].count()/data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='P')].count())[0])*100).round().astype(int)) +":"+ str((((data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='N')].count()/data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='P')].count())[0])*100).round().astype(int))}',
            truck_split_negative_direction = '{str((((data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='P')].count()/data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='P')].count())[0])*100).round().astype(int)) +":"+ str((((data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='N')].count()/data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='N')].count())[0])*100).round().astype(int)) +":"+ str((((data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='P')].count()/data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='N')].count())[0])*100).round().astype(int))}',
            truck_split_total = '{str((((data.loc[data['vehicle_class_code_secondary_scheme']==2].count()/data.loc[data['vehicle_class_code_secondary_scheme']>1].count())[0])*100).round().astype(int)) +":"+ str((((data.loc[data['vehicle_class_code_secondary_scheme']==3].count()/data.loc[data['vehicle_class_code_secondary_scheme']>1].count())[0])*100).round().astype(int)) +":"+ str((((data.loc[data['vehicle_class_code_secondary_scheme']==4].count()/data.loc[data['vehicle_class_code_secondary_scheme']>1].count())[0])*100).round().astype(int))}',
            estimated_axles_per_truck_negative_direction = {((data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='N')].count()[0]*2+data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='N')].count()[0]*5+data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='N')].count()[0]*7)/(data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='N')].count()[0]+data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='N')].count()[0]+data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='N')].count()[0])).round(2)},
            estimated_axles_per_truck_positive_direction = {((data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='P')].count()[0]*2+data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='P')].count()[0]*5+data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='P')].count()[0]*7)/(data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='P')].count()[0]+data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='P')].count()[0]+data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='P')].count()[0])).round(2)},
            estimated_axles_per_truck_total = {((data.loc[data['vehicle_class_code_secondary_scheme']==2].count()[0]*2+data.loc[data['vehicle_class_code_secondary_scheme']==3].count()[0]*5+data.loc[data['vehicle_class_code_secondary_scheme']==4].count()[0]*7)/(data.loc[data['vehicle_class_code_secondary_scheme']==2].count()[0]+data.loc[data['vehicle_class_code_secondary_scheme']==3].count()[0]+data.loc[data['vehicle_class_code_secondary_scheme']==4].count()[0])).round(2)},
            percentage_speeding_positive_direction = {((data.loc[(data['vehicle_speed']>speed_limit)&(data['direction']=='P')].count()[0]/data.loc[data['direction'=='P']].count()[0])*100).round(2)},
            percentage_speeding_negative_direction = {((data.loc[(data['vehicle_speed']>speed_limit)&(data['direction']=='N')].count()[0]/data.loc[data['direction'=='N']].count()[0])*100).round(2)},
            percentage_speeding_total = {((data.loc[data['vehicle_speed']>speed_limit].count()[0]/data.count()[0])*100).round(2)},
            vehicles_with_rear_to_rear_headway_less_than_2sec_positive_dire = {data.loc[(data['vehicle_following_code']==2)&data['direction']=='N'].count()[0]},
            vehicles_with_rear_to_rear_headway_less_than_2sec_negative_dire = {data.loc[(data['vehicle_following_code']==2)&data['direction']=='P'].count()[0]},
            vehicles_with_rear_to_rear_headway_less_than_2sec_total = {data.loc[data['vehicle_following_code']==2].count()[0]},
            estimated_e80_positive_direction = {data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='N')].count()[0]*0.6+data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='N')].count()[0]*2.5+data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='N')].count()[0]*2.1},
            estimated_e80_negative_direction = {data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='P')].count()[0]*0.6+data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='P')].count()[0]*2.5+data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='P')].count()[0]*2.1},
            estimated_e80_on_road = {data.loc[data['vehicle_class_code_secondary_scheme']==2].count()[0]*0.6+data.loc[data['vehicle_class_code_secondary_scheme']==3].count()[0]*2.5+data.loc[data['vehicle_class_code_secondary_scheme']==4].count()[0]*2.1},
            adt_positive_direction = {data.loc[data['direction']=='N'].groupby(pd.Grouper(key='start_datetime',freq='D')).count().mean()[0].round().astype(int)},
            adt_negative_direction = {data.loc[data['direction']=='P'].groupby(pd.Grouper(key='start_datetime',freq='D')).count().mean()[0].round().astype(int)},
            adt_total = {data.groupby(pd.Grouper(key='start_datetime',freq='D')).count().mean()[0].round().astype(int)},
            adtt_positive_direction = {data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='N')].groupby(pd.Grouper(key='start_datetime',freq='D')).count().mean()[0].round().astype(int)},
            adtt_negative_direction = {data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='P')].groupby(pd.Grouper(key='start_datetime',freq='D')).count().mean()[0].round().astype(int)},
            adtt_total = {data.loc[data['vehicle_class_code_secondary_scheme']>1].groupby(pd.Grouper(key='start_datetime',freq='D')).count().mean()[0].round().astype(int)},
            highest_volume_per_hour_positive_direction = {data.loc[data['direction']=='N'].groupby(pd.Grouper(key='start_datetime',freq='H')).count().max()[0]},
            highest_volume_per_hour_negative_direction = {data.loc[data['direction']=='P'].groupby(pd.Grouper(key='start_datetime',freq='H')).count().max()[0]},
            highest_volume_per_hour_total = {data.groupby(pd.Grouper(key='start_datetime',freq='H')).count().max()[0]},
            "15th_highest_volume_per_hour_positive_direction" = {data.loc[data['direction']=='N'].groupby(pd.Grouper(key='start_datetime',freq='D')).count().quantile(0.15)[0].astype(int)},
            "15th_highest_volume_per_hour_negative_direction" = {data.loc[data['direction']=='P'].groupby(pd.Grouper(key='start_datetime',freq='D')).count().quantile(0.15)[0].astype(int)},
            "15th_highest_volume_per_hour_total" = {data.groupby(pd.Grouper(key='start_datetime',freq='D')).count().quantile(0.15)[0].astype(int)},
            "30th_highest_volume_per_hour_positive_direction" = {data.loc[data['direction']=='N'].groupby(pd.Grouper(key='start_datetime',freq='D')).count().quantile(0.30)[0].astype(int)},
            "30th_highest_volume_per_hour_negative_direction" = {data.loc[data['direction']=='P'].groupby(pd.Grouper(key='start_datetime',freq='D')).count().quantile(0.30)[0].astype(int)},
            "30th_highest_volume_per_hour_total" = {data.groupby(pd.Grouper(key='start_datetime',freq='D')).count().quantile(0.30)[0].astype(int)},
            "15th_percentile_speed_positive_direction" = {data.loc[data['direction']=='N']['vehicle_speed'].quantile(0.15).round(2)},
            "15th_percentile_speed_negative_direction" = {data.loc[data['direction']=='P']['vehicle_speed'].quantile(0.15).round(2)},
            "15th_percentile_speed_total" = {data['vehicle_speed'].quantile(0.15).round(2)},
            "85th_percentile_speed_positive_direction" = {data.loc[data['direction']=='N']['vehicle_speed'].quantile(0.85).round(2)},
            "85th_percentile_speed_negative_direction" = {data.loc[data['direction']=='P']['vehicle_speed'].quantile(0.85).round(2)},
            "85th_percentile_speed_total" = {data['vehicle_speed'].quantile(0.85).round(2)},
            avg_weekday_traffic = {data.groupby(pd.Grouper(key='start_datetime',freq='B')).count().mean()[0].round().astype(int)},
            number_of_days_counted = {data.groupby([data['start_datetime'].dt.to_period('D')]).count().count()[0]},
            duration_hours = {data.groupby([data['start_datetime'].dt.to_period('H')]).count().count()[0]}
            where
            site_id = '{data['site_id'][0]}'
            and start_datetime = '{data['start_datetime'][0]}'
            and end_datetime = '{data['end_datetime'][0]}';
            """
        return UPDATE_STRING

def data_insert_type10(row):
    qry = f"""
    INSERT INTO trafc.electronic_count_data_type_10 (
        site_id,
        header_id,
        "year",
        number_of_fields_associated_with_the_basic_vehicle_data,
        data_source_code,
        edit_code,
        departure_date,
        departure_time,
        assigned_lane_number,
        physical_lane_number,
        forward_reverse_code,
        vehicle_category,
        vehicle_class_code_primary_scheme,
        vehicle_class_code_secondary_scheme,
        vehicle_speed,
        vehicle_length,
        site_occupancy_time_in_milliseconds,
        chassis_height_code,
        vehicle_following_code,
        vehicle_tag_code,
        trailer_count,
        axle_count,
        bumper_to_1st_axle_spacing,
        tyre_type,
        start_datetime,
        direction,
        data_id
    )
    VALUES (
        '{row['site_id']}',
        '{row['header_id']}',
        {row['year']},
        {row['number_of_fields_associated_with_the_basic_vehicle_data']},
        {row['data_source_code']},
        {row['edit_code']},
        '{row['departure_date']}',
        '{row['departure_time']}',
        {row['assigned_lane_number']},
        {row['physical_lane_number']},
        {row['forward_reverse_code']},
        {row['vehicle_category']},
        {row['vehicle_class_code_primary_scheme']},
        {row['vehicle_class_code_secondary_scheme']},
        {row['vehicle_speed']},
        {row['vehicle_length']},
        {row['site_occupancy_time_in_milliseconds']},
        {row['chassis_height_code']},
        {row['vehicle_following_code']},
        {row['vehicle_tag_code']},
        {row['trailer_count']},
        {row['axle_count']},
        {row['bumper_to_1st_axle_spacing']},
        {row['tyre_type']},
        '{row['start_datetime']}',
        '{row['direction']}',
        '{row['data_id']}'
    )
    """
    return qry

def data_update_type10(row):
    qry = f"""
    UPDATE trafc.electronic_count_data_type_10 SET
        site_id = '{row['site_id']}',
        header_id = '{row['header_id']}',
        "year" = {row['year']},
        number_of_fields_associated_with_the_basic_vehicle_data = {row['number_of_fields_associated_with_the_basic_vehicle_data']},
        data_source_code = {row['data_source_code']},
        edit_code = {row['edit_code']},
        departure_date = '{row['departure_date']}',
        departure_time = '{row['departure_time']}',
        assigned_lane_number = {row['assigned_lane_number']},
        physical_lane_number = {row['physical_lane_number']},
        forward_reverse_code = {row['forward_reverse_code']},
        vehicle_category = {row['vehicle_category']},
        vehicle_class_code_primary_scheme = {row['vehicle_class_code_primary_scheme']},
        vehicle_class_code_secondary_scheme = {row['vehicle_class_code_secondary_scheme']},
        vehicle_speed = {row['vehicle_speed']},
        vehicle_length = {row['vehicle_length']},
        site_occupancy_time_in_milliseconds = {row['site_occupancy_time_in_milliseconds']},
        chassis_height_code = {row['chassis_height_code']},
        vehicle_following_code = {row['vehicle_following_code']},
        vehicle_tag_code = {row['vehicle_tag_code']},
        trailer_count = {row['trailer_count']},
        axle_count = {row['axle_count']},
        bumper_to_1st_axle_spacing = {row['bumper_to_1st_axle_spacing']},
        tyre_type = {row['tyre_type']},
        start_datetime'{row['start_datetime']}',
        direction'{row['direction']}',
        data_id'{row['data_id']}'
        where site_id = '{row['site_id']}' and physical_lane_number = {row['physical_lane_number']} and start_datetime'{row['start_datetime']}' 
    )
    """
    return qry

def header_update_type10(data):
    speed_limit_qry = f"select max_speed from trafc.countstation where tcname = '{data['site_id'][0]}' ;"
    speed_limit = pd.read_sql_query(speed_limit_qry,config.ENGINE)
    speed_limit = speed_limit['max_speed'][0]

    UPDATE_STRING = f"""update
        trafc.electronic_count_header
        set
        total_light_positive_direction = {data.loc[(data['vehicle_class_code_secondary_scheme']<=1)&(data['direction']=='N')].count()[0].round().astype(int)},
        total_light_negative_direction = {data.loc[(data['vehicle_class_code_secondary_scheme']<=1)&(data['direction']=='P')].count()[0].round().astype(int)},
        total_light_vehicles = {data.loc[data['vehicle_class_code_secondary_scheme']<=1].count()[0].round().astype(int)},
        total_heavy_positive_direction = {data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='N')].count()[0].round().astype(int)},
        total_heavy_negative_direction = {data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='P')].count()[0].round().astype(int)},
        total_heavy_vehicles = {data.loc[data['vehicle_class_code_secondary_scheme']>1].count()[0].round().astype(int)},
        total_short_heavy_positive_direction = {data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='N')].count()[0].round().astype(int)},
        total_short_heavy_negative_direction = {data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='P')].count()[0].round().astype(int)},
        total_short_heavy_vehicles = {data.loc[data['vehicle_class_code_secondary_scheme']==2].count()[0].round().astype(int)},
        total_medium_heavy_positive_direction = {data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='N')].count()[0].round().astype(int)},
        total_medium_heavy_negative_direction = {data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='P')].count()[0].round().astype(int)},
        total_medium_heavy_vehicles = {data.loc[data['vehicle_class_code_secondary_scheme']==3].count()[0].round().astype(int)},
        total_long_heavy_positive_direction = {data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='N')].count()[0].round().astype(int)},
        total_long_heavy_negative_direction = {data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='P')].count()[0].round().astype(int)},
        total_long_heavy_vehicles = {data.loc[data['vehicle_class_code_secondary_scheme']==4].count()[0].round().astype(int)},
        total_vehicles_positive_direction = {data.loc[data['direction']=='N'].count()[0].round().astype(int)},
        total_vehicles_negative_direction = {data.loc[data['direction']=='P'].count()[0].round().astype(int)},
        total_vehicles = {data.count()[0]},
        average_speed_positive_direction = {data.loc[data['direction']=='N']['vehicle_speed'].mean().round(2)},
        average_speed_negative_direction = {data.loc[data['direction']=='P']['vehicle_speed'].mean().round(2)},
        average_speed = {data['vehicle_speed'].mean().round(2)},
        average_speed_light_vehicles_positive_direction = {data['vehicle_speed'].loc[(data['vehicle_class_code_secondary_scheme']<=1)&(data['direction']=='N')].mean().round(2)},
        average_speed_light_vehicles_negative_direction = {data['vehicle_speed'].loc[(data['vehicle_class_code_secondary_scheme']<=1)&(data['direction']=='P')].mean().round(2)},
        average_speed_light_vehicles = {data['vehicle_speed'].loc[data['vehicle_class_code_secondary_scheme']<=1].mean().round(2)},
        average_speed_heavy_vehicles_positive_direction = {data['vehicle_speed'].loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='N')].mean().round(2)},
        average_speed_heavy_vehicles_negative_direction = {data['vehicle_speed'].loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='P')].mean().round(2)},
        average_speed_heavy_vehicles = {data['vehicle_speed'].loc[data['vehicle_class_code_secondary_scheme']>1].mean().round(2)},
        truck_split_positive_direction = '{str((((data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='N')].count()/data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='N')].count())[0])*100).round().astype(int)) +":"+ str((((data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='P')].count()/data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='P')].count())[0])*100).round().astype(int)) +":"+ str((((data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='N')].count()/data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='P')].count())[0])*100).round().astype(int))}',
        truck_split_negative_direction = '{str((((data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='P')].count()/data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='P')].count())[0])*100).round().astype(int)) +":"+ str((((data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='N')].count()/data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='N')].count())[0])*100).round().astype(int)) +":"+ str((((data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='P')].count()/data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='N')].count())[0])*100).round().astype(int))}',
        truck_split_total = '{str((((data.loc[data['vehicle_class_code_secondary_scheme']==2].count()/data.loc[data['vehicle_class_code_secondary_scheme']>1].count())[0])*100).round().astype(int)) +":"+ str((((data.loc[data['vehicle_class_code_secondary_scheme']==3].count()/data.loc[data['vehicle_class_code_secondary_scheme']>1].count())[0])*100).round().astype(int)) +":"+ str((((data.loc[data['vehicle_class_code_secondary_scheme']==4].count()/data.loc[data['vehicle_class_code_secondary_scheme']>1].count())[0])*100).round().astype(int))}',
        estimated_axles_per_truck_negative_direction = {((data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='N')].count()[0]*2+data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='N')].count()[0]*5+data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='N')].count()[0]*7)/(data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='N')].count()[0]+data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='N')].count()[0]+data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='N')].count()[0])).round(2)},
        estimated_axles_per_truck_positive_direction = {((data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='P')].count()[0]*2+data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='P')].count()[0]*5+data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='P')].count()[0]*7)/(data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='P')].count()[0]+data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='P')].count()[0]+data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='P')].count()[0])).round(2)},
        estimated_axles_per_truck_total = {((data.loc[data['vehicle_class_code_secondary_scheme']==2].count()[0]*2+data.loc[data['vehicle_class_code_secondary_scheme']==3].count()[0]*5+data.loc[data['vehicle_class_code_secondary_scheme']==4].count()[0]*7)/(data.loc[data['vehicle_class_code_secondary_scheme']==2].count()[0]+data.loc[data['vehicle_class_code_secondary_scheme']==3].count()[0]+data.loc[data['vehicle_class_code_secondary_scheme']==4].count()[0])).round(2)},
        percentage_speeding_positive_direction = {((data.loc[(data['vehicle_speed']>speed_limit)&(data['direction']=='P')].count()[0]/data.loc[data['direction'=='P']].count()[0])*100).round(2)},
        percentage_speeding_negative_direction = {((data.loc[(data['vehicle_speed']>speed_limit)&(data['direction']=='N')].count()[0]/data.loc[data['direction'=='N']].count()[0])*100).round(2)},
        percentage_speeding_total = {((data.loc[data['vehicle_speed']>speed_limit].count()[0]/data.count()[0])*100).round(2)},
        vehicles_with_rear_to_rear_headway_less_than_2sec_positive_dire = {data.loc[(data['vehicle_following_code']==2)&data['direction']=='N'].count()[0]},
        vehicles_with_rear_to_rear_headway_less_than_2sec_negative_dire = {data.loc[(data['vehicle_following_code']==2)&data['direction']=='P'].count()[0]},
        vehicles_with_rear_to_rear_headway_less_than_2sec_total = {data.loc[data['vehicle_following_code']==2].count()[0]},
        estimated_e80_positive_direction = {data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='N')].count()[0]*0.6+data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='N')].count()[0]*2.5+data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='N')].count()[0]*2.1},
        estimated_e80_negative_direction = {data.loc[(data['vehicle_class_code_secondary_scheme']==2)&(data['direction']=='P')].count()[0]*0.6+data.loc[(data['vehicle_class_code_secondary_scheme']==3)&(data['direction']=='P')].count()[0]*2.5+data.loc[(data['vehicle_class_code_secondary_scheme']==4)&(data['direction']=='P')].count()[0]*2.1},
        estimated_e80_on_road = {data.loc[data['vehicle_class_code_secondary_scheme']==2].count()[0]*0.6+data.loc[data['vehicle_class_code_secondary_scheme']==3].count()[0]*2.5+data.loc[data['vehicle_class_code_secondary_scheme']==4].count()[0]*2.1},
        adt_positive_direction = {data.loc[data['direction']=='N'].groupby(pd.Grouper(key='start_datetime',freq='D')).count().mean()[0].round().astype(int)},
        adt_negative_direction = {data.loc[data['direction']=='P'].groupby(pd.Grouper(key='start_datetime',freq='D')).count().mean()[0].round().astype(int)},
        adt_total = {data.groupby(pd.Grouper(key='start_datetime',freq='D')).count().mean()[0].round().astype(int)},
        adtt_positive_direction = {data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='N')].groupby(pd.Grouper(key='start_datetime',freq='D')).count().mean()[0].round().astype(int)},
        adtt_negative_direction = {data.loc[(data['vehicle_class_code_secondary_scheme']>1)&(data['direction']=='P')].groupby(pd.Grouper(key='start_datetime',freq='D')).count().mean()[0].round().astype(int)},
        adtt_total = {data.loc[data['vehicle_class_code_secondary_scheme']>1].groupby(pd.Grouper(key='start_datetime',freq='D')).count().mean()[0].round().astype(int)},
        highest_volume_per_hour_positive_direction = {data.loc[data['direction']=='N'].groupby(pd.Grouper(key='start_datetime',freq='H')).count().max()[0]},
        highest_volume_per_hour_negative_direction = {data.loc[data['direction']=='P'].groupby(pd.Grouper(key='start_datetime',freq='H')).count().max()[0]},
        highest_volume_per_hour_total = {data.groupby(pd.Grouper(key='start_datetime',freq='H')).count().max()[0]},
        "15th_highest_volume_per_hour_positive_direction" = {data.loc[data['direction']=='N'].groupby(pd.Grouper(key='start_datetime',freq='D')).count().quantile(0.15)[0].astype(int)},
        "15th_highest_volume_per_hour_negative_direction" = {data.loc[data['direction']=='P'].groupby(pd.Grouper(key='start_datetime',freq='D')).count().quantile(0.15)[0].astype(int)},
        "15th_highest_volume_per_hour_total" = {data.groupby(pd.Grouper(key='start_datetime',freq='D')).count().quantile(0.15)[0].astype(int)},
        "30th_highest_volume_per_hour_positive_direction" = {data.loc[data['direction']=='N'].groupby(pd.Grouper(key='start_datetime',freq='D')).count().quantile(0.30)[0].astype(int)},
        "30th_highest_volume_per_hour_negative_direction" = {data.loc[data['direction']=='P'].groupby(pd.Grouper(key='start_datetime',freq='D')).count().quantile(0.30)[0].astype(int)},
        "30th_highest_volume_per_hour_total" = {data.groupby(pd.Grouper(key='start_datetime',freq='D')).count().quantile(0.30)[0].astype(int)},
        "15th_percentile_speed_positive_direction" = {data.loc[data['direction']=='N']['vehicle_speed'].quantile(0.15).round(2)},
        "15th_percentile_speed_negative_direction" = {data.loc[data['direction']=='P']['vehicle_speed'].quantile(0.15).round(2)},
        "15th_percentile_speed_total" = {data['vehicle_speed'].quantile(0.15).round(2)},
        "85th_percentile_speed_positive_direction" = {data.loc[data['direction']=='N']['vehicle_speed'].quantile(0.85).round(2)},
        "85th_percentile_speed_negative_direction" = {data.loc[data['direction']=='P']['vehicle_speed'].quantile(0.85).round(2)},
        "85th_percentile_speed_total" = {data['vehicle_speed'].quantile(0.85).round(2)},
        avg_weekday_traffic = {data.groupby(pd.Grouper(key='start_datetime',freq='B')).count().mean()[0].round().astype(int)},
        number_of_days_counted = {data.groupby([data['start_datetime'].dt.to_period('D')]).count().count()[0]},
        duration_hours = {data.groupby([data['start_datetime'].dt.to_period('H')]).count().count()[0]}
        where
        site_id = '{data['site_id'][0]}'
        and start_datetime = '{data['start_datetime'][0]}'
        and end_datetime = '{data['end_datetime'][0]}';
        """
    return UPDATE_STRING

def wim_stations_header_insert_qrys(header_id):
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

def wim_stations_header_insert_dfs(SELECT_TYPE10_QRY, AXLE_SPACING_SELECT_QRY, WHEEL_MASS_SELECT_QRY):
    df = pd.read_sql_query(SELECT_TYPE10_QRY,config.ENGINE)
    df = df.fillna(0)
    df2 = pd.read_sql_query(AXLE_SPACING_SELECT_QRY,config.ENGINE)
    df2 = df2.fillna(0)
    df3 = pd.read_sql_query(WHEEL_MASS_SELECT_QRY,config.ENGINE)
    df3 = df3.fillna(0)
    return df, df2, df3

def wim_stations_header_upsert(header_id, df, df2, df3):
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
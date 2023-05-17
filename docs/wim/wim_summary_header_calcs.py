import pandas as pd

def summary_header_calcs(self, header: pd.DataFrame, data: pd.DataFrame, type: int) -> pd.DataFrame:
    try:
        speed_limit_qry = f"select max_speed from trafc.countstation where tcname = '{self.site_id}' ;"
        speed_limit = pd.read_sql_query(
            speed_limit_qry, config.ENGINE).reset_index(drop=True)
        try:
            speed_limit = speed_limit['max_speed'].iloc[0]
        except IndexError:
            speed_limit = 60
        if data.empty:
            pass
        else:
            data = data.fillna(0, axis=0)
            if type == 10:
                try:
                    header['total_light_negative_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme'] <= 1) & (
                        data['direction'] == 'N')].count()[0].round().astype(int)
                except (ValueError, IndexError):
                    header['total_light_negative_direction'] = 0
                try:
                    header['total_light_positive_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme'] <= 1) & (
                        data['direction'] == 'P')].count()[0].round().astype(int)
                except (ValueError, IndexError):
                    header['total_light_positive_direction'] = 0
                header['total_light_vehicles'] = data.loc[data['vehicle_class_code_secondary_scheme'] <= 1].count()[
                    0].round().astype(int)
                try:
                    header['total_heavy_negative_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme'] > 1) & (
                        data['direction'] == 'N')].count()[0].round().astype(int)
                except (ValueError, IndexError):
                    header['total_heavy_negative_direction'] = 0
                try:
                    header['total_heavy_positive_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme'] > 1) & (
                        data['direction'] == 'P')].count()[0].round().astype(int)
                except (ValueError, IndexError):
                    header['total_heavy_positive_direction'] = 0
                header['total_heavy_vehicles'] = data.loc[data['vehicle_class_code_secondary_scheme'] > 1].count()[
                    0].round().astype(int)
                try:
                    header['total_short_heavy_negative_direction'] = data.loc[(
                        data['vehicle_class_code_secondary_scheme'] == 2) & (data['direction'] == 'N')].count()[0].round().astype(int)
                except (ValueError, IndexError):
                    header['total_short_heavy_negative_direction'] = 0
                try:
                    header['total_short_heavy_positive_direction'] = data.loc[(
                        data['vehicle_class_code_secondary_scheme'] == 2) & (data['direction'] == 'P')].count()[0].round().astype(int)
                except (ValueError, IndexError):
                    header['total_short_heavy_positive_direction'] = 0
                header['total_short_heavy_vehicles'] = data.loc[data['vehicle_class_code_secondary_scheme'] == 2].count()[
                    0].round().astype(int)
                try:
                    header['total_medium_heavy_negative_direction'] = data.loc[(
                        data['vehicle_class_code_secondary_scheme'] == 3) & (data['direction'] == 'N')].count()[0].round().astype(int)
                except (ValueError, IndexError):
                    header['total_medium_heavy_negative_direction'] = 0
                try:
                    header['total_medium_heavy_positive_direction'] = data.loc[(
                        data['vehicle_class_code_secondary_scheme'] == 3) & (data['direction'] == 'P')].count()[0].round().astype(int)
                except (ValueError, IndexError):
                    header['total_medium_heavy_positive_direction'] = 0
                header['total_medium_heavy_vehicles'] = data.loc[data['vehicle_class_code_secondary_scheme'] == 3].count()[
                    0].round().astype(int)
                try:
                    header['total_long_heavy_negative_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme'] == 4) & (
                        data['direction'] == 'N')].count()[0].round().astype(int)
                except (ValueError, IndexError):
                    header['total_long_heavy_negative_direction'] = 0
                try:
                    header['total_long_heavy_positive_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme'] == 4) & (
                        data['direction'] == 'P')].count()[0].round().astype(int)
                except (ValueError, IndexError):
                    header['total_long_heavy_positive_direction'] = 0
                header['total_long_heavy_vehicles'] = data.loc[data['vehicle_class_code_secondary_scheme'] == 4].count()[
                    0].round().astype(int)
                try:
                    header['total_vehicles_negative_direction'] = data.loc[data['direction'] == 'N'].count()[
                        0].round().astype(int)
                except (ValueError, IndexError):
                    header['total_vehicles_negative_direction'] = 0
                try:
                    header['total_vehicles_positive_direction'] = data.loc[data['direction'] == 'P'].count()[
                        0].round().astype(int)
                except (ValueError, IndexError):
                    header['total_vehicles_positive_direction'] = 0
                header['total_vehicles'] = data.count()[
                    0].round().astype(int)
                try:
                    header['average_speed_negative_direction'] = data.loc[data['direction']
                                                                            == 'N']['vehicle_speed'].mean().round(4)
                except (ValueError, IndexError):
                    header['average_speed_negative_direction'] = 0
                try:
                    header['average_speed_positive_direction'] = data.loc[data['direction']
                                                                            == 'P']['vehicle_speed'].mean().round(4)
                except (ValueError, IndexError):
                    header['average_speed_positive_direction'] = 0
                header['average_speed'] = data['vehicle_speed'].mean().round(4)
                try:
                    header['average_speed_light_vehicles_negative_direction'] = data['vehicle_speed'].loc[(
                        data['vehicle_class_code_secondary_scheme'] <= 1) & (data['direction'] == 'N')].mean().round(4)
                except (ValueError, IndexError):
                    header['average_speed_light_vehicles_negative_direction'] = 0
                try:
                    header['average_speed_light_vehicles_positive_direction'] = data['vehicle_speed'].loc[(
                        data['vehicle_class_code_secondary_scheme'] <= 1) & (data['direction'] == 'P')].mean().round(4)
                except (ValueError, IndexError):
                    header['average_speed_light_vehicles_positive_direction'] = 0
                header['average_speed_light_vehicles'] = data['vehicle_speed'].loc[
                    data['vehicle_class_code_secondary_scheme'] <= 1].mean().round(4)
                try:
                    header['average_speed_heavy_vehicles_negative_direction'] = data['vehicle_speed'].loc[(
                        data['vehicle_class_code_secondary_scheme'] > 1) & (data['direction'] == 'N')].mean().round(4)
                except (ValueError, IndexError):
                    header['average_speed_heavy_vehicles_negative_direction'] = 0
                try:
                    header['average_speed_heavy_vehicles_positive_direction'] = data['vehicle_speed'].loc[(
                        data['vehicle_class_code_secondary_scheme'] > 1) & (data['direction'] == 'P')].mean().round(4)
                except (ValueError, IndexError):
                    header['average_speed_heavy_vehicles_positive_direction'] = 0
                header['average_speed_heavy_vehicles'] = data['vehicle_speed'].loc[data['vehicle_class_code_secondary_scheme'] > 1].mean(
                ).round(4)
                try:
                    header['truck_split_negative_direction'] = {str((((data.loc[(data['vehicle_class_code_secondary_scheme'] == 2) & (data['direction'] == 'N')].count()/data.loc[(data['vehicle_class_code_secondary_scheme'] > 1) & (data['direction'] == 'N')].count())[0])*100).round().astype(int)) + ":" + str((((data.loc[(data['vehicle_class_code_secondary_scheme'] == 3) & (data['direction'] == 'N')].count(
                    )/data.loc[(data['vehicle_class_code_secondary_scheme'] > 1) & (data['direction'] == 'N')].count())[0])*100).round().astype(int)) + ":" + str((((data.loc[(data['vehicle_class_code_secondary_scheme'] == 4) & (data['direction'] == 'N')].count()/data.loc[(data['vehicle_class_code_secondary_scheme'] > 1) & (data['direction'] == 'N')].count())[0])*100).round().astype(int))}
                except (ValueError, IndexError):
                    header['truck_split_negative_direction'] = 0
                try:
                    header['truck_split_positive_direction'] = {str((((data.loc[(data['vehicle_class_code_secondary_scheme'] == 2) & (data['direction'] == 'P')].count()/data.loc[(data['vehicle_class_code_secondary_scheme'] > 1) & (data['direction'] == 'P')].count())[0])*100).round().astype(int)) + ":" + str((((data.loc[(data['vehicle_class_code_secondary_scheme'] == 3) & (data['direction'] == 'P')].count(
                    )/data.loc[(data['vehicle_class_code_secondary_scheme'] > 1) & (data['direction'] == 'P')].count())[0])*100).round().astype(int)) + ":" + str((((data.loc[(data['vehicle_class_code_secondary_scheme'] == 4) & (data['direction'] == 'P')].count()/data.loc[(data['vehicle_class_code_secondary_scheme'] > 1) & (data['direction'] == 'P')].count())[0])*100).round().astype(int))}
                except (ValueError, IndexError):
                    header['truck_split_positive_direction'] = 0
                header['truck_split_total'] = {str((((data.loc[data['vehicle_class_code_secondary_scheme'] == 2].count()/data.loc[data['vehicle_class_code_secondary_scheme'] > 1].count())[0])*100).round().astype(int)) + ":" + str((((data.loc[data['vehicle_class_code_secondary_scheme'] == 3].count(
                )/data.loc[data['vehicle_class_code_secondary_scheme'] > 1].count())[0])*100).round().astype(int)) + ":" + str((((data.loc[data['vehicle_class_code_secondary_scheme'] == 4].count()/data.loc[data['vehicle_class_code_secondary_scheme'] > 1].count())[0])*100).round().astype(int))}
                try:
                    header['estimated_axles_per_truck_negative_direction'] = ((data.loc[(data['vehicle_class_code_secondary_scheme'] == 2) & (data['direction'] == 'N')].count()[0]*2+data.loc[(data['vehicle_class_code_secondary_scheme'] == 3) & (data['direction'] == 'N')].count()[0]*5+data.loc[(data['vehicle_class_code_secondary_scheme'] == 4) & (data['direction'] == 'N')].count()[
                        0]*7)/(data.loc[(data['vehicle_class_code_secondary_scheme'] == 2) & (data['direction'] == 'N')].count()[0]+data.loc[(data['vehicle_class_code_secondary_scheme'] == 3) & (data['direction'] == 'N')].count()[0]+data.loc[(data['vehicle_class_code_secondary_scheme'] == 4) & (data['direction'] == 'N')].count()[0])).round(4)
                except (ValueError, IndexError):
                    header['estimated_axles_per_truck_negative_direction'] = 0
                try:
                    header['estimated_axles_per_truck_positive_direction'] = ((data.loc[(data['vehicle_class_code_secondary_scheme'] == 2) & (data['direction'] == 'P')].count()[0]*2+data.loc[(data['vehicle_class_code_secondary_scheme'] == 3) & (data['direction'] == 'P')].count()[0]*5+data.loc[(data['vehicle_class_code_secondary_scheme'] == 4) & (data['direction'] == 'P')].count()[
                        0]*7)/(data.loc[(data['vehicle_class_code_secondary_scheme'] == 2) & (data['direction'] == 'P')].count()[0]+data.loc[(data['vehicle_class_code_secondary_scheme'] == 3) & (data['direction'] == 'P')].count()[0]+data.loc[(data['vehicle_class_code_secondary_scheme'] == 4) & (data['direction'] == 'P')].count()[0])).round(4)
                except (ValueError, IndexError):
                    header['estimated_axles_per_truck_positive_direction'] = 0
                header['estimated_axles_per_truck_total'] = ((data.loc[data['vehicle_class_code_secondary_scheme'] == 2].count()[0]*2+data.loc[data['vehicle_class_code_secondary_scheme'] == 3].count()[0]*5+data.loc[data['vehicle_class_code_secondary_scheme'] == 4].count()[
                    0]*7)/(data.loc[data['vehicle_class_code_secondary_scheme'] == 2].count()[0]+data.loc[data['vehicle_class_code_secondary_scheme'] == 3].count()[0]+data.loc[data['vehicle_class_code_secondary_scheme'] == 4].count()[0])).round(4)
                try:
                    header['percentage_speeding_positive_direction'] = ((data.loc[(data['vehicle_speed'] > speed_limit) & (
                        data['direction'] == 'P')].count()[0]/data.loc[data['direction' == 'P']].count()[0])*100).round(4)
                except (ValueError, IndexError):
                    header['percentage_speeding_positive_direction'] = 0
                try:
                    header['percentage_speeding_negative_direction'] = ((data.loc[(data['vehicle_speed'] > speed_limit) & (
                        data['direction'] == 'N')].count()[0]/data.loc[data['direction' == 'N']].count()[0])*100).round(4)
                except (ValueError, IndexError):
                    header['percentage_speeding_negative_direction'] = 0
                header['percentage_speeding_total'] = (
                    (data.loc[data['vehicle_speed'] > speed_limit].count()[0]/data.count()[0])*100).round(4)
                try:
                    header['vehicles_with_rear_to_rear_headway_less_than_2sec_negative_dire'] = data.loc[(
                        data['vehicle_following_code'] == 2) & data['direction'] == 'N'].count()[0]
                except (ValueError, IndexError):
                    header['vehicles_with_rear_to_rear_headway_less_than_2sec_negative_dire'] = 0
                try:
                    header['vehicles_with_rear_to_rear_headway_less_than_2sec_positive_dire'] = data.loc[(
                        data['vehicle_following_code'] == 2) & data['direction'] == 'P'].count()[0]
                except (ValueError, IndexError):
                    header['vehicles_with_rear_to_rear_headway_less_than_2sec_positive_dire'] = 0
                header['vehicles_with_rear_to_rear_headway_less_than_2sec_total'] = data.loc[data['vehicle_following_code'] == 2].count()[
                    0]
                try:
                    header['estimated_e80_negative_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme'] == 2) & (data['direction'] == 'N')].count()[0]*0.6+data.loc[(
                        data['vehicle_class_code_secondary_scheme'] == 3) & (data['direction'] == 'N')].count()[0]*2.5+data.loc[(data['vehicle_class_code_secondary_scheme'] == 4) & (data['direction'] == 'N')].count()[0]*2.1
                except (ValueError, IndexError):
                    header['estimated_e80_negative_direction'] = 0
                try:
                    header['estimated_e80_positive_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme'] == 2) & (data['direction'] == 'P')].count()[0]*0.6+data.loc[(
                        data['vehicle_class_code_secondary_scheme'] == 3) & (data['direction'] == 'P')].count()[0]*2.5+data.loc[(data['vehicle_class_code_secondary_scheme'] == 4) & (data['direction'] == 'P')].count()[0]*2.1
                except (ValueError, IndexError):
                    header['estimated_e80_positive_direction'] = 0
                header['estimated_e80_on_road'] = data.loc[data['vehicle_class_code_secondary_scheme'] == 2].count(
                )[0]*0.6+data.loc[data['vehicle_class_code_secondary_scheme'] == 3].count()[0]*2.5+data.loc[data['vehicle_class_code_secondary_scheme'] == 4].count()[0]*2.1
                try:
                    header['adt_negative_direction'] = data.loc[data['direction'] == 'N'].groupby(
                        pd.Grouper(key='start_datetime', freq='D')).count().mean()[0].round().astype(int)
                except (ValueError, IndexError):
                    header['adt_negative_direction'] = 0
                try:
                    header['adt_positive_direction'] = data.loc[data['direction'] == 'P'].groupby(
                        pd.Grouper(key='start_datetime', freq='D')).count().mean()[0].round().astype(int)
                except (ValueError, IndexError):
                    header['adt_positive_direction'] = 0
                header['adt_total'] = data.groupby(pd.Grouper(
                    key='start_datetime', freq='D')).count().mean()[0].round().astype(int)
                try:
                    header['adtt_negative_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme'] > 1) & (
                        data['direction'] == 'N')].groupby(pd.Grouper(key='start_datetime', freq='D')).count().mean()[0].round().astype(int)
                except (ValueError, IndexError):
                    header['adtt_negative_direction'] = 0
                try:
                    header['adtt_positive_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme'] > 1) & (
                        data['direction'] == 'P')].groupby(pd.Grouper(key='start_datetime', freq='D')).count().mean()[0].round().astype(int)
                except (ValueError, IndexError):
                    header['adtt_positive_direction'] = 0
                header['adtt_total'] = data.loc[data['vehicle_class_code_secondary_scheme'] > 1].groupby(
                    pd.Grouper(key='start_datetime', freq='D')).count().mean()[0].round().astype(int)
                try:
                    header['highest_volume_per_hour_negative_direction'] = data.loc[data['direction'] == 'N'].groupby(
                        pd.Grouper(key='start_datetime', freq='H')).count().max()[0]
                except (ValueError, IndexError):
                    header['highest_volume_per_hour_negative_direction'] = 0
                try:
                    header['highest_volume_per_hour_positive_direction'] = data.loc[data['direction'] == 'P'].groupby(
                        pd.Grouper(key='start_datetime', freq='H')).count().max()[0]
                except (ValueError, IndexError):
                    header['highest_volume_per_hour_positive_direction'] = 0
                header['highest_volume_per_hour_total'] = data.groupby(
                    pd.Grouper(key='start_datetime', freq='H')).count().max()[0]
                try:
                    header["15th_highest_volume_per_hour_negative_direction"] = round(data.loc[data['direction'] == 'N'].groupby(
                        pd.Grouper(key='start_datetime', freq='D')).count().quantile(0.15)[0].round().astype(int))
                except (ValueError, IndexError):
                    header["15th_highest_volume_per_hour_negative_direction"] = 0
                try:
                    header["15th_highest_volume_per_hour_positive_direction"] = round(data.loc[data['direction'] == 'P'].groupby(
                        pd.Grouper(key='start_datetime', freq='D')).count().quantile(0.15)[0].round().astype(int))
                except (ValueError, IndexError):
                    header["15th_highest_volume_per_hour_positive_direction"] = 0
                header["15th_highest_volume_per_hour_total"] = data.groupby(pd.Grouper(
                    key='start_datetime', freq='D')).count().quantile(0.15)[0].round().astype(int)
                try:
                    header["30th_highest_volume_per_hour_negative_direction"] = round(data.loc[data['direction'] == 'N'].groupby(
                        pd.Grouper(key='start_datetime', freq='D')).count().quantile(0.30)[0].round().astype(int))
                except (ValueError, IndexError):
                    header["30th_highest_volume_per_hour_negative_direction"] = 0
                try:
                    header["30th_highest_volume_per_hour_positive_direction"] = round(data.loc[data['direction'] == 'P'].groupby(
                        pd.Grouper(key='start_datetime', freq='D')).count().quantile(0.30)[0].round().astype(int))
                except (ValueError, IndexError):
                    header["30th_highest_volume_per_hour_positive_direction"] = 0
                header["30th_highest_volume_per_hour_total"] = data.groupby(pd.Grouper(
                    key='start_datetime', freq='D')).count().quantile(0.30)[0].round().astype(int)
                try:
                    header["15th_percentile_speed_negative_direction"] = data.loc[data['direction']
                                                                                    == 'N']['vehicle_speed'].quantile(0.15).round(4)
                except (ValueError, IndexError):
                    header["15th_percentile_speed_negative_direction"] = 0
                try:
                    header["15th_percentile_speed_positive_direction"] = data.loc[data['direction']
                                                                                    == 'P']['vehicle_speed'].quantile(0.15).round(4)
                except (ValueError, IndexError):
                    header["15th_percentile_speed_positive_direction"] = 0
                header["15th_percentile_speed_total"] = data['vehicle_speed'].quantile(
                    0.15).round(4)
                try:
                    header["85th_percentile_speed_negative_direction"] = data.loc[data['direction']
                                                                                    == 'N']['vehicle_speed'].quantile(0.85).round(4)
                except (ValueError, IndexError):
                    header["85th_percentile_speed_negative_direction"] = 0
                try:
                    header["85th_percentile_speed_positive_direction"] = data.loc[data['direction']
                                                                                    == 'P']['vehicle_speed'].quantile(0.85).round(4)
                except (ValueError, IndexError):
                    header["85th_percentile_speed_positive_direction"] = 0
                header["85th_percentile_speed_total"] = data['vehicle_speed'].quantile(
                    0.85).round(4)
                header['avg_weekday_traffic'] = data.groupby(pd.Grouper(
                    key='start_datetime', freq='B')).count().mean()[0].round().astype(int)
                header['number_of_days_counted'] = data.groupby(
                    [data['start_datetime'].dt.to_period('D')]).count().count()[0]
                header['duration_hours'] = data.groupby(
                    [data['start_datetime'].dt.to_period('H')]).count().count()[0]

                return header
    except IndexError:
        return header

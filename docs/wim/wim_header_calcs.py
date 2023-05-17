import pandas as pd

def wim_header_calcs(self, df: pd.DataFrame, df2: pd.DataFrame, df3: pd.DataFrame):
    """
    df1 is the SELECT_TYPE10_QRY
    df2 is the AXLE_SPACING_SELECT_QRY
    df3 is the WHEEL_MASS_SELECT_QRY
    """

    try:
        self.egrl_percent = 1-(
            ((df.loc[df['edit_code'] == 2].count()[0])/(df.count()[0])))
    except:
        self.egrl_percent = 1
    try:
        self.egrl_percent_positive_direction = 1-((df.loc[(df['edit_code'] == 2) & (
            df['direction'] == 'P')].count()[0]/df.loc[df['direction'] == 'P'].count()[0]))
    except:
        self.egrl_percent_positive_direction = 1
    try:
        self.egrl_percent_negative_direction = 1-((df.loc[(df['edit_code'] == 2) & (
            df['direction'] == 'P')].count()[0]/df.loc[df['direction'] == 'N'].count()[0]))
    except:
        self.egrl_percent_negative_direction = 1
    try:
        self.egrw_percent = 1-(((df2.loc[df2['edit_code'] == 2].count(
        )[0]+df3.loc[df3['edit_code'] == 2].count()[0])/df.count()[0]))
    except:
        self.egrw_percent = 0
    try:
        self.egrw_percent_positive_direction = 1-(((df2.loc[(df2['edit_code'] == 2) & (df2['direction'] == 'P')].count(
        )[0]+df3.loc[(df3['edit_code'] == 2) & (df3['direction'] == 'P')].count()[0])/df.loc[df['direction'] == 'P'].count()[0]))
    except:
        self.egrw_percent_positive_direction = 1
    try:
        self.egrw_percent_negative_direction = 1-(((df2.loc[(df2['edit_code'] == 2) & (df2['direction'] == 'N')].count(
        )[0]+df3.loc[(df3['edit_code'] == 2) & (df3['direction'] == 'N')].count()[0])/df.loc[df['direction'] == 'N'].count()[0]))
    except:
        self.egrw_percent_negative_direction = 1

    self.num_weighed = df3.groupby(
        pd.Grouper(key='id')).count().count()[0] or 0
    self.num_weighed_positive_direction = df3.loc[df3['direction'] == 'P'].groupby(
        pd.Grouper(key='id')).count().count()[0] or 0
    self.num_weighed_negative_direction = df3.loc[df3['direction'] == 'N'].groupby(
        pd.Grouper(key='id')).count().count()[0] or 0
    self.mean_equivalent_axle_mass = round(
        (df3.groupby(pd.Grouper(key='id'))['wheel_mass'].mean()).mean(), 2) or 0
    self.mean_equivalent_axle_mass_positive_direction = round(
        (df3.loc[df3['direction'] == 'P'].groupby(pd.Grouper(key='id'))['wheel_mass'].mean()).mean(), 2) or 0
    self.mean_equivalent_axle_mass_negative_direction = round(
        (df3.loc[df3['direction'] == 'N'].groupby(pd.Grouper(key='id'))['wheel_mass'].mean()).mean(), 2) or 0
    self.mean_axle_spacing = round((df2.groupby(pd.Grouper(key='id')).mean()).mean()[
                                    'axle_spacing_number'], 0) or 0
    self.mean_axle_spacing_positive_direction = round((df2.loc[df2['direction'] == 'P'].groupby(
        pd.Grouper(key='id')).mean()).mean()['axle_spacing_number'], 0) or 0
    self.mean_axle_spacing_negative_direction = round((df2.loc[df2['direction'] == 'N'].groupby(
        pd.Grouper(key='id')).mean()).mean()['axle_spacing_number'], 0) or 0

    self.e80_per_axle = ((df3['wheel_mass']/8200)**4.2).sum() or 0

    self.e80_per_axle_positive_direction = (
        (df3.loc[df3['direction'] == 'P']['wheel_mass']/8200)**4.2).sum() or 0

    self.e80_per_axle_negative_direction = (
        (df3.loc[df3['direction'] == 'N']['wheel_mass']/8200)**4.2).sum() or 0

    self.olhv = len(
        df3.loc[df3['gross_mass'] > df3['vehicle_mass_limit_kg']]['id'].unique()) or 0
    self.olhv_positive_direction = len(df3.loc[(df3['gross_mass'] > df3['vehicle_mass_limit_kg']) & (
        df3['direction'] == 'P')]['id'].unique()) or 0
    self.olhv_negative_direction = len(df3.loc[(df3['gross_mass'] > df3['vehicle_mass_limit_kg']) & (
        df3['direction'] == 'N')]['id'].unique()) or 0
    try:
        self.olhv_percent = (
            (len(df3.loc[df3['gross_mass'] > df3['vehicle_mass_limit_kg']]['id'].unique())/len(df3['id'].unique())))
    except ZeroDivisionError:
        self.olhv_percent = 0
    try:
        self.olhv_percent_positive_direction = ((len(df3.loc[(df3['gross_mass'] > df3['vehicle_mass_limit_kg']) & (
            df3['direction'] == 'P')]['id'].unique())/len(df3.loc[df3['direction'] == 'P']['id'].unique())))
    except ZeroDivisionError:
        self.olhv_percent_positive_direction = 0
    try:
        self.olhv_percent_negative_direction = ((len(df3.loc[(df3['gross_mass'] > df3['vehicle_mass_limit_kg']) & (
            df3['direction'] == 'N')]['id'].unique())/len(df3.loc[df3['direction'] == 'N']['id'].unique())))
    except ZeroDivisionError:
        self.olhv_percent_negative_direction = 0
    self.tonnage_generated = (
        (df3['wheel_mass']).sum()/1000).round().astype(int) or 0
    self.tonnage_generated_positive_direction = (
        (df3.loc[df3['direction'] == 'P']['wheel_mass']).sum()/1000).round().astype(int) or 0
    self.tonnage_generated_negative_direction = (
        (df3.loc[df3['direction'] == 'N']['wheel_mass']).sum()/1000).round().astype(int) or 0
    self.olton = (df3.loc[(df3['gross_mass'] > df3['vehicle_mass_limit_kg'])]
                    ['wheel_mass']).sum().astype(int) or 0
    self.olton_positive_direction = (df3.loc[(df3['gross_mass'] > df3['vehicle_mass_limit_kg']) & (
        df3['direction'] == 'P')]['wheel_mass']).sum().astype(int) or 0
    self.olton_negative_direction = (df3.loc[(df3['gross_mass'] > df3['vehicle_mass_limit_kg']) & (
        df3['direction'] == 'N')]['wheel_mass']).sum().astype(int) or 0
    try:
        self.olton_percent = ((df3.loc[(df3['gross_mass'] > df3['vehicle_mass_limit_kg'])]
                                ['wheel_mass']).sum().round(4)/(df3['wheel_mass']).sum().round(4))
    except ZeroDivisionError:
        self.olton_percent = 0
    try:
        self.olton_percent_positive_direction = ((df3.loc[(df3['gross_mass'] > df3['vehicle_mass_limit_kg']) & (
            df3['direction'] == 'P')]['wheel_mass']).sum().round(4)/(df3.loc[df3['direction'] == 'P']['wheel_mass']).sum().round(4))
    except ZeroDivisionError:
        self.olton_percent_positive_direction = 0
    try:
        self.olton_percent_negative_direction = ((df3.loc[(df3['gross_mass'] > df3['vehicle_mass_limit_kg']) & (
            df3['direction'] == 'N')]['wheel_mass']).sum().round(4)/(df3.loc[df3['direction'] == 'N']['wheel_mass']).sum().round(4))
    except ZeroDivisionError:
        self.olton_percent_negative_direction = 0
    self.ole80 = round(
        ((df3.loc[(df3['gross_mass'] > df3['vehicle_mass_limit_kg'])]['wheel_mass']/8200)**4.2).sum(), 0) or 0
    self.ole80_positive_direction = round(((df3.loc[(df3['gross_mass'] > df3['vehicle_mass_limit_kg']) & (
        df3['direction'] == 'P')]['wheel_mass']/8200)**4.2).sum(), 0) or 0
    self.ole80_negative_direction = round(((df3.loc[(df3['gross_mass'] > df3['vehicle_mass_limit_kg']) & (
        df3['direction'] == 'N')]['wheel_mass']/8200)**4.2).sum(), 0) or 0

    try:
        self.ole80_percent = ((df3.loc[(df3['gross_mass'] > df3['vehicle_mass_limit_kg'])]['wheel_mass']/8200)**4.2).sum(
        )/((df3['wheel_mass']/8200)**4.2).sum()
    except ZeroDivisionError:
        self.ole80_percent = 0

    try:
        self.ole80_percent_positive_direction = ((((df3.loc[(df3['gross_mass'] > df3['vehicle_mass_limit_kg']) & (
            df3['direction'] == 'P')]['wheel_mass']/8200)**4.2).sum().round(4)/((df3.loc[df3['direction'] == 'P']['wheel_mass']/8200)**4.2).sum().round())).round(4)
    except ZeroDivisionError:
        self.ole80_percent_positive_direction = 0

    try:
        self.ole80_percent_negative_direction = ((((df3.loc[(df3['gross_mass'] > df3['vehicle_mass_limit_kg']) & (
            df3['direction'] == 'N')]['wheel_mass']/8200)**4.2).sum().round(4)/((df3.loc[df3['direction'] == 'N']['wheel_mass']/8200)**4.2).sum().round())).round(4)
    except ZeroDivisionError:
        self.ole80_percent_negative_direction = 0

    self.xe80 = round(((df3['wheel_mass']/8200)**4.2).sum() -
                        (((df3['wheel_mass']-(9000*0.05))/8200)**4.2).sum())

    self.xe80_positive_direction = round(((df3.loc[df3['direction'] == 'P']['wheel_mass']/8200)**4.2).sum()-(
        ((df3.loc[df3['direction'] == 'P']['wheel_mass']-(9000*0.05))/8200)**4.2).sum()) or 0

    self.xe80_negative_direction = round(((df3.loc[df3['direction'] == 'N']['wheel_mass']/8200)**4.2).sum()-(
        ((df3.loc[df3['direction'] == 'N']['wheel_mass']-(9000*0.05))/8200)**4.2).sum()) or 0

    try:
        self.xe80_percent = ((((df3['wheel_mass']/8200)**4.2).sum()-(
            ((df3['wheel_mass'])*(9000*0.05)/8200)**4.2).sum())/((df3['wheel_mass']/8200)**4.2).sum()).round(4)
    except ZeroDivisionError:
        self.xe80_percent = 0

    try:
        self.xe80_percent_positive_direction = (((((df3.loc[df3['direction'] == 'P']['wheel_mass']/8200)**4.2).sum()-(
            ((df3.loc[df3['direction'] == 'P']['wheel_mass'])*(9000*0.05)/8200)**4.2).sum())/((df3.loc[df3['direction'] == 'P']['wheel_mass']/8200)**4.2).sum())).round(4)
    except ZeroDivisionError:
        self.xe80_percent_positive_direction = 0

    try:
        self.xe80_percent_negative_direction = (((((df3.loc[df3['direction'] == 'N']['wheel_mass']/8200)**4.2).sum()-(
            ((df3.loc[df3['direction'] == 'N']['wheel_mass'])*(9000*0.05)/8200)**4.2).sum())/((df3.loc[df3['direction'] == 'N']['wheel_mass']/8200)**4.2).sum())).round(4)
    except ZeroDivisionError:
        self.xe80_percent_negative_direction = 0

    try:
        self.e80_per_day = ((((df3['wheel_mass']/8200)**4.2).sum().round()/df3.groupby(
            pd.Grouper(key='start_datetime', freq='D')).count().count()[0])).round(4)
    except ZeroDivisionError:
        self.e80_per_day = 0

    try:
        self.e80_per_day_positive_direction = ((((df3.loc[df3['direction'] == 'P']['wheel_mass']/8200)**4.2).sum().round(
        )/df3.loc[df3['direction'] == 'P'].groupby(pd.Grouper(key='start_datetime', freq='D')).count().count()[0])).round(4)
    except ZeroDivisionError:
        self.e80_per_day_positive_direction = 0
    try:
        self.e80_per_day_negative_direction = ((((df3.loc[df3['direction'] == 'N']['wheel_mass']/8200)**4.2).sum().round(
        )/df3.loc[df3['direction'] == 'N'].groupby(pd.Grouper(key='start_datetime', freq='D')).count().count()[0])).round(4)
    except ZeroDivisionError:
        self.e80_per_day_negative_direction = 0
    try:
        self.e80_per_heavy_vehicle = ((((df3.loc[df3['vehicle_class_code_primary_scheme'] > 3]['wheel_mass']/8200)**4.2).sum(
        ).round()/df3.loc[df3['vehicle_class_code_primary_scheme'] > 3].count()[0])).round(4)
    except ZeroDivisionError:
        self.e80_per_heavy_vehicle = 0
    try:
        self.e80_per_heavy_vehicle_positive_direction = ((((df3.loc[(df3['vehicle_class_code_primary_scheme'] > 3) & (
            df3['direction'] == 'P')]['wheel_mass']/8200)**4.2).sum().round()/df3.loc[(df3['vehicle_class_code_primary_scheme'] > 3) & (df3['direction'] == 'P')].count()[0])).round(4)
    except ZeroDivisionError:
        self.e80_per_heavy_vehicle_positive_direction = 0
    try:
        self.e80_per_heavy_vehicle_negative_direction = ((((df3.loc[(df3['vehicle_class_code_primary_scheme'] > 3) & (
            df3['direction'] == 'N')]['wheel_mass']/8200)**4.2).sum().round()/df3.loc[(df3['vehicle_class_code_primary_scheme'] > 3) & (df3['direction'] == 'N')].count()[0])).round(4)
    except ZeroDivisionError:
        self.e80_per_heavy_vehicle_negative_direction = 0
        
    # the below calculates the number of axle groups
    try:
        nr_axle_groups = df2.loc[df2.axle_spacing_cm <= 220].groupby(df2.id)['id'].count()
        nr_axle_groups.name = 'nr_axle_groups'
        df2 = df2.drop(columns=['nr_axle_groups'])
        df2 = df2.join(nr_axle_groups, on='id')
    except:
        pass

    
    # The below gets the steering single axle vehicles.
    mask = df2['id'].isin(df2.loc[(df2['axle_spacing_number'] == 1) & (df2['axle_spacing_cm'] > 220), 'id'].tolist())
    steering_single_axles = df2.loc[mask].copy()
    steering_single_axles_per_dir = steering_single_axles.loc[steering_single_axles.wheel_mass_number <= 2].groupby(['id','direction']).sum()
    steering_single_axles_per_dir = steering_single_axles_per_dir.reset_index()
    steering_single_axles = steering_single_axles.loc[steering_single_axles.wheel_mass_number <= 2].groupby('id').sum()

    self.worst_steering_single_axle_cnt = steering_single_axles.loc[steering_single_axles.wheel_mass > 7500].count()[0]
    self.worst_steering_single_axle_olhv_perc = steering_single_axles.loc[steering_single_axles.wheel_mass > 7500].count()[0] / steering_single_axles.count()[0]
    self.worst_steering_single_axle_tonperhv = (steering_single_axles.loc[steering_single_axles.wheel_mass > 7500, 'wheel_mass'].sum() / steering_single_axles.loc[steering_single_axles.wheel_mass > 7500].count()[0])/1000
    try:
        self.worst_steering_single_axle_cnt_positive_direciton = steering_single_axles_per_dir.loc[(steering_single_axles_per_dir['direction'] == 'P') & (steering_single_axles_per_dir.wheel_mass > 7500)].count()[0]
    except:
        self.worst_steering_single_axle_cnt_positive_direciton = 0
    try:
        self.worst_steering_single_axle_olhv_perc_positive_direciton = steering_single_axles_per_dir.loc[(steering_single_axles_per_dir['direction'] == 'P') & (steering_single_axles_per_dir.wheel_mass > 7500)].count()[0] / steering_single_axles_per_dir.loc[(steering_single_axles_per_dir['direction'] == 'P')].count()[0]
    except:
        self.worst_steering_single_axle_olhv_perc_positive_direciton = 0
    try:
        self.worst_steering_single_axle_tonperhv_positive_direciton = steering_single_axles_per_dir.loc[(steering_single_axles_per_dir['direction'] == 'P') & (steering_single_axles_per_dir.wheel_mass > 7500),'wheel_mass'].sum() / steering_single_axles_per_dir.loc[(steering_single_axles_per_dir['direction'] == 'P') & (steering_single_axles_per_dir.wheel_mass > 7500)].count()[0]
    except:
        self.worst_steering_single_axle_tonperhv_positive_direciton = 0
    try:
        self.worst_steering_single_axle_cnt_negative_direciton = steering_single_axles_per_dir.loc[(steering_single_axles_per_dir['direction'] == 'N') & (steering_single_axles_per_dir.wheel_mass > 7500)].count()[0]
    except:
        self.worst_steering_single_axle_cnt_negative_direciton = 0
    try:
        self.worst_steering_single_axle_olhv_perc_negative_direciton = steering_single_axles_per_dir.loc[(steering_single_axles_per_dir['direction'] == 'N') & (steering_single_axles_per_dir.wheel_mass > 7500)].count()[0] / steering_single_axles_per_dir.loc[(steering_single_axles_per_dir['direction'] == 'N')].count()[0]
    except:
        self.worst_steering_single_axle_olhv_perc_negative_direciton = 0
    try:
        self.worst_steering_single_axle_tonperhv_negative_direciton = steering_single_axles_per_dir.loc[(steering_single_axles_per_dir['direction'] == 'N') & (steering_single_axles_per_dir.wheel_mass > 7500),'wheel_mass'].sum() / steering_single_axles_per_dir.loc[(steering_single_axles_per_dir['direction'] == 'N') & (steering_single_axles_per_dir.wheel_mass > 7500)].count()[0]
    except:
        self.worst_steering_single_axle_tonperhv_negative_direciton = 0

    
    # The below gets the steering double axle vehicles.
    mask = df2['id'].isin(df2.loc[(df2['axle_spacing_number'] == 1) & (df2['axle_spacing_cm'] <= 220), 'id'].tolist())
    steering_double_axles = df2.loc[mask].copy()
    steering_double_axles_per_dir = steering_double_axles.loc[steering_double_axles.wheel_mass_number <= 2].groupby(['id','direction']).sum()
    steering_double_axles_per_dir = steering_double_axles_per_dir.reset_index()
    steering_double_axles = steering_double_axles.loc[steering_double_axles.wheel_mass_number <= 2].groupby('id').sum()

    self.worst_steering_double_axle_cnt = steering_double_axles.loc[steering_double_axles.wheel_mass > 15000].count()[0]
    self.worst_steering_double_axle_olhv_perc = steering_double_axles.loc[steering_double_axles.wheel_mass > 15000].count()[0] / steering_double_axles.count()[0]
    self.worst_steering_double_axle_tonperhv = (steering_double_axles.loc[steering_double_axles.wheel_mass > 15000, 'wheel_mass'].sum() / steering_double_axles.loc[steering_double_axles.wheel_mass > 15000].count()[0])/1000
    try:
        self.worst_steering_double_axle_cnt_positive_direciton = steering_double_axles_per_dir.loc[(steering_double_axles_per_dir['direction'] == 'P') & (steering_double_axles.wheel_mass > 15000)].count()[0]
    except:
        self.worst_steering_double_axle_cnt_positive_direciton = 0 
    try:
        self.worst_steering_double_axle_olhv_perc_positive_direciton = steering_double_axles_per_dir.loc[(steering_double_axles_per_dir['direction'] == 'P') & (steering_double_axles.wheel_mass > 15000)].count()[0] / steering_double_axles_per_dir.loc[(steering_double_axles_per_dir['direction'] == 'P')].count()[0]
    except:
        self.worst_steering_double_axle_olhv_perc_positive_direciton = 0 
    try:
        self.worst_steering_double_axle_tonperhv_positive_direciton = (steering_double_axles_per_dir.loc[(steering_double_axles_per_dir['direction'] == 'P') & (steering_double_axles.wheel_mass > 15000), 'wheel_mass'].sum() / steering_double_axles_per_dir.loc[(steering_double_axles_per_dir['direction'] == 'P') & (steering_double_axles_per_dir.wheel_mass > 15000)].count()[0])/1000
    except:
        self.worst_steering_double_axle_tonperhv_positive_direciton = 0 
    try:
        self.worst_steering_double_axle_cnt_negative_direciton = steering_double_axles_per_dir.loc[(steering_double_axles_per_dir['direction'] == 'N') & (steering_double_axles_per_dir.wheel_mass > 15000)].count()[0]
    except:
        self.worst_steering_double_axle_cnt_negative_direciton = 0 
    try:
        self.worst_steering_double_axle_olhv_perc_negative_direciton = steering_double_axles_per_dir.loc[(steering_double_axles_per_dir['direction'] == 'N') & (steering_double_axles_per_dir.wheel_mass > 15000)].count()[0] / steering_double_axles_per_dir.loc[(steering_double_axles_per_dir['direction'] == 'N')].count()[0]
    except:
        self.worst_steering_double_axle_olhv_perc_negative_direciton = 0 
    try:
        self.worst_steering_double_axle_tonperhv_negative_direciton = (steering_double_axles_per_dir.loc[(steering_double_axles_per_dir['direction'] == 'N') & (steering_double_axles_per_dir.wheel_mass > 15000), 'wheel_mass'].sum() / steering_double_axles_per_dir.loc[(steering_double_axles_per_dir['direction'] == 'N') & (steering_double_axles_per_dir.wheel_mass > 15000)].count()[0])/1000
    except:
        self.worst_steering_double_axle_tonperhv_negative_direciton = 0 
    

    # The below gets the non-steering single axle vehicles.
    mask = df2['id'].isin(df2.loc[(df2['axle_spacing_number'] == 1) & (df2['axle_spacing_cm'] > 220) & (df2['vehicle_class_code_primary_scheme'].isin([4,5,7])), 'id'].tolist())
    non_steering_single_axles = df2.loc[mask].copy()
    non_steering_single_axles_per_dir = non_steering_single_axles.loc[non_steering_single_axles.wheel_mass_number > 1].groupby(['id','direction']).sum()
    non_steering_single_axles_per_dir = non_steering_single_axles_per_dir.reset_index()
    non_steering_single_axles = non_steering_single_axles.loc[non_steering_single_axles.wheel_mass_number > 1].groupby('id').sum()

    self.worst_non_steering_single_axle_cnt = non_steering_single_axles.loc[non_steering_single_axles.wheel_mass > 9000].count()[0]
    self.worst_non_steering_single_axle_olhv_perc = non_steering_single_axles.loc[non_steering_single_axles.wheel_mass > 9000].count()[0] / non_steering_single_axles.count()[0]
    self.worst_non_steering_single_axle_tonperhv = (non_steering_single_axles.loc[non_steering_single_axles.wheel_mass > 9000, 'wheel_mass'].sum() / non_steering_single_axles.loc[non_steering_single_axles.wheel_mass > 9000].count()[0])/1000
    try:
        self.worst_non_steering_single_axle_cnt_positive_direciton = non_steering_single_axles_per_dir.loc[(non_steering_single_axles_per_dir['direction'] == 'P') & (non_steering_single_axles_per_dir.wheel_mass > 9000)].count()[0]
    except:
        self.worst_non_steering_single_axle_cnt_positive_direciton = 0
    try:
        self.worst_non_steering_single_axle_olhv_perc_positive_direciton = non_steering_single_axles_per_dir.loc[(non_steering_single_axles_per_dir['direction'] == 'P') & (non_steering_single_axles_per_dir.wheel_mass > 9000)].count()[0] / non_steering_single_axles_per_dir.loc[(non_steering_single_axles_per_dir['direction'] == 'P')].count()[0]
    except:
        self.worst_non_steering_single_axle_olhv_perc_positive_direciton = 0
    try:
        self.worst_non_steering_single_axle_tonperhv_positive_direciton = (non_steering_single_axles_per_dir.loc[(non_steering_single_axles_per_dir['direction'] == 'P') & (non_steering_single_axles_per_dir.wheel_mass > 9000), 'wheel_mass'].sum() / non_steering_single_axles_per_dir.loc[(non_steering_single_axles_per_dir['direction'] == 'P') & (non_steering_single_axles_per_dir.wheel_mass > 9000)].count()[0])/1000
    except:
        self.worst_non_steering_single_axle_tonperhv_positive_direciton = 0
    try:
        self.worst_non_steering_single_axle_cnt_negative_direciton = non_steering_single_axles_per_dir.loc[(non_steering_single_axles_per_dir['direction'] == 'N') & (non_steering_single_axles_per_dir.wheel_mass > 9000)].count()[0]
    except:
        self.worst_non_steering_single_axle_cnt_negative_direciton = 0
    try:
        self.worst_non_steering_single_axle_olhv_perc_negative_direciton = non_steering_single_axles_per_dir.loc[(non_steering_single_axles_per_dir['direction'] == 'N') & (non_steering_single_axles_per_dir.wheel_mass > 9000)].count()[0] / non_steering_single_axles_per_dir.loc[(non_steering_single_axles_per_dir['direction'] == 'N')].count()[0]
    except:
        self.worst_non_steering_single_axle_olhv_perc_negative_direciton = 0
    try:
        self.worst_non_steering_single_axle_tonperhv_negative_direciton = (non_steering_single_axles_per_dir.loc[(non_steering_single_axles_per_dir['direction'] == 'N') & (non_steering_single_axles_per_dir.wheel_mass > 9000), 'wheel_mass'].sum() / non_steering_single_axles_per_dir.loc[(non_steering_single_axles_per_dir['direction'] == 'N') & (non_steering_single_axles_per_dir.wheel_mass > 9000)].count()[0])/1000
    except:
        self.worst_non_steering_single_axle_tonperhv_negative_direciton = 0

    # The below gets the non-steering double axle vehicles.
    # mask = df2['id'].isin(df2.loc[(df2['axle_spacing_number'] == 1) & (df2['axle_spacing_cm'] > 220) & (df2['vehicle_class_code_primary_scheme'].isin([6,8,9,10,11,12,13,14,15,16,17])), 'id'].tolist())
    # non_steering_double_axles = df2.loc[mask].copy()
    # non_steering_double_axles_per_dir = non_steering_double_axles.loc[non_steering_double_axles.wheel_mass_number > 1].groupby(['id','direction']).sum()
    # non_steering_double_axles = non_steering_double_axles.loc[non_steering_double_axles.wheel_mass_number > 1].groupby('id').sum()
    
    # TODO:
    self.worst_non_steering_double_axle_cnt = 0
    self.worst_non_steering_double_axle_olhv_perc = 0
    self.worst_non_steering_double_axle_tonperhv = 0
    self.worst_non_steering_double_axle_cnt_positive_direciton = 0
    self.worst_non_steering_double_axle_olhv_perc_positive_direciton = 0
    self.worst_non_steering_double_axle_tonperhv_positive_direciton = 0
    self.worst_non_steering_double_axle_cnt_negative_direciton = 0
    self.worst_non_steering_double_axle_olhv_perc_negative_direciton = 0
    self.worst_non_steering_double_axle_tonperhv_negative_direciton = 0
    
    # The below gets the triple axle vehicles.
    # mask = df2['id'].isin(df2.loc[(df2['axle_spacing_number'] > 1) & (df2['axle_spacing_cm'] <= 220), 'id'].tolist())
    # triple_axles = df2.loc[mask].copy()
    # # triple_axles['check'] = triple_axles['axle_spacing_number'].diff().eq(1).any(axis=1).astype(int)
    # triple_axles_per_dir = triple_axles.loc[triple_axles.wheel_mass_number <= 2].groupby(['id','direction']).sum()
    # triple_axles = triple_axles.loc[triple_axles.wheel_mass_number <= 2].groupby('id').sum()

    # TODO:
    self.worst_triple_axle_cnt = 0
    self.worst_triple_axle_olhv_perc = 0
    self.worst_triple_axle_tonperhv = 0
    self.worst_triple_axle_cnt_positive_direciton = 0
    self.worst_triple_axle_olhv_perc_positive_direciton = 0
    self.worst_triple_axle_tonperhv_positive_direciton = 0
    self.worst_triple_axle_cnt_negative_direciton = 0
    self.worst_triple_axle_olhv_perc_negative_direciton = 0
    self.worst_triple_axle_tonperhv_negative_direciton = 0

    self.bridge_formula_cnt = round(
        (18000 + 2.1 * (df2.loc[df2['axle_spacing_number'] > 1].groupby('id')['axle_spacing_cm'].sum().mean())), 2) or 0
    self.bridge_formula_olhv_perc = 0
    self.bridge_formula_tonperhv = 0
    self.bridge_formula_cnt_positive_direciton = round((18000 + 2.1 * (df2.loc[(df2['axle_spacing_number'] > 1) & (
        df2['direction'] == 'P')].groupby('id')['axle_spacing_cm'].sum().mean())), 2) or 0
    self.bridge_formula_olhv_perc_positive_direciton = 0
    self.bridge_formula_tonperhv_positive_direciton = 0
    self.bridge_formula_cnt_negative_direciton = round((18000 + 2.1 * (df2.loc[(df2['axle_spacing_number'] > 1) & (
        df2['direction'] == 'P')].groupby('id')['axle_spacing_cm'].sum().mean())), 2) or 0
    self.bridge_formula_olhv_perc_negative_direciton = 0
    self.bridge_formula_tonperhv_negative_direciton = 0

    # TODO:
    self.gross_formula_cnt = 0
    self.gross_formula_olhv_perc = 0
    self.gross_formula_tonperhv = 0
    self.gross_formula_cnt_positive_direciton = 0
    self.gross_formula_olhv_perc_positive_direciton = 0
    self.gross_formula_tonperhv_positive_direciton = 0
    self.gross_formula_cnt_negative_direciton = 0
    self.gross_formula_olhv_perc_negative_direciton = 0
    self.gross_formula_tonperhv_negative_direciton = 0

    self.total_avg_cnt = df.loc[df['group'] == 'Heavy'].count()[0]
    self.total_avg_olhv_perc = 0
    self.total_avg_tonperhv = round(
        ((df3['wheel_mass']).sum()/1000)/df.loc[df['group'] == 'Heavy'].count()[0], 2)
    self.total_avg_cnt_positive_direciton = df.loc[(
        df['group'] == 'Heavy') & (df['direction'] == 'P')].count()[0]
    self.total_avg_olhv_perc_positive_direciton = 0
    self.total_avg_tonperhv_positive_direciton = round(
        ((df3.loc[df3['direction'] == 'P']['wheel_mass']).sum()/1000)/df.loc[df['group'] == 'Heavy'].count()[0], 2)
    self.total_avg_cnt_negative_direciton = df.loc[(
        df['group'] == 'Heavy') & (df['direction'] == 'N')].count()[0]
    self.total_avg_olhv_perc_negative_direciton = 0
    self.total_avg_tonperhv_negative_direciton = round(
        ((df3.loc[df3['direction'] == 'N']['wheel_mass']).sum()/1000)/df.loc[df['group'] == 'Heavy'].count()[0], 2)

    # FIXME:
    for i in range(4,18):
        try:
            setattr(self,f"total_vehicles_cls_{str(i)+'tot'}",df2.loc[(df2['vehicle_class_code_primary_scheme'] == i)].groupby('id').count().count()[0])
        except KeyError:
            setattr(self,f"total_vehicles_cls_{str(i)+'tot'}",0)
        
        try:
            setattr(self,f"weighed_vehicles_cls_{str(i)+'tot'}",df2.loc[(df2['vehicle_class_code_primary_scheme'] == i)].groupby('id').count().count()[0])
        except KeyError:
            setattr(self,f"weighed_vehicles_cls_{str(i)+'tot'}",0)

        try:
            setattr(self,f"perc_truck_dist_cls_{str(i)+'tot'}",(df2.loc[(df2['vehicle_class_code_primary_scheme'] == i)].groupby('id').count().count()[0]
            / df2.loc[(df2['vehicle_class_code_primary_scheme'] >= 4)].groupby('id').count().count()[0]))
        except KeyError:
            setattr(self,f"perc_truck_dist_cls_{str(i)+'tot'}",0)
        
        try:
            setattr(self,f"total_axles_cls_{str(i)+'tot'}",df2.loc[(df2['vehicle_class_code_primary_scheme'] == i)].groupby('id').count().sum()[0])
        except KeyError:
            setattr(self,f"total_axles_cls_{str(i)+'tot'}",0)
        
        try:
            setattr(self,f"axles_over9t_cls_{str(i)+'tot'}",df2.loc[(df2['vehicle_class_code_primary_scheme'] == i) & (df2['wheel_mass'] > 9000)].groupby('id').count().count()[0])
        except KeyError:
            setattr(self,f"axles_over9t_cls_{str(i)+'tot'}",0)
        
        try:
            setattr(self,f"total_mass_cls_{str(i)+'tot'}",df2.loc[(df2['vehicle_class_code_primary_scheme'] == i)].groupby('id')['wheel_mass'].sum()[0])
        except KeyError:
            setattr(self,f"total_mass_cls_{str(i)+'tot'}" ,0)                
        try:
            setattr(self,f"cnt_mass_over9t_cls_{str(i)+'tot'}",round(df2.loc[(df2['vehicle_class_code_primary_scheme'] == i),'wheel_mass'].sum()/1000))
        except KeyError:
            setattr(self,f"cnt_mass_over9t_cls_{str(i)+'tot'}",0)
        
        try:
            setattr(self,f"eal_pervehicle_cls_{str(i)+'tot'}",(round(df2.loc[(df2['vehicle_class_code_primary_scheme'] == i),'wheel_mass'].sum()/1000)/
            df2.loc[(df2['vehicle_class_code_primary_scheme'] == i)].groupby('id').count().sum()[0]))
        except KeyError:
            setattr(self,f"eal_pervehicle_cls_{str(i)+'tot'}",0)
        
        try:
            setattr(self,f"total_eal_cls_{str(i)+'tot'}",((round(df2.loc[(df2['vehicle_class_code_primary_scheme'] == i),'wheel_mass'].sum()/1000)/
            df2.loc[(df2['vehicle_class_code_primary_scheme'] == i)].groupby('id').count().sum()[0])
            *df2.loc[(df2['vehicle_class_code_primary_scheme'] == i)].groupby('id').count().count()[0]))
        except KeyError:
            setattr(self,f"total_eal_cls_{str(i)+'tot'}",0)
        
        try:
            setattr(self,f"total_eal_over9t_cls_{str(i)+'tot'}",0)
        except KeyError:
            setattr(self,f"total_eal_over9t_cls_{str(i)+'tot'}",0)

        for dir in ['P','N']:
            try:
                setattr(self,f"total_vehicles_cls_{str(i)+dir.lower()}",df2.loc[(df2['vehicle_class_code_primary_scheme'] == i) & (df2['direction'] == dir)].groupby('id').count().count()[0])
            except KeyError:
                setattr(self,f"total_vehicles_cls_{str(i)+dir.lower()}",0)
            
            try:
                setattr(self,f"weighed_vehicles_cls_{str(i)+dir.lower()}",df2.loc[(df2['vehicle_class_code_primary_scheme'] == i) & (df2['direction'] == dir)].groupby('id').count().count()[0])
            except KeyError:
                setattr(self,f"weighed_vehicles_cls_{str(i)+dir.lower()}",0)

            try:
                setattr(self,f"perc_truck_dist_cls_{str(i)+dir.lower()}",(df2.loc[(df2['vehicle_class_code_primary_scheme'] == i) & (df2['direction'] == dir)].groupby('id').count().count()[0]
                / df2.loc[(df2['vehicle_class_code_primary_scheme'] >= 4) & (df2['direction'] == dir)].groupby('id').count().count()[0]))
            except KeyError:
                setattr(self,f"perc_truck_dist_cls_{str(i)+dir.lower()}",0)
            
            try:
                setattr(self,f"total_axles_cls_{str(i)+dir.lower()}",df2.loc[(df2['vehicle_class_code_primary_scheme'] == i) & (df2['direction'] == dir)].groupby('id').count().sum()[0])
            except KeyError:
                setattr(self,f"total_axles_cls_{str(i)+dir.lower()}",0)
            
            try:
                setattr(self,f"axles_over9t_cls_{str(i)+dir.lower()}",df2.loc[(df2['vehicle_class_code_primary_scheme'] == i) & (df2['direction'] == dir) & (df2['wheel_mass'] > 9000)].groupby('id').count().count()[0])
            except KeyError:
                setattr(self,f"axles_over9t_cls_{str(i)+dir.lower()}",0)
            
            try:
                setattr(self,f"total_mass_cls_{str(i)+dir.lower()}",df2.loc[(df2['vehicle_class_code_primary_scheme'] == i) & (df2['direction'] == dir)].groupby('id')['wheel_mass'].sum()[0])
            except KeyError:
                setattr(self,f"total_mass_cls_{str(i)+dir.lower()}" ,0)                
            try:
                setattr(self,f"cnt_mass_over9t_cls_{str(i)+dir.lower()}",round(df2.loc[(df2['vehicle_class_code_primary_scheme'] == i) & (df2['direction'] == dir),'wheel_mass'].sum()/1000))
            except KeyError:
                setattr(self,f"cnt_mass_over9t_cls_{str(i)+dir.lower()}",0)
            
            try:
                setattr(self,f"eal_pervehicle_cls_{str(i)+dir.lower()}",(round(df2.loc[(df2['vehicle_class_code_primary_scheme'] == i) & (df2['direction'] == dir),'wheel_mass'].sum()/1000)/
                df2.loc[(df2['vehicle_class_code_primary_scheme'] == i) & (df2['direction'] == dir)].groupby('id').count().sum()[0]))
            except KeyError:
                setattr(self,f"eal_pervehicle_cls_{str(i)+dir.lower()}",0)
            
            try:
                setattr(self,f"total_eal_cls_{str(i)+dir.lower()}",((round(df2.loc[(df2['vehicle_class_code_primary_scheme'] == i) & (df2['direction'] == dir),'wheel_mass'].sum()/1000)/
                df2.loc[(df2['vehicle_class_code_primary_scheme'] == i) & (df2['direction'] == dir)].groupby('id').count().sum()[0])
                *df2.loc[(df2['vehicle_class_code_primary_scheme'] == i) & (df2['direction'] == dir)].groupby('id').count().count()[0]))
            except KeyError:
                setattr(self,f"total_eal_cls_{str(i)+dir.lower()}",0)
            
            try:
                setattr(self,f"total_eal_over9t_cls_{str(i)+dir.lower()}",0)
            except KeyError:
                setattr(self,f"total_eal_over9t_cls_{str(i)+dir.lower()}",0)

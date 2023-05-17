import config
import pandas as pd


def wim_header_upsert_func1(self, header_id: str) -> str:
    """
    It returns a tuple of three strings, each of which is a SQL query.

    :param header_id: str = '1'
    :type header_id: str
    :return: a tuple of 3 strings.
    """
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
        axs.axle_spacing_cm,
        wm.wheel_mass_number,
        wm.wheel_mass,
        vm.kg as vehicle_mass_limit_kg,
        sum(wm.wheel_mass) over(partition by t10.id) as gross_mass
        FROM trafc.electronic_count_data_type_10 t10
        left join trafc.traffic_e_type10_wheel_mass wm ON wm.type10_id = t10.data_id
        left join trafc.traffic_e_type10_axle_spacing axs ON axs.type10_id = t10.data_id and axs.axle_spacing_number = wm.wheel_mass_number
        Left join traf_lu.gross_vehicle_mass_limits vm on vm.number_of_axles = t10.axle_count
        where t10.header_id = '{header_id}' and wm.wheel_mass_number is not null
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
        sum(wm.wheel_mass) over(partition by t10.id) as gross_mass
        FROM trafc.electronic_count_data_type_10 t10
        inner join trafc.traffic_e_type10_wheel_mass wm ON wm.type10_id = t10.data_id
        inner join traf_lu.gross_vehicle_mass_limits vm on vm.number_of_axles = t10.axle_count
        where t10.header_id = '{header_id}'
        """
    return SELECT_TYPE10_QRY, AXLE_SPACING_SELECT_QRY, WHEEL_MASS_SELECT_QRY


def wim_header_upsert_func2(
    self,
    SELECT_TYPE10_QRY: str,
    AXLE_SPACING_SELECT_QRY: str,
    WHEEL_MASS_SELECT_QRY: str,
) -> pd.DataFrame:
    """
    This function takes in three SQL queries and returns three dataframes

    :param SELECT_TYPE10_QRY: str = "SELECT * FROM trafc.electronic_count_data_type_10 t10 limit 1"
    :type SELECT_TYPE10_QRY: str
    :param AXLE_SPACING_SELECT_QRY: str = "SELECT * FROM axle_spacinglimit 1"
    :type AXLE_SPACING_SELECT_QRY: str
    :param WHEEL_MASS_SELECT_QRY: str = "select * from wheel_masslimit 1"
    :type WHEEL_MASS_SELECT_QRY: str
    :return: A tuple of 3 dataframes
    """
    df = pd.read_sql_query(SELECT_TYPE10_QRY, config.ENGINE)
    df = df.fillna(0)
    df2 = pd.read_sql_query(AXLE_SPACING_SELECT_QRY, config.ENGINE)
    df2 = df2.fillna(0)
    df3 = pd.read_sql_query(WHEEL_MASS_SELECT_QRY, config.ENGINE)
    df3 = df3.fillna(0)
    return df, df2, df3


def wim_header_upsert(self, header_id) -> str:
    UPSERT_STRING = f"""INSERT INTO trafc.electronic_count_header_hswim (
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
        total_vehicles_cls_4tot,
        weighed_vehicles_cls_4tot,
        perc_truck_dist_cls_4tot,
        total_axles_cls_4tot,
        axles_over9t_cls_4tot,
        total_mass_cls_4tot,
        cnt_mass_over9t_cls_4tot,
        eal_pervehicle_cls_4tot,
        total_eal_cls_4tot,
        total_eal_over9t_cls_4tot,
        total_vehicles_cls_5tot,
        weighed_vehicles_cls_5tot,
        perc_truck_dist_cls_5tot,
        total_axles_cls_5tot,
        axles_over9t_cls_5tot,
        total_mass_cls_5tot,
        cnt_mass_over9t_cls_5tot,
        eal_pervehicle_cls_5tot,
        total_eal_cls_5tot,
        total_eal_over9t_cls_5tot,
        total_vehicles_cls_6tot,
        weighed_vehicles_cls_6tot,
        perc_truck_dist_cls_6tot,
        total_axles_cls_6tot,
        axles_over9t_cls_6tot,
        total_mass_cls_6tot,
        cnt_mass_over9t_cls_6tot,
        eal_pervehicle_cls_6tot,
        total_eal_cls_6tot,
        total_eal_over9t_cls_6tot,
        total_vehicles_cls_7tot,
        weighed_vehicles_cls_7tot,
        perc_truck_dist_cls_7tot,
        total_axles_cls_7tot,
        axles_over9t_cls_7tot,
        total_mass_cls_7tot,
        cnt_mass_over9t_cls_7tot,
        eal_pervehicle_cls_7tot,
        total_eal_cls_7tot,
        total_eal_over9t_cls_7tot,
        total_vehicles_cls_8tot,
        weighed_vehicles_cls_8tot,
        perc_truck_dist_cls_8tot,
        total_axles_cls_8tot,
        axles_over9t_cls_8tot,
        total_mass_cls_8tot,
        cnt_mass_over9t_cls_8tot,
        eal_pervehicle_cls_8tot,
        total_eal_cls_8tot,
        total_eal_over9t_cls_8tot,
        total_vehicles_cls_9tot,
        weighed_vehicles_cls_9tot,
        perc_truck_dist_cls_9tot,
        total_axles_cls_9tot,
        axles_over9t_cls_9tot,
        total_mass_cls_9tot,
        cnt_mass_over9t_cls_9tot,
        eal_pervehicle_cls_9tot,
        total_eal_cls_9tot,
        total_eal_over9t_cls_9tot,
        total_vehicles_cls_10tot,
        weighed_vehicles_cls_10tot,
        perc_truck_dist_cls_10tot,
        total_axles_cls_10tot,
        axles_over9t_cls_10tot,
        total_mass_cls_10tot,
        cnt_mass_over9t_cls_10tot,
        eal_pervehicle_cls_10tot,
        total_eal_cls_10tot,
        total_eal_over9t_cls_10tot,
        total_vehicles_cls_11tot,
        weighed_vehicles_cls_11tot,
        perc_truck_dist_cls_11tot,
        total_axles_cls_11tot,
        axles_over9t_cls_11tot,
        total_mass_cls_11tot,
        cnt_mass_over9t_cls_11tot,
        eal_pervehicle_cls_11tot,
        total_eal_cls_11tot,
        total_eal_over9t_cls_11tot,
        total_vehicles_cls_12tot,
        weighed_vehicles_cls_12tot,
        perc_truck_dist_cls_12tot,
        total_axles_cls_12tot,
        axles_over9t_cls_12tot,
        total_mass_cls_12tot,
        cnt_mass_over9t_cls_12tot,
        eal_pervehicle_cls_12tot,
        total_eal_cls_12tot,
        total_eal_over9t_cls_12tot,
        total_vehicles_cls_13tot,
        weighed_vehicles_cls_13tot,
        perc_truck_dist_cls_13tot,
        total_axles_cls_13tot,
        axles_over9t_cls_13tot,
        total_mass_cls_13tot,
        cnt_mass_over9t_cls_13tot,
        eal_pervehicle_cls_13tot,
        total_eal_cls_13tot,
        total_eal_over9t_cls_13tot,
        total_vehicles_cls_14tot,
        weighed_vehicles_cls_14tot,
        perc_truck_dist_cls_14tot,
        total_axles_cls_14tot,
        axles_over9t_cls_14tot,
        total_mass_cls_14tot,
        cnt_mass_over9t_cls_14tot,
        eal_pervehicle_cls_14tot,
        total_eal_cls_14tot,
        total_eal_over9t_cls_14tot,
        total_vehicles_cls_15tot,
        weighed_vehicles_cls_15tot,
        perc_truck_dist_cls_15tot,
        total_axles_cls_15tot,
        axles_over9t_cls_15tot,
        total_mass_cls_15tot,
        cnt_mass_over9t_cls_15tot,
        eal_pervehicle_cls_15tot,
        total_eal_cls_15tot,
        total_eal_over9t_cls_15tot,
        total_vehicles_cls_16tot,
        weighed_vehicles_cls_16tot,
        perc_truck_dist_cls_16tot,
        total_axles_cls_16tot,
        axles_over9t_cls_16tot,
        total_mass_cls_16tot,
        cnt_mass_over9t_cls_16tot,
        eal_pervehicle_cls_16tot,
        total_eal_cls_16tot,
        total_eal_over9t_cls_16tot,
        total_vehicles_cls_17tot,
        weighed_vehicles_cls_17tot,
        perc_truck_dist_cls_17tot,
        total_axles_cls_17tot,
        axles_over9t_cls_17tot,
        total_mass_cls_17tot,
        cnt_mass_over9t_cls_17tot,
        eal_pervehicle_cls_17tot,
        total_eal_cls_17tot,
        total_eal_over9t_cls_17tot,
        total_vehicles_cls_4p,
        weighed_vehicles_cls_4p,
        perc_truck_dist_cls_4p,
        total_axles_cls_4p,
        axles_over9t_cls_4p,
        total_mass_cls_4p,
        cnt_mass_over9t_cls_4p,
        eal_pervehicle_cls_4p,
        total_eal_cls_4p,
        total_eal_over9t_cls_4p,
        total_vehicles_cls_5p,
        weighed_vehicles_cls_5p,
        perc_truck_dist_cls_5p,
        total_axles_cls_5p,
        axles_over9t_cls_5p,
        total_mass_cls_5p,
        cnt_mass_over9t_cls_5p,
        eal_pervehicle_cls_5p,
        total_eal_cls_5p,
        total_eal_over9t_cls_5p,
        total_vehicles_cls_6p,
        weighed_vehicles_cls_6p,
        perc_truck_dist_cls_6p,
        total_axles_cls_6p,
        axles_over9t_cls_6p,
        total_mass_cls_6p,
        cnt_mass_over9t_cls_6p,
        eal_pervehicle_cls_6p,
        total_eal_cls_6p,
        total_eal_over9t_cls_6p,
        total_vehicles_cls_7p,
        weighed_vehicles_cls_7p,
        perc_truck_dist_cls_7p,
        total_axles_cls_7p,
        axles_over9t_cls_7p,
        total_mass_cls_7p,
        cnt_mass_over9t_cls_7p,
        eal_pervehicle_cls_7p,
        total_eal_cls_7p,
        total_eal_over9t_cls_7p,
        total_vehicles_cls_8p,
        weighed_vehicles_cls_8p,
        perc_truck_dist_cls_8p,
        total_axles_cls_8p,
        axles_over9t_cls_8p,
        total_mass_cls_8p,
        cnt_mass_over9t_cls_8p,
        eal_pervehicle_cls_8p,
        total_eal_cls_8p,
        total_eal_over9t_cls_8p,
        total_vehicles_cls_9p,
        weighed_vehicles_cls_9p,
        perc_truck_dist_cls_9p,
        total_axles_cls_9p,
        axles_over9t_cls_9p,
        total_mass_cls_9p,
        cnt_mass_over9t_cls_9p,
        eal_pervehicle_cls_9p,
        total_eal_cls_9p,
        total_eal_over9t_cls_9p,
        total_vehicles_cls_10p,
        weighed_vehicles_cls_10p,
        perc_truck_dist_cls_10p,
        total_axles_cls_10p,
        axles_over9t_cls_10p,
        total_mass_cls_10p,
        cnt_mass_over9t_cls_10p,
        eal_pervehicle_cls_10p,
        total_eal_cls_10p,
        total_eal_over9t_cls_10p,
        total_vehicles_cls_11p,
        weighed_vehicles_cls_11p,
        perc_truck_dist_cls_11p,
        total_axles_cls_11p,
        axles_over9t_cls_11p,
        total_mass_cls_11p,
        cnt_mass_over9t_cls_11p,
        eal_pervehicle_cls_11p,
        total_eal_cls_11p,
        total_eal_over9t_cls_11p,
        total_vehicles_cls_12p,
        weighed_vehicles_cls_12p,
        perc_truck_dist_cls_12p,
        total_axles_cls_12p,
        axles_over9t_cls_12p,
        total_mass_cls_12p,
        cnt_mass_over9t_cls_12p,
        eal_pervehicle_cls_12p,
        total_eal_cls_12p,
        total_eal_over9t_cls_12p,
        total_vehicles_cls_13p,
        weighed_vehicles_cls_13p,
        perc_truck_dist_cls_13p,
        total_axles_cls_13p,
        axles_over9t_cls_13p,
        total_mass_cls_13p,
        cnt_mass_over9t_cls_13p,
        eal_pervehicle_cls_13p,
        total_eal_cls_13p,
        total_eal_over9t_cls_13p,
        total_vehicles_cls_14p,
        weighed_vehicles_cls_14p,
        perc_truck_dist_cls_14p,
        total_axles_cls_14p,
        axles_over9t_cls_14p,
        total_mass_cls_14p,
        cnt_mass_over9t_cls_14p,
        eal_pervehicle_cls_14p,
        total_eal_cls_14p,
        total_eal_over9t_cls_14p,
        total_vehicles_cls_15p,
        weighed_vehicles_cls_15p,
        perc_truck_dist_cls_15p,
        total_axles_cls_15p,
        axles_over9t_cls_15p,
        total_mass_cls_15p,
        cnt_mass_over9t_cls_15p,
        eal_pervehicle_cls_15p,
        total_eal_cls_15p,
        total_eal_over9t_cls_15p,
        total_vehicles_cls_16p,
        weighed_vehicles_cls_16p,
        perc_truck_dist_cls_16p,
        total_axles_cls_16p,
        axles_over9t_cls_16p,
        total_mass_cls_16p,
        cnt_mass_over9t_cls_16p,
        eal_pervehicle_cls_16p,
        total_eal_cls_16p,
        total_eal_over9t_cls_16p,
        total_vehicles_cls_17p,
        weighed_vehicles_cls_17p,
        perc_truck_dist_cls_17p,
        total_axles_cls_17p,
        axles_over9t_cls_17p,
        total_mass_cls_17p,
        cnt_mass_over9t_cls_17p,
        eal_pervehicle_cls_17p,
        total_eal_cls_17p,
        total_eal_over9t_cls_17p,
        total_vehicles_cls_4n,
        weighed_vehicles_cls_4n,
        perc_truck_dist_cls_4n,
        total_axles_cls_4n,
        axles_over9t_cls_4n,
        total_mass_cls_4n,
        cnt_mass_over9t_cls_4n,
        eal_pervehicle_cls_4n,
        total_eal_cls_4n,
        total_eal_over9t_cls_4n,
        total_vehicles_cls_5n,
        weighed_vehicles_cls_5n,
        perc_truck_dist_cls_5n,
        total_axles_cls_5n,
        axles_over9t_cls_5n,
        total_mass_cls_5n,
        cnt_mass_over9t_cls_5n,
        eal_pervehicle_cls_5n,
        total_eal_cls_5n,
        total_eal_over9t_cls_5n,
        total_vehicles_cls_6n,
        weighed_vehicles_cls_6n,
        perc_truck_dist_cls_6n,
        total_axles_cls_6n,
        axles_over9t_cls_6n,
        total_mass_cls_6n,
        cnt_mass_over9t_cls_6n,
        eal_pervehicle_cls_6n,
        total_eal_cls_6n,
        total_eal_over9t_cls_6n,
        total_vehicles_cls_7n,
        weighed_vehicles_cls_7n,
        perc_truck_dist_cls_7n,
        total_axles_cls_7n,
        axles_over9t_cls_7n,
        total_mass_cls_7n,
        cnt_mass_over9t_cls_7n,
        eal_pervehicle_cls_7n,
        total_eal_cls_7n,
        total_eal_over9t_cls_7n,
        total_vehicles_cls_8n,
        weighed_vehicles_cls_8n,
        perc_truck_dist_cls_8n,
        total_axles_cls_8n,
        axles_over9t_cls_8n,
        total_mass_cls_8n,
        cnt_mass_over9t_cls_8n,
        eal_pervehicle_cls_8n,
        total_eal_cls_8n,
        total_eal_over9t_cls_8n,
        total_vehicles_cls_9n,
        weighed_vehicles_cls_9n,
        perc_truck_dist_cls_9n,
        total_axles_cls_9n,
        axles_over9t_cls_9n,
        total_mass_cls_9n,
        cnt_mass_over9t_cls_9n,
        eal_pervehicle_cls_9n,
        total_eal_cls_9n,
        total_eal_over9t_cls_9n,
        total_vehicles_cls_10n,
        weighed_vehicles_cls_10n,
        perc_truck_dist_cls_10n,
        total_axles_cls_10n,
        axles_over9t_cls_10n,
        total_mass_cls_10n,
        cnt_mass_over9t_cls_10n,
        eal_pervehicle_cls_10n,
        total_eal_cls_10n,
        total_eal_over9t_cls_10n,
        total_vehicles_cls_11n,
        weighed_vehicles_cls_11n,
        perc_truck_dist_cls_11n,
        total_axles_cls_11n,
        axles_over9t_cls_11n,
        total_mass_cls_11n,
        cnt_mass_over9t_cls_11n,
        eal_pervehicle_cls_11n,
        total_eal_cls_11n,
        total_eal_over9t_cls_11n,
        total_vehicles_cls_12n,
        weighed_vehicles_cls_12n,
        perc_truck_dist_cls_12n,
        total_axles_cls_12n,
        axles_over9t_cls_12n,
        total_mass_cls_12n,
        cnt_mass_over9t_cls_12n,
        eal_pervehicle_cls_12n,
        total_eal_cls_12n,
        total_eal_over9t_cls_12n,
        total_vehicles_cls_13n,
        weighed_vehicles_cls_13n,
        perc_truck_dist_cls_13n,
        total_axles_cls_13n,
        axles_over9t_cls_13n,
        total_mass_cls_13n,
        cnt_mass_over9t_cls_13n,
        eal_pervehicle_cls_13n,
        total_eal_cls_13n,
        total_eal_over9t_cls_13n,
        total_vehicles_cls_14n,
        weighed_vehicles_cls_14n,
        perc_truck_dist_cls_14n,
        total_axles_cls_14n,
        axles_over9t_cls_14n,
        total_mass_cls_14n,
        cnt_mass_over9t_cls_14n,
        eal_pervehicle_cls_14n,
        total_eal_cls_14n,
        total_eal_over9t_cls_14n,
        total_vehicles_cls_15n,
        weighed_vehicles_cls_15n,
        perc_truck_dist_cls_15n,
        total_axles_cls_15n,
        axles_over9t_cls_15n,
        total_mass_cls_15n,
        cnt_mass_over9t_cls_15n,
        eal_pervehicle_cls_15n,
        total_eal_cls_15n,
        total_eal_over9t_cls_15n,
        total_vehicles_cls_16n,
        weighed_vehicles_cls_16n,
        perc_truck_dist_cls_16n,
        total_axles_cls_16n,
        axles_over9t_cls_16n,
        total_mass_cls_16n,
        cnt_mass_over9t_cls_16n,
        eal_pervehicle_cls_16n,
        total_eal_cls_16n,
        total_eal_over9t_cls_16n,
        total_vehicles_cls_17n,
        weighed_vehicles_cls_17n,
        perc_truck_dist_cls_17n,
        total_axles_cls_17n,
        axles_over9t_cls_17n,
        total_mass_cls_17n,
        cnt_mass_over9t_cls_17n,
        eal_pervehicle_cls_17n,
        total_eal_cls_17n,
        total_eal_over9t_cls_17n
        )
    VALUES(
        '{header_id}',
        {self.egrl_percent}, --self.egrl_percent
        {self.egrw_percent}, --self.egrw_percent
        {self.mean_equivalent_axle_mass}, --self.mean_equivalent_axle_mass
        {self.mean_equivalent_axle_mass_positive_direction}, --self.mean_equivalent_axle_mass_positive_direction
        {self.mean_equivalent_axle_mass_negative_direction}, --self.mean_equivalent_axle_mass_negative_direction
        {self.mean_axle_spacing}, --self.mean_axle_spacing
        {self.mean_axle_spacing_positive_direction}, --self.mean_axle_spacing_positive_direction
        {self.mean_axle_spacing_negative_direction}, --self.mean_axle_spacing_negative_direction
        {self.e80_per_axle}, --self.e80_per_axle
        {self.e80_per_axle_positive_direction}, --self.e80_per_axle_positive_direction
        {self.e80_per_axle_negative_direction}, --self.e80_per_axle_negative_direction
        {self.olhv}, --self.olhv
        {self.olhv_positive_direction}, --self.olhv_positive_direction
        {self.olhv_negative_direction}, --self.olhv_negative_direction
        {self.olhv_percent}, --self.olhv_percent
        {self.olhv_percent_positive_direction}, --self.olhv_percent_positive_direction
        {self.olhv_percent_negative_direction}, --self.olhv_percent_negative_direction
        {self.tonnage_generated}, --self.tonnage_generated
        {self.tonnage_generated_positive_direction}, --self.tonnage_generated_positive_direction
        {self.tonnage_generated_negative_direction}, --self.tonnage_generated_negative_direction
        {self.olton}, --self.olton
        {self.olton_positive_direction}, --self.olton_positive_direction
        {self.olton_negative_direction}, --self.olton_negative_direction
        {self.olton_percent}, --self.olton_percent
        {self.olton_percent_positive_direction}, --self.olton_percent_positive_direction
        {self.olton_percent_negative_direction}, --self.olton_percent_negative_direction
        {self.ole80}, --self.ole80
        {self.ole80_positive_direction}, --self.ole80_positive_direction
        {self.ole80_negative_direction}, --self.ole80_negative_direction
        {self.ole80_percent}, --self.ole80_percent
        {self.ole80_percent_positive_direction}, --self.ole80_percent_positive_direction
        {self.ole80_percent_negative_direction}, --self.ole80_percent_negative_direction
        {self.xe80}, --self.xe80
        {self.xe80_positive_direction}, --self.xe80_positive_direction
        {self.xe80_negative_direction}, --self.xe80_negative_direction
        {self.xe80_percent}, --self.xe80_percent
        {self.xe80_percent_positive_direction}, --self.xe80_percent_positive_direction
        {self.xe80_percent_negative_direction}, --self.xe80_percent_negative_direction
        {self.e80_per_day}, --self.e80_per_day
        {self.e80_per_day_positive_direction}, --self.e80_per_day_positive_direction
        {self.e80_per_day_negative_direction}, --self.e80_per_day_negative_direction
        {self.e80_per_heavy_vehicle}, --self.e80_per_heavy_vehicle
        {self.e80_per_heavy_vehicle_positive_direction}, --self.e80_per_heavy_vehicle_positive_direction
        {self.e80_per_heavy_vehicle_negative_direction}, --self.e80_per_heavy_vehicle_negative_direction
        {self.worst_steering_single_axle_cnt}, --self.worst_steering_single_axle_cnt
        {self.worst_steering_single_axle_olhv_perc}, --self.worst_steering_single_axle_olhv_perc
        {self.worst_steering_single_axle_tonperhv}, --self.worst_steering_single_axle_tonperhv
        {self.worst_steering_double_axle_cnt}, --self.worst_steering_double_axle_cnt
        {self.worst_steering_double_axle_olhv_perc}, --self.worst_steering_double_axle_olhv_perc
        {self.worst_steering_double_axle_tonperhv}, --self.worst_steering_double_axle_tonperhv
        {self.worst_non_steering_single_axle_cnt}, --self.worst_non_steering_single_axle_cnt
        {self.worst_non_steering_single_axle_olhv_perc}, --self.worst_non_steering_single_axle_olhv_perc
        {self.worst_non_steering_single_axle_tonperhv}, --self.worst_non_steering_single_axle_tonperhv
        {self.worst_non_steering_double_axle_cnt}, --self.worst_non_steering_double_axle_cnt
        {self.worst_non_steering_double_axle_olhv_perc}, --self.worst_non_steering_double_axle_olhv_perc
        {self.worst_non_steering_double_axle_tonperhv}, --self.worst_non_steering_double_axle_tonperhv
        {self.worst_triple_axle_cnt}, --self.worst_triple_axle_cnt
        {self.worst_triple_axle_olhv_perc}, --self.worst_triple_axle_olhv_perc
        {self.worst_triple_axle_tonperhv}, --self.worst_triple_axle_tonperhv
        {self.bridge_formula_cnt}, --self.bridge_formula_cnt
        {self.bridge_formula_olhv_perc}, --self.bridge_formula_olhv_perc
        {self.bridge_formula_tonperhv}, --self.bridge_formula_tonperhv
        {self.gross_formula_cnt}, --self.gross_formula_cnt
        {self.gross_formula_olhv_perc}, --self.gross_formula_olhv_perc
        {self.gross_formula_tonperhv}, --self.gross_formula_tonperhv
        {self.total_avg_cnt}, --self.total_avg_cnt
        {self.total_avg_olhv_perc}, --self.total_avg_olhv_perc
        {self.total_avg_tonperhv}, --self.total_avg_tonperhv
        {self.worst_steering_single_axle_cnt_positive_direciton}, --self.worst_steering_single_axle_cnt_positive_direciton
        {self.worst_steering_single_axle_olhv_perc_positive_direciton}, --self.worst_steering_single_axle_olhv_perc_positive_direciton
        {self.worst_steering_single_axle_tonperhv_positive_direciton}, --self.worst_steering_single_axle_tonperhv_positive_direciton
        {self.worst_steering_double_axle_cnt_positive_direciton}, --self.worst_steering_double_axle_cnt_positive_direciton
        {self.worst_steering_double_axle_olhv_perc_positive_direciton}, --self.worst_steering_double_axle_olhv_perc_positive_direciton
        {self.worst_steering_double_axle_tonperhv_positive_direciton}, --self.worst_steering_double_axle_tonperhv_positive_direciton
        {self.worst_non_steering_single_axle_cnt_positive_direciton}, --self.worst_non_steering_single_axle_cnt_positive_direciton
        {self.worst_non_steering_single_axle_olhv_perc_positive_direciton}, --self.worst_non_steering_single_axle_olhv_perc_positive_direciton
        {self.worst_non_steering_single_axle_tonperhv_positive_direciton}, --self.worst_non_steering_single_axle_tonperhv_positive_direciton
        {self.worst_non_steering_double_axle_cnt_positive_direciton}, --self.worst_non_steering_double_axle_cnt_positive_direciton
        {self.worst_non_steering_double_axle_olhv_perc_positive_direciton}, --self.worst_non_steering_double_axle_olhv_perc_positive_direciton
        {self.worst_non_steering_double_axle_tonperhv_positive_direciton}, --self.worst_non_steering_double_axle_tonperhv_positive_direciton
        {self.worst_triple_axle_cnt_positive_direciton}, --self.worst_triple_axle_cnt_positive_direciton
        {self.worst_triple_axle_olhv_perc_positive_direciton}, --self.worst_triple_axle_olhv_perc_positive_direciton
        {self.worst_triple_axle_tonperhv_positive_direciton}, --self.worst_triple_axle_tonperhv_positive_direciton
        {self.bridge_formula_cnt_positive_direciton}, --self.bridge_formula_cnt_positive_direciton
        {self.bridge_formula_olhv_perc_positive_direciton}, --self.bridge_formula_olhv_perc_positive_direciton
        {self.bridge_formula_tonperhv_positive_direciton}, --self.bridge_formula_tonperhv_positive_direciton
        {self.gross_formula_cnt_positive_direciton}, --self.gross_formula_cnt_positive_direciton
        {self.gross_formula_olhv_perc_positive_direciton}, --self.gross_formula_olhv_perc_positive_direciton
        {self.gross_formula_tonperhv_positive_direciton}, --self.gross_formula_tonperhv_positive_direciton
        {self.total_avg_cnt_positive_direciton}, --self.total_avg_cnt_positive_direciton
        {self.total_avg_olhv_perc_positive_direciton}, --self.total_avg_olhv_perc_positive_direciton
        {self.total_avg_tonperhv_positive_direciton}, --self.total_avg_tonperhv_positive_direciton
        {self.worst_steering_single_axle_cnt_negative_direciton}, --self.worst_steering_single_axle_cnt_negative_direciton
        {self.worst_steering_single_axle_olhv_perc_negative_direciton}, --self.worst_steering_single_axle_olhv_perc_negative_direciton
        {self.worst_steering_single_axle_tonperhv_negative_direciton}, --self.worst_steering_single_axle_tonperhv_negative_direciton
        {self.worst_steering_double_axle_cnt_negative_direciton}, --self.worst_steering_double_axle_cnt_negative_direciton
        {self.worst_steering_double_axle_olhv_perc_negative_direciton}, --self.worst_steering_double_axle_olhv_perc_negative_direciton
        {self.worst_steering_double_axle_tonperhv_negative_direciton}, --self.worst_steering_double_axle_tonperhv_negative_direciton
        {self.worst_non_steering_single_axle_cnt_negative_direciton}, --self.worst_non_steering_single_axle_cnt_negative_direciton
        {self.worst_non_steering_single_axle_olhv_perc_negative_direciton}, --self.worst_non_steering_single_axle_olhv_perc_negative_direciton
        {self.worst_non_steering_single_axle_tonperhv_negative_direciton}, --self.worst_non_steering_single_axle_tonperhv_negative_direciton
        {self.worst_non_steering_double_axle_cnt_negative_direciton}, --self.worst_non_steering_double_axle_cnt_negative_direciton
        {self.worst_non_steering_double_axle_olhv_perc_negative_direciton}, --self.worst_non_steering_double_axle_olhv_perc_negative_direciton
        {self.worst_non_steering_double_axle_tonperhv_negative_direciton}, --self.worst_non_steering_double_axle_tonperhv_negative_direciton
        {self.worst_triple_axle_cnt_negative_direciton}, --self.worst_triple_axle_cnt_negative_direciton
        {self.worst_triple_axle_olhv_perc_negative_direciton}, --self.worst_triple_axle_olhv_perc_negative_direciton
        {self.worst_triple_axle_tonperhv_negative_direciton}, --self.worst_triple_axle_tonperhv_negative_direciton
        {self.bridge_formula_cnt_negative_direciton}, --self.bridge_formula_cnt_negative_direciton
        {self.bridge_formula_olhv_perc_negative_direciton}, --self.bridge_formula_olhv_perc_negative_direciton
        {self.bridge_formula_tonperhv_negative_direciton}, --self.bridge_formula_tonperhv_negative_direciton
        {self.gross_formula_cnt_negative_direciton}, --self.gross_formula_cnt_negative_direciton
        {self.gross_formula_olhv_perc_negative_direciton}, --self.gross_formula_olhv_perc_negative_direciton
        {self.gross_formula_tonperhv_negative_direciton}, --self.gross_formula_tonperhv_negative_direciton
        {self.total_avg_cnt_negative_direciton}, --self.total_avg_cnt_negative_direciton
        {self.total_avg_olhv_perc_negative_direciton}, --self.total_avg_olhv_perc_negative_direciton
        {self.total_avg_tonperhv_negative_direciton}, --self.total_avg_tonperhv_negative_direciton
        {self.egrl_percent_positive_direction}, --self.egrl_percent_positive_direction
        {self.egrl_percent_negative_direction}, --self.egrl_percent_negative_direction
        {self.egrw_percent_positive_direction}, --self.egrw_percent_positive_direction
        {self.egrw_percent_negative_direction}, --self.egrw_percent_negative_direction
        {self.num_weighed}, --self.num_weighed
        {self.num_weighed_positive_direction}, --self.num_weighed_positive_direction
        {self.num_weighed_negative_direction}, --self.num_weighed_negative_direction
        {self.total_vehicles_cls_4tot}, --self.total_vehicles_cls_4tot
        {self.weighed_vehicles_cls_4tot}, --self.weighed_vehicles_cls_4tot
        {self.perc_truck_dist_cls_4tot}, --self.perc_truck_dist_cls_4tot
        {self.total_axles_cls_4tot}, --self.total_axles_cls_4tot
        {self.axles_over9t_cls_4tot}, --self.axles_over9t_cls_4tot
        {self.total_mass_cls_4tot}, --self.total_mass_cls_4tot
        {self.cnt_mass_over9t_cls_4tot}, --self.cnt_mass_over9t_cls_4tot
        {self.eal_pervehicle_cls_4tot}, --self.eal_pervehicle_cls_4tot
        {self.total_eal_cls_4tot}, --self.total_eal_cls_4tot
        {self.total_eal_over9t_cls_4tot}, --self.total_eal_over9t_cls_4tot
        {self.total_vehicles_cls_5tot}, --self.total_vehicles_cls_5tot
        {self.weighed_vehicles_cls_5tot}, --self.weighed_vehicles_cls_5tot
        {self.perc_truck_dist_cls_5tot}, --self.perc_truck_dist_cls_5tot
        {self.total_axles_cls_5tot}, --self.total_axles_cls_5tot
        {self.axles_over9t_cls_5tot}, --self.axles_over9t_cls_5tot
        {self.total_mass_cls_5tot}, --self.total_mass_cls_5tot
        {self.cnt_mass_over9t_cls_5tot}, --self.cnt_mass_over9t_cls_5tot
        {self.eal_pervehicle_cls_5tot}, --self.eal_pervehicle_cls_5tot
        {self.total_eal_cls_5tot}, --self.total_eal_cls_5tot
        {self.total_eal_over9t_cls_5tot}, --self.total_eal_over9t_cls_5tot
        {self.total_vehicles_cls_6tot}, --self.total_vehicles_cls_6tot
        {self.weighed_vehicles_cls_6tot}, --self.weighed_vehicles_cls_6tot
        {self.perc_truck_dist_cls_6tot}, --self.perc_truck_dist_cls_6tot
        {self.total_axles_cls_6tot}, --self.total_axles_cls_6tot
        {self.axles_over9t_cls_6tot}, --self.axles_over9t_cls_6tot
        {self.total_mass_cls_6tot}, --self.total_mass_cls_6tot
        {self.cnt_mass_over9t_cls_6tot}, --self.cnt_mass_over9t_cls_6tot
        {self.eal_pervehicle_cls_6tot}, --self.eal_pervehicle_cls_6tot
        {self.total_eal_cls_6tot}, --self.total_eal_cls_6tot
        {self.total_eal_over9t_cls_6tot}, --self.total_eal_over9t_cls_6tot
        {self.total_vehicles_cls_7tot}, --self.total_vehicles_cls_7tot
        {self.weighed_vehicles_cls_7tot}, --self.weighed_vehicles_cls_7tot
        {self.perc_truck_dist_cls_7tot}, --self.perc_truck_dist_cls_7tot
        {self.total_axles_cls_7tot}, --self.total_axles_cls_7tot
        {self.axles_over9t_cls_7tot}, --self.axles_over9t_cls_7tot
        {self.total_mass_cls_7tot}, --self.total_mass_cls_7tot
        {self.cnt_mass_over9t_cls_7tot}, --self.cnt_mass_over9t_cls_7tot
        {self.eal_pervehicle_cls_7tot}, --self.eal_pervehicle_cls_7tot
        {self.total_eal_cls_7tot}, --self.total_eal_cls_7tot
        {self.total_eal_over9t_cls_7tot}, --self.total_eal_over9t_cls_7tot
        {self.total_vehicles_cls_8tot}, --self.total_vehicles_cls_8tot
        {self.weighed_vehicles_cls_8tot}, --self.weighed_vehicles_cls_8tot
        {self.perc_truck_dist_cls_8tot}, --self.perc_truck_dist_cls_8tot
        {self.total_axles_cls_8tot}, --self.total_axles_cls_8tot
        {self.axles_over9t_cls_8tot}, --self.axles_over9t_cls_8tot
        {self.total_mass_cls_8tot}, --self.total_mass_cls_8tot
        {self.cnt_mass_over9t_cls_8tot}, --self.cnt_mass_over9t_cls_8tot
        {self.eal_pervehicle_cls_8tot}, --self.eal_pervehicle_cls_8tot
        {self.total_eal_cls_8tot}, --self.total_eal_cls_8tot
        {self.total_eal_over9t_cls_8tot}, --self.total_eal_over9t_cls_8tot
        {self.total_vehicles_cls_9tot}, --self.total_vehicles_cls_9tot
        {self.weighed_vehicles_cls_9tot}, --self.weighed_vehicles_cls_9tot
        {self.perc_truck_dist_cls_9tot}, --self.perc_truck_dist_cls_9tot
        {self.total_axles_cls_9tot}, --self.total_axles_cls_9tot
        {self.axles_over9t_cls_9tot}, --self.axles_over9t_cls_9tot
        {self.total_mass_cls_9tot}, --self.total_mass_cls_9tot
        {self.cnt_mass_over9t_cls_9tot}, --self.cnt_mass_over9t_cls_9tot
        {self.eal_pervehicle_cls_9tot}, --self.eal_pervehicle_cls_9tot
        {self.total_eal_cls_9tot}, --self.total_eal_cls_9tot
        {self.total_eal_over9t_cls_9tot}, --self.total_eal_over9t_cls_9tot
        {self.total_vehicles_cls_10tot}, --self.total_vehicles_cls_10tot
        {self.weighed_vehicles_cls_10tot}, --self.weighed_vehicles_cls_10tot
        {self.perc_truck_dist_cls_10tot}, --self.perc_truck_dist_cls_10tot
        {self.total_axles_cls_10tot}, --self.total_axles_cls_10tot
        {self.axles_over9t_cls_10tot}, --self.axles_over9t_cls_10tot
        {self.total_mass_cls_10tot}, --self.total_mass_cls_10tot
        {self.cnt_mass_over9t_cls_10tot}, --self.cnt_mass_over9t_cls_10tot
        {self.eal_pervehicle_cls_10tot}, --self.eal_pervehicle_cls_10tot
        {self.total_eal_cls_10tot}, --self.total_eal_cls_10tot
        {self.total_eal_over9t_cls_10tot}, --self.total_eal_over9t_cls_10tot
        {self.total_vehicles_cls_11tot}, --self.total_vehicles_cls_11tot
        {self.weighed_vehicles_cls_11tot}, --self.weighed_vehicles_cls_11tot
        {self.perc_truck_dist_cls_11tot}, --self.perc_truck_dist_cls_11tot
        {self.total_axles_cls_11tot}, --self.total_axles_cls_11tot
        {self.axles_over9t_cls_11tot}, --self.axles_over9t_cls_11tot
        {self.total_mass_cls_11tot}, --self.total_mass_cls_11tot
        {self.cnt_mass_over9t_cls_11tot}, --self.cnt_mass_over9t_cls_11tot
        {self.eal_pervehicle_cls_11tot}, --self.eal_pervehicle_cls_11tot
        {self.total_eal_cls_11tot}, --self.total_eal_cls_11tot
        {self.total_eal_over9t_cls_11tot}, --self.total_eal_over9t_cls_11tot
        {self.total_vehicles_cls_12tot}, --self.total_vehicles_cls_12tot
        {self.weighed_vehicles_cls_12tot}, --self.weighed_vehicles_cls_12tot
        {self.perc_truck_dist_cls_12tot}, --self.perc_truck_dist_cls_12tot
        {self.total_axles_cls_12tot}, --self.total_axles_cls_12tot
        {self.axles_over9t_cls_12tot}, --self.axles_over9t_cls_12tot
        {self.total_mass_cls_12tot}, --self.total_mass_cls_12tot
        {self.cnt_mass_over9t_cls_12tot}, --self.cnt_mass_over9t_cls_12tot
        {self.eal_pervehicle_cls_12tot}, --self.eal_pervehicle_cls_12tot
        {self.total_eal_cls_12tot}, --self.total_eal_cls_12tot
        {self.total_eal_over9t_cls_12tot}, --self.total_eal_over9t_cls_12tot
        {self.total_vehicles_cls_13tot}, --self.total_vehicles_cls_13tot
        {self.weighed_vehicles_cls_13tot}, --self.weighed_vehicles_cls_13tot
        {self.perc_truck_dist_cls_13tot}, --self.perc_truck_dist_cls_13tot
        {self.total_axles_cls_13tot}, --self.total_axles_cls_13tot
        {self.axles_over9t_cls_13tot}, --self.axles_over9t_cls_13tot
        {self.total_mass_cls_13tot}, --self.total_mass_cls_13tot
        {self.cnt_mass_over9t_cls_13tot}, --self.cnt_mass_over9t_cls_13tot
        {self.eal_pervehicle_cls_13tot}, --self.eal_pervehicle_cls_13tot
        {self.total_eal_cls_13tot}, --self.total_eal_cls_13tot
        {self.total_eal_over9t_cls_13tot}, --self.total_eal_over9t_cls_13tot
        {self.total_vehicles_cls_14tot}, --self.total_vehicles_cls_14tot
        {self.weighed_vehicles_cls_14tot}, --self.weighed_vehicles_cls_14tot
        {self.perc_truck_dist_cls_14tot}, --self.perc_truck_dist_cls_14tot
        {self.total_axles_cls_14tot}, --self.total_axles_cls_14tot
        {self.axles_over9t_cls_14tot}, --self.axles_over9t_cls_14tot
        {self.total_mass_cls_14tot}, --self.total_mass_cls_14tot
        {self.cnt_mass_over9t_cls_14tot}, --self.cnt_mass_over9t_cls_14tot
        {self.eal_pervehicle_cls_14tot}, --self.eal_pervehicle_cls_14tot
        {self.total_eal_cls_14tot}, --self.total_eal_cls_14tot
        {self.total_eal_over9t_cls_14tot}, --self.total_eal_over9t_cls_14tot
        {self.total_vehicles_cls_15tot}, --self.total_vehicles_cls_15tot
        {self.weighed_vehicles_cls_15tot}, --self.weighed_vehicles_cls_15tot
        {self.perc_truck_dist_cls_15tot}, --self.perc_truck_dist_cls_15tot
        {self.total_axles_cls_15tot}, --self.total_axles_cls_15tot
        {self.axles_over9t_cls_15tot}, --self.axles_over9t_cls_15tot
        {self.total_mass_cls_15tot}, --self.total_mass_cls_15tot
        {self.cnt_mass_over9t_cls_15tot}, --self.cnt_mass_over9t_cls_15tot
        {self.eal_pervehicle_cls_15tot}, --self.eal_pervehicle_cls_15tot
        {self.total_eal_cls_15tot}, --self.total_eal_cls_15tot
        {self.total_eal_over9t_cls_15tot}, --self.total_eal_over9t_cls_15tot
        {self.total_vehicles_cls_16tot}, --self.total_vehicles_cls_16tot
        {self.weighed_vehicles_cls_16tot}, --self.weighed_vehicles_cls_16tot
        {self.perc_truck_dist_cls_16tot}, --self.perc_truck_dist_cls_16tot
        {self.total_axles_cls_16tot}, --self.total_axles_cls_16tot
        {self.axles_over9t_cls_16tot}, --self.axles_over9t_cls_16tot
        {self.total_mass_cls_16tot}, --self.total_mass_cls_16tot
        {self.cnt_mass_over9t_cls_16tot}, --self.cnt_mass_over9t_cls_16tot
        {self.eal_pervehicle_cls_16tot}, --self.eal_pervehicle_cls_16tot
        {self.total_eal_cls_16tot}, --self.total_eal_cls_16tot
        {self.total_eal_over9t_cls_16tot}, --self.total_eal_over9t_cls_16tot
        {self.total_vehicles_cls_17tot}, --self.total_vehicles_cls_17tot
        {self.weighed_vehicles_cls_17tot}, --self.weighed_vehicles_cls_17tot
        {self.perc_truck_dist_cls_17tot}, --self.perc_truck_dist_cls_17tot
        {self.total_axles_cls_17tot}, --self.total_axles_cls_17tot
        {self.axles_over9t_cls_17tot}, --self.axles_over9t_cls_17tot
        {self.total_mass_cls_17tot}, --self.total_mass_cls_17tot
        {self.cnt_mass_over9t_cls_17tot}, --self.cnt_mass_over9t_cls_17tot
        {self.eal_pervehicle_cls_17tot}, --self.eal_pervehicle_cls_17tot
        {self.total_eal_cls_17tot}, --self.total_eal_cls_17tot
        {self.total_eal_over9t_cls_17tot}, --self.total_eal_over9t_cls_17tot
        {self.total_vehicles_cls_4p}, --self.total_vehicles_cls_4p
        {self.weighed_vehicles_cls_4p}, --self.weighed_vehicles_cls_4p
        {self.perc_truck_dist_cls_4p}, --self.perc_truck_dist_cls_4p
        {self.total_axles_cls_4p}, --self.total_axles_cls_4p
        {self.axles_over9t_cls_4p}, --self.axles_over9t_cls_4p
        {self.total_mass_cls_4p}, --self.total_mass_cls_4p
        {self.cnt_mass_over9t_cls_4p}, --self.cnt_mass_over9t_cls_4p
        {self.eal_pervehicle_cls_4p}, --self.eal_pervehicle_cls_4p
        {self.total_eal_cls_4p}, --self.total_eal_cls_4p
        {self.total_eal_over9t_cls_4p}, --self.total_eal_over9t_cls_4p
        {self.total_vehicles_cls_5p}, --self.total_vehicles_cls_5p
        {self.weighed_vehicles_cls_5p}, --self.weighed_vehicles_cls_5p
        {self.perc_truck_dist_cls_5p}, --self.perc_truck_dist_cls_5p
        {self.total_axles_cls_5p}, --self.total_axles_cls_5p
        {self.axles_over9t_cls_5p}, --self.axles_over9t_cls_5p
        {self.total_mass_cls_5p}, --self.total_mass_cls_5p
        {self.cnt_mass_over9t_cls_5p}, --self.cnt_mass_over9t_cls_5p
        {self.eal_pervehicle_cls_5p}, --self.eal_pervehicle_cls_5p
        {self.total_eal_cls_5p}, --self.total_eal_cls_5p
        {self.total_eal_over9t_cls_5p}, --self.total_eal_over9t_cls_5p
        {self.total_vehicles_cls_6p}, --self.total_vehicles_cls_6p
        {self.weighed_vehicles_cls_6p}, --self.weighed_vehicles_cls_6p
        {self.perc_truck_dist_cls_6p}, --self.perc_truck_dist_cls_6p
        {self.total_axles_cls_6p}, --self.total_axles_cls_6p
        {self.axles_over9t_cls_6p}, --self.axles_over9t_cls_6p
        {self.total_mass_cls_6p}, --self.total_mass_cls_6p
        {self.cnt_mass_over9t_cls_6p}, --self.cnt_mass_over9t_cls_6p
        {self.eal_pervehicle_cls_6p}, --self.eal_pervehicle_cls_6p
        {self.total_eal_cls_6p}, --self.total_eal_cls_6p
        {self.total_eal_over9t_cls_6p}, --self.total_eal_over9t_cls_6p
        {self.total_vehicles_cls_7p}, --self.total_vehicles_cls_7p
        {self.weighed_vehicles_cls_7p}, --self.weighed_vehicles_cls_7p
        {self.perc_truck_dist_cls_7p}, --self.perc_truck_dist_cls_7p
        {self.total_axles_cls_7p}, --self.total_axles_cls_7p
        {self.axles_over9t_cls_7p}, --self.axles_over9t_cls_7p
        {self.total_mass_cls_7p}, --self.total_mass_cls_7p
        {self.cnt_mass_over9t_cls_7p}, --self.cnt_mass_over9t_cls_7p
        {self.eal_pervehicle_cls_7p}, --self.eal_pervehicle_cls_7p
        {self.total_eal_cls_7p}, --self.total_eal_cls_7p
        {self.total_eal_over9t_cls_7p}, --self.total_eal_over9t_cls_7p
        {self.total_vehicles_cls_8p}, --self.total_vehicles_cls_8p
        {self.weighed_vehicles_cls_8p}, --self.weighed_vehicles_cls_8p
        {self.perc_truck_dist_cls_8p}, --self.perc_truck_dist_cls_8p
        {self.total_axles_cls_8p}, --self.total_axles_cls_8p
        {self.axles_over9t_cls_8p}, --self.axles_over9t_cls_8p
        {self.total_mass_cls_8p}, --self.total_mass_cls_8p
        {self.cnt_mass_over9t_cls_8p}, --self.cnt_mass_over9t_cls_8p
        {self.eal_pervehicle_cls_8p}, --self.eal_pervehicle_cls_8p
        {self.total_eal_cls_8p}, --self.total_eal_cls_8p
        {self.total_eal_over9t_cls_8p}, --self.total_eal_over9t_cls_8p
        {self.total_vehicles_cls_9p}, --self.total_vehicles_cls_9p
        {self.weighed_vehicles_cls_9p}, --self.weighed_vehicles_cls_9p
        {self.perc_truck_dist_cls_9p}, --self.perc_truck_dist_cls_9p
        {self.total_axles_cls_9p}, --self.total_axles_cls_9p
        {self.axles_over9t_cls_9p}, --self.axles_over9t_cls_9p
        {self.total_mass_cls_9p}, --self.total_mass_cls_9p
        {self.cnt_mass_over9t_cls_9p}, --self.cnt_mass_over9t_cls_9p
        {self.eal_pervehicle_cls_9p}, --self.eal_pervehicle_cls_9p
        {self.total_eal_cls_9p}, --self.total_eal_cls_9p
        {self.total_eal_over9t_cls_9p}, --self.total_eal_over9t_cls_9p
        {self.total_vehicles_cls_10p}, --self.total_vehicles_cls_10p
        {self.weighed_vehicles_cls_10p}, --self.weighed_vehicles_cls_10p
        {self.perc_truck_dist_cls_10p}, --self.perc_truck_dist_cls_10p
        {self.total_axles_cls_10p}, --self.total_axles_cls_10p
        {self.axles_over9t_cls_10p}, --self.axles_over9t_cls_10p
        {self.total_mass_cls_10p}, --self.total_mass_cls_10p
        {self.cnt_mass_over9t_cls_10p}, --self.cnt_mass_over9t_cls_10p
        {self.eal_pervehicle_cls_10p}, --self.eal_pervehicle_cls_10p
        {self.total_eal_cls_10p}, --self.total_eal_cls_10p
        {self.total_eal_over9t_cls_10p}, --self.total_eal_over9t_cls_10p
        {self.total_vehicles_cls_11p}, --self.total_vehicles_cls_11p
        {self.weighed_vehicles_cls_11p}, --self.weighed_vehicles_cls_11p
        {self.perc_truck_dist_cls_11p}, --self.perc_truck_dist_cls_11p
        {self.total_axles_cls_11p}, --self.total_axles_cls_11p
        {self.axles_over9t_cls_11p}, --self.axles_over9t_cls_11p
        {self.total_mass_cls_11p}, --self.total_mass_cls_11p
        {self.cnt_mass_over9t_cls_11p}, --self.cnt_mass_over9t_cls_11p
        {self.eal_pervehicle_cls_11p}, --self.eal_pervehicle_cls_11p
        {self.total_eal_cls_11p}, --self.total_eal_cls_11p
        {self.total_eal_over9t_cls_11p}, --self.total_eal_over9t_cls_11p
        {self.total_vehicles_cls_12p}, --self.total_vehicles_cls_12p
        {self.weighed_vehicles_cls_12p}, --self.weighed_vehicles_cls_12p
        {self.perc_truck_dist_cls_12p}, --self.perc_truck_dist_cls_12p
        {self.total_axles_cls_12p}, --self.total_axles_cls_12p
        {self.axles_over9t_cls_12p}, --self.axles_over9t_cls_12p
        {self.total_mass_cls_12p}, --self.total_mass_cls_12p
        {self.cnt_mass_over9t_cls_12p}, --self.cnt_mass_over9t_cls_12p
        {self.eal_pervehicle_cls_12p}, --self.eal_pervehicle_cls_12p
        {self.total_eal_cls_12p}, --self.total_eal_cls_12p
        {self.total_eal_over9t_cls_12p}, --self.total_eal_over9t_cls_12p
        {self.total_vehicles_cls_13p}, --self.total_vehicles_cls_13p
        {self.weighed_vehicles_cls_13p}, --self.weighed_vehicles_cls_13p
        {self.perc_truck_dist_cls_13p}, --self.perc_truck_dist_cls_13p
        {self.total_axles_cls_13p}, --self.total_axles_cls_13p
        {self.axles_over9t_cls_13p}, --self.axles_over9t_cls_13p
        {self.total_mass_cls_13p}, --self.total_mass_cls_13p
        {self.cnt_mass_over9t_cls_13p}, --self.cnt_mass_over9t_cls_13p
        {self.eal_pervehicle_cls_13p}, --self.eal_pervehicle_cls_13p
        {self.total_eal_cls_13p}, --self.total_eal_cls_13p
        {self.total_eal_over9t_cls_13p}, --self.total_eal_over9t_cls_13p
        {self.total_vehicles_cls_14p}, --self.total_vehicles_cls_14p
        {self.weighed_vehicles_cls_14p}, --self.weighed_vehicles_cls_14p
        {self.perc_truck_dist_cls_14p}, --self.perc_truck_dist_cls_14p
        {self.total_axles_cls_14p}, --self.total_axles_cls_14p
        {self.axles_over9t_cls_14p}, --self.axles_over9t_cls_14p
        {self.total_mass_cls_14p}, --self.total_mass_cls_14p
        {self.cnt_mass_over9t_cls_14p}, --self.cnt_mass_over9t_cls_14p
        {self.eal_pervehicle_cls_14p}, --self.eal_pervehicle_cls_14p
        {self.total_eal_cls_14p}, --self.total_eal_cls_14p
        {self.total_eal_over9t_cls_14p}, --self.total_eal_over9t_cls_14p
        {self.total_vehicles_cls_15p}, --self.total_vehicles_cls_15p
        {self.weighed_vehicles_cls_15p}, --self.weighed_vehicles_cls_15p
        {self.perc_truck_dist_cls_15p}, --self.perc_truck_dist_cls_15p
        {self.total_axles_cls_15p}, --self.total_axles_cls_15p
        {self.axles_over9t_cls_15p}, --self.axles_over9t_cls_15p
        {self.total_mass_cls_15p}, --self.total_mass_cls_15p
        {self.cnt_mass_over9t_cls_15p}, --self.cnt_mass_over9t_cls_15p
        {self.eal_pervehicle_cls_15p}, --self.eal_pervehicle_cls_15p
        {self.total_eal_cls_15p}, --self.total_eal_cls_15p
        {self.total_eal_over9t_cls_15p}, --self.total_eal_over9t_cls_15p
        {self.total_vehicles_cls_16p}, --self.total_vehicles_cls_16p
        {self.weighed_vehicles_cls_16p}, --self.weighed_vehicles_cls_16p
        {self.perc_truck_dist_cls_16p}, --self.perc_truck_dist_cls_16p
        {self.total_axles_cls_16p}, --self.total_axles_cls_16p
        {self.axles_over9t_cls_16p}, --self.axles_over9t_cls_16p
        {self.total_mass_cls_16p}, --self.total_mass_cls_16p
        {self.cnt_mass_over9t_cls_16p}, --self.cnt_mass_over9t_cls_16p
        {self.eal_pervehicle_cls_16p}, --self.eal_pervehicle_cls_16p
        {self.total_eal_cls_16p}, --self.total_eal_cls_16p
        {self.total_eal_over9t_cls_16p}, --self.total_eal_over9t_cls_16p
        {self.total_vehicles_cls_17p}, --self.total_vehicles_cls_17p
        {self.weighed_vehicles_cls_17p}, --self.weighed_vehicles_cls_17p
        {self.perc_truck_dist_cls_17p}, --self.perc_truck_dist_cls_17p
        {self.total_axles_cls_17p}, --self.total_axles_cls_17p
        {self.axles_over9t_cls_17p}, --self.axles_over9t_cls_17p
        {self.total_mass_cls_17p}, --self.total_mass_cls_17p
        {self.cnt_mass_over9t_cls_17p}, --self.cnt_mass_over9t_cls_17p
        {self.eal_pervehicle_cls_17p}, --self.eal_pervehicle_cls_17p
        {self.total_eal_cls_17p}, --self.total_eal_cls_17p
        {self.total_eal_over9t_cls_17p}, --self.total_eal_over9t_cls_17p
        {self.total_vehicles_cls_4n}, --self.total_vehicles_cls_4n
        {self.weighed_vehicles_cls_4n}, --self.weighed_vehicles_cls_4n
        {self.perc_truck_dist_cls_4n}, --self.perc_truck_dist_cls_4n
        {self.total_axles_cls_4n}, --self.total_axles_cls_4n
        {self.axles_over9t_cls_4n}, --self.axles_over9t_cls_4n
        {self.total_mass_cls_4n}, --self.total_mass_cls_4n
        {self.cnt_mass_over9t_cls_4n}, --self.cnt_mass_over9t_cls_4n
        {self.eal_pervehicle_cls_4n}, --self.eal_pervehicle_cls_4n
        {self.total_eal_cls_4n}, --self.total_eal_cls_4n
        {self.total_eal_over9t_cls_4n}, --self.total_eal_over9t_cls_4n
        {self.total_vehicles_cls_5n}, --self.total_vehicles_cls_5n
        {self.weighed_vehicles_cls_5n}, --self.weighed_vehicles_cls_5n
        {self.perc_truck_dist_cls_5n}, --self.perc_truck_dist_cls_5n
        {self.total_axles_cls_5n}, --self.total_axles_cls_5n
        {self.axles_over9t_cls_5n}, --self.axles_over9t_cls_5n
        {self.total_mass_cls_5n}, --self.total_mass_cls_5n
        {self.cnt_mass_over9t_cls_5n}, --self.cnt_mass_over9t_cls_5n
        {self.eal_pervehicle_cls_5n}, --self.eal_pervehicle_cls_5n
        {self.total_eal_cls_5n}, --self.total_eal_cls_5n
        {self.total_eal_over9t_cls_5n}, --self.total_eal_over9t_cls_5n
        {self.total_vehicles_cls_6n}, --self.total_vehicles_cls_6n
        {self.weighed_vehicles_cls_6n}, --self.weighed_vehicles_cls_6n
        {self.perc_truck_dist_cls_6n}, --self.perc_truck_dist_cls_6n
        {self.total_axles_cls_6n}, --self.total_axles_cls_6n
        {self.axles_over9t_cls_6n}, --self.axles_over9t_cls_6n
        {self.total_mass_cls_6n}, --self.total_mass_cls_6n
        {self.cnt_mass_over9t_cls_6n}, --self.cnt_mass_over9t_cls_6n
        {self.eal_pervehicle_cls_6n}, --self.eal_pervehicle_cls_6n
        {self.total_eal_cls_6n}, --self.total_eal_cls_6n
        {self.total_eal_over9t_cls_6n}, --self.total_eal_over9t_cls_6n
        {self.total_vehicles_cls_7n}, --self.total_vehicles_cls_7n
        {self.weighed_vehicles_cls_7n}, --self.weighed_vehicles_cls_7n
        {self.perc_truck_dist_cls_7n}, --self.perc_truck_dist_cls_7n
        {self.total_axles_cls_7n}, --self.total_axles_cls_7n
        {self.axles_over9t_cls_7n}, --self.axles_over9t_cls_7n
        {self.total_mass_cls_7n}, --self.total_mass_cls_7n
        {self.cnt_mass_over9t_cls_7n}, --self.cnt_mass_over9t_cls_7n
        {self.eal_pervehicle_cls_7n}, --self.eal_pervehicle_cls_7n
        {self.total_eal_cls_7n}, --self.total_eal_cls_7n
        {self.total_eal_over9t_cls_7n}, --self.total_eal_over9t_cls_7n
        {self.total_vehicles_cls_8n}, --self.total_vehicles_cls_8n
        {self.weighed_vehicles_cls_8n}, --self.weighed_vehicles_cls_8n
        {self.perc_truck_dist_cls_8n}, --self.perc_truck_dist_cls_8n
        {self.total_axles_cls_8n}, --self.total_axles_cls_8n
        {self.axles_over9t_cls_8n}, --self.axles_over9t_cls_8n
        {self.total_mass_cls_8n}, --self.total_mass_cls_8n
        {self.cnt_mass_over9t_cls_8n}, --self.cnt_mass_over9t_cls_8n
        {self.eal_pervehicle_cls_8n}, --self.eal_pervehicle_cls_8n
        {self.total_eal_cls_8n}, --self.total_eal_cls_8n
        {self.total_eal_over9t_cls_8n}, --self.total_eal_over9t_cls_8n
        {self.total_vehicles_cls_9n}, --self.total_vehicles_cls_9n
        {self.weighed_vehicles_cls_9n}, --self.weighed_vehicles_cls_9n
        {self.perc_truck_dist_cls_9n}, --self.perc_truck_dist_cls_9n
        {self.total_axles_cls_9n}, --self.total_axles_cls_9n
        {self.axles_over9t_cls_9n}, --self.axles_over9t_cls_9n
        {self.total_mass_cls_9n}, --self.total_mass_cls_9n
        {self.cnt_mass_over9t_cls_9n}, --self.cnt_mass_over9t_cls_9n
        {self.eal_pervehicle_cls_9n}, --self.eal_pervehicle_cls_9n
        {self.total_eal_cls_9n}, --self.total_eal_cls_9n
        {self.total_eal_over9t_cls_9n}, --self.total_eal_over9t_cls_9n
        {self.total_vehicles_cls_10n}, --self.total_vehicles_cls_10n
        {self.weighed_vehicles_cls_10n}, --self.weighed_vehicles_cls_10n
        {self.perc_truck_dist_cls_10n}, --self.perc_truck_dist_cls_10n
        {self.total_axles_cls_10n}, --self.total_axles_cls_10n
        {self.axles_over9t_cls_10n}, --self.axles_over9t_cls_10n
        {self.total_mass_cls_10n}, --self.total_mass_cls_10n
        {self.cnt_mass_over9t_cls_10n}, --self.cnt_mass_over9t_cls_10n
        {self.eal_pervehicle_cls_10n}, --self.eal_pervehicle_cls_10n
        {self.total_eal_cls_10n}, --self.total_eal_cls_10n
        {self.total_eal_over9t_cls_10n}, --self.total_eal_over9t_cls_10n
        {self.total_vehicles_cls_11n}, --self.total_vehicles_cls_11n
        {self.weighed_vehicles_cls_11n}, --self.weighed_vehicles_cls_11n
        {self.perc_truck_dist_cls_11n}, --self.perc_truck_dist_cls_11n
        {self.total_axles_cls_11n}, --self.total_axles_cls_11n
        {self.axles_over9t_cls_11n}, --self.axles_over9t_cls_11n
        {self.total_mass_cls_11n}, --self.total_mass_cls_11n
        {self.cnt_mass_over9t_cls_11n}, --self.cnt_mass_over9t_cls_11n
        {self.eal_pervehicle_cls_11n}, --self.eal_pervehicle_cls_11n
        {self.total_eal_cls_11n}, --self.total_eal_cls_11n
        {self.total_eal_over9t_cls_11n}, --self.total_eal_over9t_cls_11n
        {self.total_vehicles_cls_12n}, --self.total_vehicles_cls_12n
        {self.weighed_vehicles_cls_12n}, --self.weighed_vehicles_cls_12n
        {self.perc_truck_dist_cls_12n}, --self.perc_truck_dist_cls_12n
        {self.total_axles_cls_12n}, --self.total_axles_cls_12n
        {self.axles_over9t_cls_12n}, --self.axles_over9t_cls_12n
        {self.total_mass_cls_12n}, --self.total_mass_cls_12n
        {self.cnt_mass_over9t_cls_12n}, --self.cnt_mass_over9t_cls_12n
        {self.eal_pervehicle_cls_12n}, --self.eal_pervehicle_cls_12n
        {self.total_eal_cls_12n}, --self.total_eal_cls_12n
        {self.total_eal_over9t_cls_12n}, --self.total_eal_over9t_cls_12n
        {self.total_vehicles_cls_13n}, --self.total_vehicles_cls_13n
        {self.weighed_vehicles_cls_13n}, --self.weighed_vehicles_cls_13n
        {self.perc_truck_dist_cls_13n}, --self.perc_truck_dist_cls_13n
        {self.total_axles_cls_13n}, --self.total_axles_cls_13n
        {self.axles_over9t_cls_13n}, --self.axles_over9t_cls_13n
        {self.total_mass_cls_13n}, --self.total_mass_cls_13n
        {self.cnt_mass_over9t_cls_13n}, --self.cnt_mass_over9t_cls_13n
        {self.eal_pervehicle_cls_13n}, --self.eal_pervehicle_cls_13n
        {self.total_eal_cls_13n}, --self.total_eal_cls_13n
        {self.total_eal_over9t_cls_13n}, --self.total_eal_over9t_cls_13n
        {self.total_vehicles_cls_14n}, --self.total_vehicles_cls_14n
        {self.weighed_vehicles_cls_14n}, --self.weighed_vehicles_cls_14n
        {self.perc_truck_dist_cls_14n}, --self.perc_truck_dist_cls_14n
        {self.total_axles_cls_14n}, --self.total_axles_cls_14n
        {self.axles_over9t_cls_14n}, --self.axles_over9t_cls_14n
        {self.total_mass_cls_14n}, --self.total_mass_cls_14n
        {self.cnt_mass_over9t_cls_14n}, --self.cnt_mass_over9t_cls_14n
        {self.eal_pervehicle_cls_14n}, --self.eal_pervehicle_cls_14n
        {self.total_eal_cls_14n}, --self.total_eal_cls_14n
        {self.total_eal_over9t_cls_14n}, --self.total_eal_over9t_cls_14n
        {self.total_vehicles_cls_15n}, --self.total_vehicles_cls_15n
        {self.weighed_vehicles_cls_15n}, --self.weighed_vehicles_cls_15n
        {self.perc_truck_dist_cls_15n}, --self.perc_truck_dist_cls_15n
        {self.total_axles_cls_15n}, --self.total_axles_cls_15n
        {self.axles_over9t_cls_15n}, --self.axles_over9t_cls_15n
        {self.total_mass_cls_15n}, --self.total_mass_cls_15n
        {self.cnt_mass_over9t_cls_15n}, --self.cnt_mass_over9t_cls_15n
        {self.eal_pervehicle_cls_15n}, --self.eal_pervehicle_cls_15n
        {self.total_eal_cls_15n}, --self.total_eal_cls_15n
        {self.total_eal_over9t_cls_15n}, --self.total_eal_over9t_cls_15n
        {self.total_vehicles_cls_16n}, --self.total_vehicles_cls_16n
        {self.weighed_vehicles_cls_16n}, --self.weighed_vehicles_cls_16n
        {self.perc_truck_dist_cls_16n}, --self.perc_truck_dist_cls_16n
        {self.total_axles_cls_16n}, --self.total_axles_cls_16n
        {self.axles_over9t_cls_16n}, --self.axles_over9t_cls_16n
        {self.total_mass_cls_16n}, --self.total_mass_cls_16n
        {self.cnt_mass_over9t_cls_16n}, --self.cnt_mass_over9t_cls_16n
        {self.eal_pervehicle_cls_16n}, --self.eal_pervehicle_cls_16n
        {self.total_eal_cls_16n}, --self.total_eal_cls_16n
        {self.total_eal_over9t_cls_16n}, --self.total_eal_over9t_cls_16n
        {self.total_vehicles_cls_17n}, --self.total_vehicles_cls_17n
        {self.weighed_vehicles_cls_17n}, --self.weighed_vehicles_cls_17n
        {self.perc_truck_dist_cls_17n}, --self.perc_truck_dist_cls_17n
        {self.total_axles_cls_17n}, --self.total_axles_cls_17n
        {self.axles_over9t_cls_17n}, --self.axles_over9t_cls_17n
        {self.total_mass_cls_17n}, --self.total_mass_cls_17n
        {self.cnt_mass_over9t_cls_17n}, --self.cnt_mass_over9t_cls_17n
        {self.eal_pervehicle_cls_17n}, --self.eal_pervehicle_cls_17n
        {self.total_eal_cls_17n}, --self.total_eal_cls_17n
        {self.total_eal_over9t_cls_17n}) --self.total_eal_over9t_cls_17n
        ON CONFLICT ON CONSTRAINT electronic_count_header_hswim_pkey DO NOTHING 
        -- UPDATE SET 
        -- egrl_percent = COALESCE(EXCLUDED.egrl_percent,egrl_percent),
        -- egrw_percent = COALESCE(EXCLUDED.egrw_percent,egrw_percent),
        -- mean_equivalent_axle_mass = COALESCE(EXCLUDED.mean_equivalent_axle_mass,mean_equivalent_axle_mass),
        -- mean_equivalent_axle_mass_positive_direction = COALESCE(EXCLUDED.mean_equivalent_axle_mass_positive_direction,mean_equivalent_axle_mass_positive_direction),
        -- mean_equivalent_axle_mass_negative_direction = COALESCE(EXCLUDED.mean_equivalent_axle_mass_negative_direction,mean_equivalent_axle_mass_negative_direction),
        -- mean_axle_spacing = COALESCE(EXCLUDED.mean_axle_spacing,mean_axle_spacing),
        -- mean_axle_spacing_positive_direction = COALESCE(EXCLUDED.mean_axle_spacing_positive_direction,mean_axle_spacing_positive_direction),
        -- mean_axle_spacing_negative_direction = COALESCE(EXCLUDED.mean_axle_spacing_negative_direction,mean_axle_spacing_negative_direction),
        -- e80_per_axle = COALESCE(EXCLUDED.e80_per_axle,e80_per_axle),
        -- e80_per_axle_positive_direction = COALESCE(EXCLUDED.e80_per_axle_positive_direction,e80_per_axle_positive_direction),
        -- e80_per_axle_negative_direction = COALESCE(EXCLUDED.e80_per_axle_negative_direction,e80_per_axle_negative_direction),
        -- olhv = COALESCE(EXCLUDED.olhv,olhv),
        -- olhv_positive_direction = COALESCE(EXCLUDED.olhv_positive_direction,olhv_positive_direction),
        -- olhv_negative_direction = COALESCE(EXCLUDED.olhv_negative_direction,olhv_negative_direction),
        -- olhv_percent = COALESCE(EXCLUDED.olhv_percent,olhv_percent),
        -- olhv_percent_positive_direction = COALESCE(EXCLUDED.olhv_percent_positive_direction,olhv_percent_positive_direction),
        -- olhv_percent_negative_direction = COALESCE(EXCLUDED.olhv_percent_negative_direction,olhv_percent_negative_direction),
        -- tonnage_generated = COALESCE(EXCLUDED.tonnage_generated,tonnage_generated),
        -- tonnage_generated_positive_direction = COALESCE(EXCLUDED.tonnage_generated_positive_direction,tonnage_generated_positive_direction),
        -- tonnage_generated_negative_direction = COALESCE(EXCLUDED.tonnage_generated_negative_direction,tonnage_generated_negative_direction),
        -- olton = COALESCE(EXCLUDED.olton,olton),
        -- olton_positive_direction = COALESCE(EXCLUDED.olton_positive_direction,olton_positive_direction),
        -- olton_negative_direction = COALESCE(EXCLUDED.olton_negative_direction,olton_negative_direction),
        -- olton_percent = COALESCE(EXCLUDED.olton_percent,olton_percent),
        -- olton_percent_positive_direction = COALESCE(EXCLUDED.olton_percent_positive_direction,olton_percent_positive_direction),
        -- olton_percent_negative_direction = COALESCE(EXCLUDED.olton_percent_negative_direction,olton_percent_negative_direction),
        -- ole8 = COALESCE(EXCLUDED.ole8,ole8),
        -- ole80_positive_direction = COALESCE(EXCLUDED.ole80_positive_direction,ole80_positive_direction),
        -- ole80_negative_direction = COALESCE(EXCLUDED.ole80_negative_direction,ole80_negative_direction),
        -- ole80_percent = COALESCE(EXCLUDED.ole80_percent,ole80_percent),
        -- ole80_percent_positive_direction = COALESCE(EXCLUDED.ole80_percent_positive_direction,ole80_percent_positive_direction),
        -- ole80_percent_negative_direction = COALESCE(EXCLUDED.ole80_percent_negative_direction,ole80_percent_negative_direction),
        -- xe8 = COALESCE(EXCLUDED.xe8,xe8),
        -- xe80_positive_direction = COALESCE(EXCLUDED.xe80_positive_direction,xe80_positive_direction),
        -- xe80_negative_direction = COALESCE(EXCLUDED.xe80_negative_direction,xe80_negative_direction),
        -- xe80_percent = COALESCE(EXCLUDED.xe80_percent,xe80_percent),
        -- xe80_percent_positive_direction = COALESCE(EXCLUDED.xe80_percent_positive_direction,xe80_percent_positive_direction),
        -- xe80_percent_negative_direction = COALESCE(EXCLUDED.xe80_percent_negative_direction,xe80_percent_negative_direction),
        -- e80_per_day = COALESCE(EXCLUDED.e80_per_day,e80_per_day),
        -- e80_per_day_positive_direction = COALESCE(EXCLUDED.e80_per_day_positive_direction,e80_per_day_positive_direction),
        -- e80_per_day_negative_direction = COALESCE(EXCLUDED.e80_per_day_negative_direction,e80_per_day_negative_direction),
        -- e80_per_heavy_vehicle = COALESCE(EXCLUDED.e80_per_heavy_vehicle,e80_per_heavy_vehicle),
        -- e80_per_heavy_vehicle_positive_direction = COALESCE(EXCLUDED.e80_per_heavy_vehicle_positive_direction,e80_per_heavy_vehicle_positive_direction),
        -- e80_per_heavy_vehicle_negative_direction = COALESCE(EXCLUDED.e80_per_heavy_vehicle_negative_direction,e80_per_heavy_vehicle_negative_direction),
        -- worst_steering_single_axle_cnt = COALESCE(EXCLUDED.worst_steering_single_axle_cnt,worst_steering_single_axle_cnt),
        -- worst_steering_single_axle_olhv_perc = COALESCE(EXCLUDED.worst_steering_single_axle_olhv_perc,worst_steering_single_axle_olhv_perc),
        -- worst_steering_single_axle_tonperhv = COALESCE(EXCLUDED.worst_steering_single_axle_tonperhv,worst_steering_single_axle_tonperhv),
        -- worst_steering_double_axle_cnt = COALESCE(EXCLUDED.worst_steering_double_axle_cnt,worst_steering_double_axle_cnt),
        -- worst_steering_double_axle_olhv_perc = COALESCE(EXCLUDED.worst_steering_double_axle_olhv_perc,worst_steering_double_axle_olhv_perc),
        -- worst_steering_double_axle_tonperhv = COALESCE(EXCLUDED.worst_steering_double_axle_tonperhv,worst_steering_double_axle_tonperhv),
        -- worst_non_steering_single_axle_cnt = COALESCE(EXCLUDED.worst_non_steering_single_axle_cnt,worst_non_steering_single_axle_cnt),
        -- worst_non_steering_single_axle_olhv_perc = COALESCE(EXCLUDED.worst_non_steering_single_axle_olhv_perc,worst_non_steering_single_axle_olhv_perc),
        -- worst_non_steering_single_axle_tonperhv = COALESCE(EXCLUDED.worst_non_steering_single_axle_tonperhv,worst_non_steering_single_axle_tonperhv),
        -- worst_non_steering_double_axle_cnt = COALESCE(EXCLUDED.worst_non_steering_double_axle_cnt,worst_non_steering_double_axle_cnt),
        -- worst_non_steering_double_axle_olhv_perc = COALESCE(EXCLUDED.worst_non_steering_double_axle_olhv_perc,worst_non_steering_double_axle_olhv_perc),
        -- worst_non_steering_double_axle_tonperhv = COALESCE(EXCLUDED.worst_non_steering_double_axle_tonperhv,worst_non_steering_double_axle_tonperhv),
        -- worst_triple_axle_cnt = COALESCE(EXCLUDED.worst_triple_axle_cnt,worst_triple_axle_cnt),
        -- worst_triple_axle_olhv_perc = COALESCE(EXCLUDED.worst_triple_axle_olhv_perc,worst_triple_axle_olhv_perc),
        -- worst_triple_axle_tonperhv = COALESCE(EXCLUDED.worst_triple_axle_tonperhv,worst_triple_axle_tonperhv),
        -- bridge_formula_cnt = COALESCE(EXCLUDED.bridge_formula_cnt,bridge_formula_cnt),
        -- bridge_formula_olhv_perc = COALESCE(EXCLUDED.bridge_formula_olhv_perc,bridge_formula_olhv_perc),
        -- bridge_formula_tonperhv = COALESCE(EXCLUDED.bridge_formula_tonperhv,bridge_formula_tonperhv),
        -- gross_formula_cnt = COALESCE(EXCLUDED.gross_formula_cnt,gross_formula_cnt),
        -- gross_formula_olhv_perc = COALESCE(EXCLUDED.gross_formula_olhv_perc,gross_formula_olhv_perc),
        -- gross_formula_tonperhv = COALESCE(EXCLUDED.gross_formula_tonperhv,gross_formula_tonperhv),
        -- total_avg_cnt = COALESCE(EXCLUDED.total_avg_cnt,total_avg_cnt),
        -- total_avg_olhv_perc = COALESCE(EXCLUDED.total_avg_olhv_perc,total_avg_olhv_perc),
        -- total_avg_tonperhv = COALESCE(EXCLUDED.total_avg_tonperhv,total_avg_tonperhv),
        -- worst_steering_single_axle_cnt_positive_direciton = COALESCE(EXCLUDED.worst_steering_single_axle_cnt_positive_direciton,worst_steering_single_axle_cnt_positive_direciton),
        -- worst_steering_single_axle_olhv_perc_positive_direciton = COALESCE(EXCLUDED.worst_steering_single_axle_olhv_perc_positive_direciton,worst_steering_single_axle_olhv_perc_positive_direciton),
        -- worst_steering_single_axle_tonperhv_positive_direciton = COALESCE(EXCLUDED.worst_steering_single_axle_tonperhv_positive_direciton,worst_steering_single_axle_tonperhv_positive_direciton),
        -- worst_steering_double_axle_cnt_positive_direciton = COALESCE(EXCLUDED.worst_steering_double_axle_cnt_positive_direciton,worst_steering_double_axle_cnt_positive_direciton),
        -- worst_steering_double_axle_olhv_perc_positive_direciton = COALESCE(EXCLUDED.worst_steering_double_axle_olhv_perc_positive_direciton,worst_steering_double_axle_olhv_perc_positive_direciton),
        -- worst_steering_double_axle_tonperhv_positive_direciton = COALESCE(EXCLUDED.worst_steering_double_axle_tonperhv_positive_direciton,worst_steering_double_axle_tonperhv_positive_direciton),
        -- worst_non_steering_single_axle_cnt_positive_direciton = COALESCE(EXCLUDED.worst_non_steering_single_axle_cnt_positive_direciton,worst_non_steering_single_axle_cnt_positive_direciton),
        -- worst_non_steering_single_axle_olhv_perc_positive_direciton = COALESCE(EXCLUDED.worst_non_steering_single_axle_olhv_perc_positive_direciton,worst_non_steering_single_axle_olhv_perc_positive_direciton),
        -- worst_non_steering_single_axle_tonperhv_positive_direciton = COALESCE(EXCLUDED.worst_non_steering_single_axle_tonperhv_positive_direciton,worst_non_steering_single_axle_tonperhv_positive_direciton),
        -- worst_non_steering_double_axle_cnt_positive_direciton = COALESCE(EXCLUDED.worst_non_steering_double_axle_cnt_positive_direciton,worst_non_steering_double_axle_cnt_positive_direciton),
        -- worst_non_steering_double_axle_olhv_perc_positive_direciton = COALESCE(EXCLUDED.worst_non_steering_double_axle_olhv_perc_positive_direciton,worst_non_steering_double_axle_olhv_perc_positive_direciton),
        -- worst_non_steering_double_axle_tonperhv_positive_direciton = COALESCE(EXCLUDED.worst_non_steering_double_axle_tonperhv_positive_direciton,worst_non_steering_double_axle_tonperhv_positive_direciton),
        -- worst_triple_axle_cnt_positive_direciton = COALESCE(EXCLUDED.worst_triple_axle_cnt_positive_direciton,worst_triple_axle_cnt_positive_direciton),
        -- worst_triple_axle_olhv_perc_positive_direciton = COALESCE(EXCLUDED.worst_triple_axle_olhv_perc_positive_direciton,worst_triple_axle_olhv_perc_positive_direciton),
        -- worst_triple_axle_tonperhv_positive_direciton = COALESCE(EXCLUDED.worst_triple_axle_tonperhv_positive_direciton,worst_triple_axle_tonperhv_positive_direciton),
        -- bridge_formula_cnt_positive_direciton = COALESCE(EXCLUDED.bridge_formula_cnt_positive_direciton,bridge_formula_cnt_positive_direciton),
        -- bridge_formula_olhv_perc_positive_direciton = COALESCE(EXCLUDED.bridge_formula_olhv_perc_positive_direciton,bridge_formula_olhv_perc_positive_direciton),
        -- bridge_formula_tonperhv_positive_direciton = COALESCE(EXCLUDED.bridge_formula_tonperhv_positive_direciton,bridge_formula_tonperhv_positive_direciton),
        -- gross_formula_cnt_positive_direciton = COALESCE(EXCLUDED.gross_formula_cnt_positive_direciton,gross_formula_cnt_positive_direciton),
        -- gross_formula_olhv_perc_positive_direciton = COALESCE(EXCLUDED.gross_formula_olhv_perc_positive_direciton,gross_formula_olhv_perc_positive_direciton),
        -- gross_formula_tonperhv_positive_direciton = COALESCE(EXCLUDED.gross_formula_tonperhv_positive_direciton,gross_formula_tonperhv_positive_direciton),
        -- total_avg_cnt_positive_direciton = COALESCE(EXCLUDED.total_avg_cnt_positive_direciton,total_avg_cnt_positive_direciton),
        -- total_avg_olhv_perc_positive_direciton = COALESCE(EXCLUDED.total_avg_olhv_perc_positive_direciton,total_avg_olhv_perc_positive_direciton),
        -- total_avg_tonperhv_positive_direciton = COALESCE(EXCLUDED.total_avg_tonperhv_positive_direciton,total_avg_tonperhv_positive_direciton),
        -- worst_steering_single_axle_cnt_negative_direciton = COALESCE(EXCLUDED.worst_steering_single_axle_cnt_negative_direciton,worst_steering_single_axle_cnt_negative_direciton),
        -- worst_steering_single_axle_olhv_perc_negative_direciton = COALESCE(EXCLUDED.worst_steering_single_axle_olhv_perc_negative_direciton,worst_steering_single_axle_olhv_perc_negative_direciton),
        -- worst_steering_single_axle_tonperhv_negative_direciton = COALESCE(EXCLUDED.worst_steering_single_axle_tonperhv_negative_direciton,worst_steering_single_axle_tonperhv_negative_direciton),
        -- worst_steering_double_axle_cnt_negative_direciton = COALESCE(EXCLUDED.worst_steering_double_axle_cnt_negative_direciton,worst_steering_double_axle_cnt_negative_direciton),
        -- worst_steering_double_axle_olhv_perc_negative_direciton = COALESCE(EXCLUDED.worst_steering_double_axle_olhv_perc_negative_direciton,worst_steering_double_axle_olhv_perc_negative_direciton),
        -- worst_steering_double_axle_tonperhv_negative_direciton = COALESCE(EXCLUDED.worst_steering_double_axle_tonperhv_negative_direciton,worst_steering_double_axle_tonperhv_negative_direciton),
        -- worst_non_steering_single_axle_cnt_negative_direciton = COALESCE(EXCLUDED.worst_non_steering_single_axle_cnt_negative_direciton,worst_non_steering_single_axle_cnt_negative_direciton),
        -- worst_non_steering_single_axle_olhv_perc_negative_direciton = COALESCE(EXCLUDED.worst_non_steering_single_axle_olhv_perc_negative_direciton,worst_non_steering_single_axle_olhv_perc_negative_direciton),
        -- worst_non_steering_single_axle_tonperhv_negative_direciton = COALESCE(EXCLUDED.worst_non_steering_single_axle_tonperhv_negative_direciton,worst_non_steering_single_axle_tonperhv_negative_direciton),
        -- worst_non_steering_double_axle_cnt_negative_direciton = COALESCE(EXCLUDED.worst_non_steering_double_axle_cnt_negative_direciton,worst_non_steering_double_axle_cnt_negative_direciton),
        -- worst_non_steering_double_axle_olhv_perc_negative_direciton = COALESCE(EXCLUDED.worst_non_steering_double_axle_olhv_perc_negative_direciton,worst_non_steering_double_axle_olhv_perc_negative_direciton),
        -- worst_non_steering_double_axle_tonperhv_negative_direciton = COALESCE(EXCLUDED.worst_non_steering_double_axle_tonperhv_negative_direciton,worst_non_steering_double_axle_tonperhv_negative_direciton),
        -- worst_triple_axle_cnt_negative_direciton = COALESCE(EXCLUDED.worst_triple_axle_cnt_negative_direciton,worst_triple_axle_cnt_negative_direciton),
        -- worst_triple_axle_olhv_perc_negative_direciton = COALESCE(EXCLUDED.worst_triple_axle_olhv_perc_negative_direciton,worst_triple_axle_olhv_perc_negative_direciton),
        -- worst_triple_axle_tonperhv_negative_direciton = COALESCE(EXCLUDED.worst_triple_axle_tonperhv_negative_direciton,worst_triple_axle_tonperhv_negative_direciton),
        -- bridge_formula_cnt_negative_direciton = COALESCE(EXCLUDED.bridge_formula_cnt_negative_direciton,bridge_formula_cnt_negative_direciton),
        -- bridge_formula_olhv_perc_negative_direciton = COALESCE(EXCLUDED.bridge_formula_olhv_perc_negative_direciton,bridge_formula_olhv_perc_negative_direciton),
        -- bridge_formula_tonperhv_negative_direciton = COALESCE(EXCLUDED.bridge_formula_tonperhv_negative_direciton,bridge_formula_tonperhv_negative_direciton),
        -- gross_formula_cnt_negative_direciton = COALESCE(EXCLUDED.gross_formula_cnt_negative_direciton,gross_formula_cnt_negative_direciton),
        -- gross_formula_olhv_perc_negative_direciton = COALESCE(EXCLUDED.gross_formula_olhv_perc_negative_direciton,gross_formula_olhv_perc_negative_direciton),
        -- gross_formula_tonperhv_negative_direciton = COALESCE(EXCLUDED.gross_formula_tonperhv_negative_direciton,gross_formula_tonperhv_negative_direciton),
        -- total_avg_cnt_negative_direciton = COALESCE(EXCLUDED.total_avg_cnt_negative_direciton,total_avg_cnt_negative_direciton),
        -- total_avg_olhv_perc_negative_direciton = COALESCE(EXCLUDED.total_avg_olhv_perc_negative_direciton,total_avg_olhv_perc_negative_direciton),
        -- total_avg_tonperhv_negative_direciton = COALESCE(EXCLUDED.total_avg_tonperhv_negative_direciton,total_avg_tonperhv_negative_direciton),
        -- egrl_percent_positive_direction = COALESCE(EXCLUDED.egrl_percent_positive_direction,egrl_percent_positive_direction),
        -- egrl_percent_negative_direction = COALESCE(EXCLUDED.egrl_percent_negative_direction,egrl_percent_negative_direction),
        -- egrw_percent_positive_direction = COALESCE(EXCLUDED.egrw_percent_positive_direction,egrw_percent_positive_direction),
        -- egrw_percent_negative_direction = COALESCE(EXCLUDED.egrw_percent_negative_direction,egrw_percent_negative_direction),
        -- num_weighed = COALESCE(EXCLUDED.num_weighed,num_weighed),
        -- num_weighed_positive_direction = COALESCE(EXCLUDED.num_weighed_positive_direction,num_weighed_positive_direction),
        -- num_weighed_negative_direction = COALESCE(EXCLUDED.num_weighed_negative_direction,num_weighed_negative_direction),
        -- wst_2_axle_busses_cnt_pos_dir = COALESCE(EXCLUDED.wst_2_axle_busses_cnt_pos_dir,wst_2_axle_busses_cnt_pos_dir),
        -- wst_2_axle_6_tyre_single_units_cnt_pos_dir = COALESCE(EXCLUDED.wst_2_axle_6_tyre_single_units_cnt_pos_dir,wst_2_axle_6_tyre_single_units_cnt_pos_dir),
        -- wst_busses_with_3_or_4_axles_cnt_pos_dir = COALESCE(EXCLUDED.wst_busses_with_3_or_4_axles_cnt_pos_dir,wst_busses_with_3_or_4_axles_cnt_pos_dir),
        -- wst_2_axle_6tyre_su_with_trailer_4axles_max_cnt_pos_dir = COALESCE(EXCLUDED.wst_2_axle_6tyre_su_with_trailer_4axles_max_cnt_pos_dir,wst_2_axle_6tyre_su_with_trailer_4axles_max_cnt_pos_dir),
        -- wst_3_axle_su_incl_single_axle_trailer_cnt_pos_dir = COALESCE(EXCLUDED.wst_3_axle_su_incl_single_axle_trailer_cnt_pos_dir,wst_3_axle_su_incl_single_axle_trailer_cnt_pos_dir),
        -- wst_4_or_less_axle_incl_a_single_trailer_cnt_pos_dir = COALESCE(EXCLUDED.wst_4_or_less_axle_incl_a_single_trailer_cnt_pos_dir,wst_4_or_less_axle_incl_a_single_trailer_cnt_pos_dir),
        -- wst_busses_with_5_or_more_axles_cnt_pos_dir = COALESCE(EXCLUDED.wst_busses_with_5_or_more_axles_cnt_pos_dir,wst_busses_with_5_or_more_axles_cnt_pos_dir),
        -- wst_3_axle_su_and_trailer_more_than_4_axles_cnt_pos_dir = COALESCE(EXCLUDED.wst_3_axle_su_and_trailer_more_than_4_axles_cnt_pos_dir,wst_3_axle_su_and_trailer_more_than_4_axles_cnt_pos_dir),
        -- wst_5_axle_single_trailer_cnt_pos_dir = COALESCE(EXCLUDED.wst_5_axle_single_trailer_cnt_pos_dir,wst_5_axle_single_trailer_cnt_pos_dir),
        -- wst_6_axle_single_trailer_cnt_pos_dir = COALESCE(EXCLUDED.wst_6_axle_single_trailer_cnt_pos_dir,wst_6_axle_single_trailer_cnt_pos_dir),
        -- wst_5_or_less_axle_multi_trailer_cnt_pos_dir = COALESCE(EXCLUDED.wst_5_or_less_axle_multi_trailer_cnt_pos_dir,wst_5_or_less_axle_multi_trailer_cnt_pos_dir),
        -- wst_6_axle_multi_trailer_cnt_pos_dir = COALESCE(EXCLUDED.wst_6_axle_multi_trailer_cnt_pos_dir,wst_6_axle_multi_trailer_cnt_pos_dir),
        -- wst_7_axle_multi_trailer_cnt_pos_dir = COALESCE(EXCLUDED.wst_7_axle_multi_trailer_cnt_pos_dir,wst_7_axle_multi_trailer_cnt_pos_dir),
        -- wst_8_or_more_axle_multi_trailer_cnt_pos_dir = COALESCE(EXCLUDED.wst_8_or_more_axle_multi_trailer_cnt_pos_dir,wst_8_or_more_axle_multi_trailer_cnt_pos_dir),
        -- wst_2_axle_busses_olhv_perc_pos_dir = COALESCE(EXCLUDED.wst_2_axle_busses_olhv_perc_pos_dir,wst_2_axle_busses_olhv_perc_pos_dir),
        -- wst_2_axle_6_tyre_single_units_olhv_perc_pos_dir = COALESCE(EXCLUDED.wst_2_axle_6_tyre_single_units_olhv_perc_pos_dir,wst_2_axle_6_tyre_single_units_olhv_perc_pos_dir),
        -- wst_busses_with_3_or_4_axles_olhv_perc_pos_dir = COALESCE(EXCLUDED.wst_busses_with_3_or_4_axles_olhv_perc_pos_dir,wst_busses_with_3_or_4_axles_olhv_perc_pos_dir),
        -- wst_2_axle_6tyre_su_with_trailer_4axles_max_olhv_perc_pos_dir = COALESCE(EXCLUDED.wst_2_axle_6tyre_su_with_trailer_4axles_max_olhv_perc_pos_dir,wst_2_axle_6tyre_su_with_trailer_4axles_max_olhv_perc_pos_dir),
        -- wst_3_axle_su_incl_single_axle_trailer_olhv_perc_pos_dir = COALESCE(EXCLUDED.wst_3_axle_su_incl_single_axle_trailer_olhv_perc_pos_dir,wst_3_axle_su_incl_single_axle_trailer_olhv_perc_pos_dir),
        -- wst_4_or_less_axle_incl_a_single_trailer_olhv_perc_pos_dir = COALESCE(EXCLUDED.wst_4_or_less_axle_incl_a_single_trailer_olhv_perc_pos_dir,wst_4_or_less_axle_incl_a_single_trailer_olhv_perc_pos_dir),
        -- wst_busses_with_5_or_more_axles_olhv_perc_pos_dir = COALESCE(EXCLUDED.wst_busses_with_5_or_more_axles_olhv_perc_pos_dir,wst_busses_with_5_or_more_axles_olhv_perc_pos_dir),
        -- wst_3_axle_su_and_trailer_more_than_4_axles_olhv_perc_pos_dir = COALESCE(EXCLUDED.wst_3_axle_su_and_trailer_more_than_4_axles_olhv_perc_pos_dir,wst_3_axle_su_and_trailer_more_than_4_axles_olhv_perc_pos_dir),
        -- wst_5_axle_single_trailer_olhv_perc_pos_dir = COALESCE(EXCLUDED.wst_5_axle_single_trailer_olhv_perc_pos_dir,wst_5_axle_single_trailer_olhv_perc_pos_dir),
        -- wst_6_axle_single_trailer_olhv_perc_pos_dir = COALESCE(EXCLUDED.wst_6_axle_single_trailer_olhv_perc_pos_dir,wst_6_axle_single_trailer_olhv_perc_pos_dir),
        -- wst_5_or_less_axle_multi_trailer_olhv_perc_pos_dir = COALESCE(EXCLUDED.wst_5_or_less_axle_multi_trailer_olhv_perc_pos_dir,wst_5_or_less_axle_multi_trailer_olhv_perc_pos_dir),
        -- wst_6_axle_multi_trailer_olhv_perc_pos_dir = COALESCE(EXCLUDED.wst_6_axle_multi_trailer_olhv_perc_pos_dir,wst_6_axle_multi_trailer_olhv_perc_pos_dir),
        -- wst_7_axle_multi_trailer_olhv_perc_pos_dir = COALESCE(EXCLUDED.wst_7_axle_multi_trailer_olhv_perc_pos_dir,wst_7_axle_multi_trailer_olhv_perc_pos_dir),
        -- wst_8_or_more_axle_multi_trailer_olhv_perc_pos_dir = COALESCE(EXCLUDED.wst_8_or_more_axle_multi_trailer_olhv_perc_pos_dir,wst_8_or_more_axle_multi_trailer_olhv_perc_pos_dir),
        -- wst_2_axle_busses_tonperhv_pos_dir = COALESCE(EXCLUDED.wst_2_axle_busses_tonperhv_pos_dir,wst_2_axle_busses_tonperhv_pos_dir),
        -- wst_2_axle_6_tyre_single_units_tonperhv_pos_dir = COALESCE(EXCLUDED.wst_2_axle_6_tyre_single_units_tonperhv_pos_dir,wst_2_axle_6_tyre_single_units_tonperhv_pos_dir),
        -- wst_busses_with_3_or_4_axles_tonperhv_pos_dir = COALESCE(EXCLUDED.wst_busses_with_3_or_4_axles_tonperhv_pos_dir,wst_busses_with_3_or_4_axles_tonperhv_pos_dir),
        -- wst_2_axle_6tyre_su_with_trailer_4axles_max_tonperhv_pos_dir = COALESCE(EXCLUDED.wst_2_axle_6tyre_su_with_trailer_4axles_max_tonperhv_pos_dir,wst_2_axle_6tyre_su_with_trailer_4axles_max_tonperhv_pos_dir),
        -- wst_3_axle_su_incl_single_axle_trailer_tonperhv_pos_dir = COALESCE(EXCLUDED.wst_3_axle_su_incl_single_axle_trailer_tonperhv_pos_dir,wst_3_axle_su_incl_single_axle_trailer_tonperhv_pos_dir),
        -- wst_4_or_less_axle_incl_a_single_trailer_tonperhv_pos_dir = COALESCE(EXCLUDED.wst_4_or_less_axle_incl_a_single_trailer_tonperhv_pos_dir,wst_4_or_less_axle_incl_a_single_trailer_tonperhv_pos_dir),
        -- wst_busses_with_5_or_more_axles_tonperhv_pos_dir = COALESCE(EXCLUDED.wst_busses_with_5_or_more_axles_tonperhv_pos_dir,wst_busses_with_5_or_more_axles_tonperhv_pos_dir),
        -- wst_3_axle_su_and_trailer_more_than_4_axles_tonperhv_pos_dir = COALESCE(EXCLUDED.wst_3_axle_su_and_trailer_more_than_4_axles_tonperhv_pos_dir,wst_3_axle_su_and_trailer_more_than_4_axles_tonperhv_pos_dir),
        -- wst_5_axle_single_trailer_tonperhv_pos_dir = COALESCE(EXCLUDED.wst_5_axle_single_trailer_tonperhv_pos_dir,wst_5_axle_single_trailer_tonperhv_pos_dir),
        -- wst_6_axle_single_trailer_tonperhv_pos_dir = COALESCE(EXCLUDED.wst_6_axle_single_trailer_tonperhv_pos_dir,wst_6_axle_single_trailer_tonperhv_pos_dir),
        -- wst_5_or_less_axle_multi_trailer_tonperhv_pos_dir = COALESCE(EXCLUDED.wst_5_or_less_axle_multi_trailer_tonperhv_pos_dir,wst_5_or_less_axle_multi_trailer_tonperhv_pos_dir),
        -- wst_6_axle_multi_trailer_tonperhv_pos_dir = COALESCE(EXCLUDED.wst_6_axle_multi_trailer_tonperhv_pos_dir,wst_6_axle_multi_trailer_tonperhv_pos_dir),
        -- wst_7_axle_multi_trailer_tonperhv_pos_dir = COALESCE(EXCLUDED.wst_7_axle_multi_trailer_tonperhv_pos_dir,wst_7_axle_multi_trailer_tonperhv_pos_dir),
        -- wst_8_or_more_axle_multi_trailer_tonperhv_pos_dir = COALESCE(EXCLUDED.wst_8_or_more_axle_multi_trailer_tonperhv_pos_dir,wst_8_or_more_axle_multi_trailer_tonperhv_pos_dir),
        -- wst_2_axle_busses_cnt_neg_dir = COALESCE(EXCLUDED.wst_2_axle_busses_cnt_neg_dir,wst_2_axle_busses_cnt_neg_dir),
        -- wst_2_axle_6_tyre_single_units_cnt_neg_dir = COALESCE(EXCLUDED.wst_2_axle_6_tyre_single_units_cnt_neg_dir,wst_2_axle_6_tyre_single_units_cnt_neg_dir),
        -- wst_busses_with_3_or_4_axles_cnt_neg_dir = COALESCE(EXCLUDED.wst_busses_with_3_or_4_axles_cnt_neg_dir,wst_busses_with_3_or_4_axles_cnt_neg_dir),
        -- wst_2_axle_6tyre_su_with_trailer_4axles_max_cnt_neg_dir = COALESCE(EXCLUDED.wst_2_axle_6tyre_su_with_trailer_4axles_max_cnt_neg_dir,wst_2_axle_6tyre_su_with_trailer_4axles_max_cnt_neg_dir),
        -- wst_3_axle_su_incl_single_axle_trailer_cnt_neg_dir = COALESCE(EXCLUDED.wst_3_axle_su_incl_single_axle_trailer_cnt_neg_dir,wst_3_axle_su_incl_single_axle_trailer_cnt_neg_dir),
        -- wst_4_or_less_axle_incl_a_single_trailer_cnt_neg_dir = COALESCE(EXCLUDED.wst_4_or_less_axle_incl_a_single_trailer_cnt_neg_dir,wst_4_or_less_axle_incl_a_single_trailer_cnt_neg_dir),
        -- wst_busses_with_5_or_more_axles_cnt_neg_dir = COALESCE(EXCLUDED.wst_busses_with_5_or_more_axles_cnt_neg_dir,wst_busses_with_5_or_more_axles_cnt_neg_dir),
        -- wst_3_axle_su_and_trailer_more_than_4_axles_cnt_neg_dir = COALESCE(EXCLUDED.wst_3_axle_su_and_trailer_more_than_4_axles_cnt_neg_dir,wst_3_axle_su_and_trailer_more_than_4_axles_cnt_neg_dir),
        -- wst_5_axle_single_trailer_cnt_neg_dir = COALESCE(EXCLUDED.wst_5_axle_single_trailer_cnt_neg_dir,wst_5_axle_single_trailer_cnt_neg_dir),
        -- wst_6_axle_single_trailer_cnt_neg_dir = COALESCE(EXCLUDED.wst_6_axle_single_trailer_cnt_neg_dir,wst_6_axle_single_trailer_cnt_neg_dir),
        -- wst_5_or_less_axle_multi_trailer_cnt_neg_dir = COALESCE(EXCLUDED.wst_5_or_less_axle_multi_trailer_cnt_neg_dir,wst_5_or_less_axle_multi_trailer_cnt_neg_dir),
        -- wst_6_axle_multi_trailer_cnt_neg_dir = COALESCE(EXCLUDED.wst_6_axle_multi_trailer_cnt_neg_dir,wst_6_axle_multi_trailer_cnt_neg_dir),
        -- wst_7_axle_multi_trailer_cnt_neg_dir = COALESCE(EXCLUDED.wst_7_axle_multi_trailer_cnt_neg_dir,wst_7_axle_multi_trailer_cnt_neg_dir),
        -- wst_8_or_more_axle_multi_trailer_cnt_neg_dir = COALESCE(EXCLUDED.wst_8_or_more_axle_multi_trailer_cnt_neg_dir,wst_8_or_more_axle_multi_trailer_cnt_neg_dir),
        -- wst_2_axle_busses_olhv_perc_neg_dir = COALESCE(EXCLUDED.wst_2_axle_busses_olhv_perc_neg_dir,wst_2_axle_busses_olhv_perc_neg_dir),
        -- wst_2_axle_6_tyre_single_units_olhv_perc_neg_dir = COALESCE(EXCLUDED.wst_2_axle_6_tyre_single_units_olhv_perc_neg_dir,wst_2_axle_6_tyre_single_units_olhv_perc_neg_dir),
        -- wst_busses_with_3_or_4_axles_olhv_perc_neg_dir = COALESCE(EXCLUDED.wst_busses_with_3_or_4_axles_olhv_perc_neg_dir,wst_busses_with_3_or_4_axles_olhv_perc_neg_dir),
        -- wst_2_axle_6tyre_su_with_trailer_4axles_max_olhv_perc_neg_dir = COALESCE(EXCLUDED.wst_2_axle_6tyre_su_with_trailer_4axles_max_olhv_perc_neg_dir,wst_2_axle_6tyre_su_with_trailer_4axles_max_olhv_perc_neg_dir),
        -- wst_3_axle_su_incl_single_axle_trailer_olhv_perc_neg_dir = COALESCE(EXCLUDED.wst_3_axle_su_incl_single_axle_trailer_olhv_perc_neg_dir,wst_3_axle_su_incl_single_axle_trailer_olhv_perc_neg_dir),
        -- wst_4_or_less_axle_incl_a_single_trailer_olhv_perc_neg_dir = COALESCE(EXCLUDED.wst_4_or_less_axle_incl_a_single_trailer_olhv_perc_neg_dir,wst_4_or_less_axle_incl_a_single_trailer_olhv_perc_neg_dir),
        -- wst_busses_with_5_or_more_axles_olhv_perc_neg_dir = COALESCE(EXCLUDED.wst_busses_with_5_or_more_axles_olhv_perc_neg_dir,wst_busses_with_5_or_more_axles_olhv_perc_neg_dir),
        -- wst_3_axle_su_and_trailer_more_than_4_axles_olhv_perc_neg_dir = COALESCE(EXCLUDED.wst_3_axle_su_and_trailer_more_than_4_axles_olhv_perc_neg_dir,wst_3_axle_su_and_trailer_more_than_4_axles_olhv_perc_neg_dir),
        -- wst_5_axle_single_trailer_olhv_perc_neg_dir = COALESCE(EXCLUDED.wst_5_axle_single_trailer_olhv_perc_neg_dir,wst_5_axle_single_trailer_olhv_perc_neg_dir),
        -- wst_6_axle_single_trailer_olhv_perc_neg_dir = COALESCE(EXCLUDED.wst_6_axle_single_trailer_olhv_perc_neg_dir,wst_6_axle_single_trailer_olhv_perc_neg_dir),
        -- wst_5_or_less_axle_multi_trailer_olhv_perc_neg_dir = COALESCE(EXCLUDED.wst_5_or_less_axle_multi_trailer_olhv_perc_neg_dir,wst_5_or_less_axle_multi_trailer_olhv_perc_neg_dir),
        -- wst_6_axle_multi_trailer_olhv_perc_neg_dir = COALESCE(EXCLUDED.wst_6_axle_multi_trailer_olhv_perc_neg_dir,wst_6_axle_multi_trailer_olhv_perc_neg_dir),
        -- wst_7_axle_multi_trailer_olhv_perc_neg_dir = COALESCE(EXCLUDED.wst_7_axle_multi_trailer_olhv_perc_neg_dir,wst_7_axle_multi_trailer_olhv_perc_neg_dir),
        -- wst_8_or_more_axle_multi_trailer_olhv_perc_neg_dir = COALESCE(EXCLUDED.wst_8_or_more_axle_multi_trailer_olhv_perc_neg_dir,wst_8_or_more_axle_multi_trailer_olhv_perc_neg_dir),
        -- wst_2_axle_busses_tonperhv_neg_dir = COALESCE(EXCLUDED.wst_2_axle_busses_tonperhv_neg_dir,wst_2_axle_busses_tonperhv_neg_dir),
        -- wst_2_axle_6_tyre_single_units_tonperhv_neg_dir = COALESCE(EXCLUDED.wst_2_axle_6_tyre_single_units_tonperhv_neg_dir,wst_2_axle_6_tyre_single_units_tonperhv_neg_dir),
        -- wst_busses_with_3_or_4_axles_tonperhv_neg_dir = COALESCE(EXCLUDED.wst_busses_with_3_or_4_axles_tonperhv_neg_dir,wst_busses_with_3_or_4_axles_tonperhv_neg_dir),
        -- wst_2_axle_6tyre_su_with_trailer_4axles_max_tonperhv_neg_dir = COALESCE(EXCLUDED.wst_2_axle_6tyre_su_with_trailer_4axles_max_tonperhv_neg_dir,wst_2_axle_6tyre_su_with_trailer_4axles_max_tonperhv_neg_dir),
        -- wst_3_axle_su_incl_single_axle_trailer_tonperhv_neg_dir = COALESCE(EXCLUDED.wst_3_axle_su_incl_single_axle_trailer_tonperhv_neg_dir,wst_3_axle_su_incl_single_axle_trailer_tonperhv_neg_dir),
        -- wst_4_or_less_axle_incl_a_single_trailer_tonperhv_neg_dir = COALESCE(EXCLUDED.wst_4_or_less_axle_incl_a_single_trailer_tonperhv_neg_dir,wst_4_or_less_axle_incl_a_single_trailer_tonperhv_neg_dir),
        -- wst_busses_with_5_or_more_axles_tonperhv_neg_dir = COALESCE(EXCLUDED.wst_busses_with_5_or_more_axles_tonperhv_neg_dir,wst_busses_with_5_or_more_axles_tonperhv_neg_dir),
        -- wst_3_axle_su_and_trailer_more_than_4_axles_tonperhv_neg_dir = COALESCE(EXCLUDED.wst_3_axle_su_and_trailer_more_than_4_axles_tonperhv_neg_dir,wst_3_axle_su_and_trailer_more_than_4_axles_tonperhv_neg_dir),
        -- wst_5_axle_single_trailer_tonperhv_neg_dir = COALESCE(EXCLUDED.wst_5_axle_single_trailer_tonperhv_neg_dir,wst_5_axle_single_trailer_tonperhv_neg_dir),
        -- wst_6_axle_single_trailer_tonperhv_neg_dir = COALESCE(EXCLUDED.wst_6_axle_single_trailer_tonperhv_neg_dir,wst_6_axle_single_trailer_tonperhv_neg_dir),
        -- wst_5_or_less_axle_multi_trailer_tonperhv_neg_dir = COALESCE(EXCLUDED.wst_5_or_less_axle_multi_trailer_tonperhv_neg_dir,wst_5_or_less_axle_multi_trailer_tonperhv_neg_dir),
        -- wst_6_axle_multi_trailer_tonperhv_neg_dir = COALESCE(EXCLUDED.wst_6_axle_multi_trailer_tonperhv_neg_dir,wst_6_axle_multi_trailer_tonperhv_neg_dir),
        -- wst_7_axle_multi_trailer_tonperhv_neg_dir = COALESCE(EXCLUDED.wst_7_axle_multi_trailer_tonperhv_neg_dir,wst_7_axle_multi_trailer_tonperhv_neg_dir),
        -- wst_8_or_more_axle_multi_trailer_tonperhv_neg_dir = COALESCE(EXCLUDED.wst_8_or_more_axle_multi_trailer_tonperhv_neg_dir,wst_8_or_more_axle_multi_trailer_tonperhv_neg_dir),
        -- wst_2_axle_busses_cnt = COALESCE(EXCLUDED.wst_2_axle_busses_cnt,wst_2_axle_busses_cnt),
        -- wst_2_axle_6_tyre_single_units_cnt = COALESCE(EXCLUDED.wst_2_axle_6_tyre_single_units_cnt,wst_2_axle_6_tyre_single_units_cnt),
        -- wst_busses_with_3_or_4_axles_cnt = COALESCE(EXCLUDED.wst_busses_with_3_or_4_axles_cnt,wst_busses_with_3_or_4_axles_cnt),
        -- wst_2_axle_6tyre_su_with_trailer_4axles_max_cnt = COALESCE(EXCLUDED.wst_2_axle_6tyre_su_with_trailer_4axles_max_cnt,wst_2_axle_6tyre_su_with_trailer_4axles_max_cnt),
        -- wst_3_axle_su_incl_single_axle_trailer_cnt = COALESCE(EXCLUDED.wst_3_axle_su_incl_single_axle_trailer_cnt,wst_3_axle_su_incl_single_axle_trailer_cnt),
        -- wst_4_or_less_axle_incl_a_single_trailer_cnt = COALESCE(EXCLUDED.wst_4_or_less_axle_incl_a_single_trailer_cnt,wst_4_or_less_axle_incl_a_single_trailer_cnt),
        -- wst_busses_with_5_or_more_axles_cnt = COALESCE(EXCLUDED.wst_busses_with_5_or_more_axles_cnt,wst_busses_with_5_or_more_axles_cnt),
        -- wst_3_axle_su_and_trailer_more_than_4_axles_cnt = COALESCE(EXCLUDED.wst_3_axle_su_and_trailer_more_than_4_axles_cnt,wst_3_axle_su_and_trailer_more_than_4_axles_cnt),
        -- wst_5_axle_single_trailer_cnt = COALESCE(EXCLUDED.wst_5_axle_single_trailer_cnt,wst_5_axle_single_trailer_cnt),
        -- wst_6_axle_single_trailer_cnt = COALESCE(EXCLUDED.wst_6_axle_single_trailer_cnt,wst_6_axle_single_trailer_cnt),
        -- wst_5_or_less_axle_multi_trailer_cnt = COALESCE(EXCLUDED.wst_5_or_less_axle_multi_trailer_cnt,wst_5_or_less_axle_multi_trailer_cnt),
        -- wst_6_axle_multi_trailer_cnt = COALESCE(EXCLUDED.wst_6_axle_multi_trailer_cnt,wst_6_axle_multi_trailer_cnt),
        -- wst_7_axle_multi_trailer_cnt = COALESCE(EXCLUDED.wst_7_axle_multi_trailer_cnt,wst_7_axle_multi_trailer_cnt),
        -- wst_8_or_more_axle_multi_trailer_cnt = COALESCE(EXCLUDED.wst_8_or_more_axle_multi_trailer_cnt,wst_8_or_more_axle_multi_trailer_cnt),
        -- wst_2_axle_busses_olhv_perc = COALESCE(EXCLUDED.wst_2_axle_busses_olhv_perc,wst_2_axle_busses_olhv_perc),
        -- wst_2_axle_6_tyre_single_units_olhv_perc = COALESCE(EXCLUDED.wst_2_axle_6_tyre_single_units_olhv_perc,wst_2_axle_6_tyre_single_units_olhv_perc),
        -- wst_busses_with_3_or_4_axles_olhv_perc = COALESCE(EXCLUDED.wst_busses_with_3_or_4_axles_olhv_perc,wst_busses_with_3_or_4_axles_olhv_perc),
        -- wst_2_axle_6tyre_su_with_trailer_4axles_max_olhv_perc = COALESCE(EXCLUDED.wst_2_axle_6tyre_su_with_trailer_4axles_max_olhv_perc,wst_2_axle_6tyre_su_with_trailer_4axles_max_olhv_perc),
        -- wst_3_axle_su_incl_single_axle_trailer_olhv_perc = COALESCE(EXCLUDED.wst_3_axle_su_incl_single_axle_trailer_olhv_perc,wst_3_axle_su_incl_single_axle_trailer_olhv_perc),
        -- wst_4_or_less_axle_incl_a_single_trailer_olhv_perc = COALESCE(EXCLUDED.wst_4_or_less_axle_incl_a_single_trailer_olhv_perc,wst_4_or_less_axle_incl_a_single_trailer_olhv_perc),
        -- wst_busses_with_5_or_more_axles_olhv_perc = COALESCE(EXCLUDED.wst_busses_with_5_or_more_axles_olhv_perc,wst_busses_with_5_or_more_axles_olhv_perc),
        -- wst_3_axle_su_and_trailer_more_than_4_axles_olhv_perc = COALESCE(EXCLUDED.wst_3_axle_su_and_trailer_more_than_4_axles_olhv_perc,wst_3_axle_su_and_trailer_more_than_4_axles_olhv_perc),
        -- wst_5_axle_single_trailer_olhv_perc = COALESCE(EXCLUDED.wst_5_axle_single_trailer_olhv_perc,wst_5_axle_single_trailer_olhv_perc),
        -- wst_6_axle_single_trailer_olhv_perc = COALESCE(EXCLUDED.wst_6_axle_single_trailer_olhv_perc,wst_6_axle_single_trailer_olhv_perc),
        -- wst_5_or_less_axle_multi_trailer_olhv_perc = COALESCE(EXCLUDED.wst_5_or_less_axle_multi_trailer_olhv_perc,wst_5_or_less_axle_multi_trailer_olhv_perc),
        -- wst_6_axle_multi_trailer_olhv_perc = COALESCE(EXCLUDED.wst_6_axle_multi_trailer_olhv_perc,wst_6_axle_multi_trailer_olhv_perc),
        -- wst_7_axle_multi_trailer_olhv_perc = COALESCE(EXCLUDED.wst_7_axle_multi_trailer_olhv_perc,wst_7_axle_multi_trailer_olhv_perc),
        -- wst_8_or_more_axle_multi_trailer_olhv_perc = COALESCE(EXCLUDED.wst_8_or_more_axle_multi_trailer_olhv_perc,wst_8_or_more_axle_multi_trailer_olhv_perc),
        -- wst_2_axle_busses_tonperhv = COALESCE(EXCLUDED.wst_2_axle_busses_tonperhv,wst_2_axle_busses_tonperhv),
        -- wst_2_axle_6_tyre_single_units_tonperhv = COALESCE(EXCLUDED.wst_2_axle_6_tyre_single_units_tonperhv,wst_2_axle_6_tyre_single_units_tonperhv),
        -- wst_busses_with_3_or_4_axles_tonperhv = COALESCE(EXCLUDED.wst_busses_with_3_or_4_axles_tonperhv,wst_busses_with_3_or_4_axles_tonperhv),
        -- wst_2_axle_6tyre_su_with_trailer_4axles_max_tonperhv = COALESCE(EXCLUDED.wst_2_axle_6tyre_su_with_trailer_4axles_max_tonperhv,wst_2_axle_6tyre_su_with_trailer_4axles_max_tonperhv),
        -- wst_3_axle_su_incl_single_axle_trailer_tonperhv = COALESCE(EXCLUDED.wst_3_axle_su_incl_single_axle_trailer_tonperhv,wst_3_axle_su_incl_single_axle_trailer_tonperhv),
        -- wst_4_or_less_axle_incl_a_single_trailer_tonperhv = COALESCE(EXCLUDED.wst_4_or_less_axle_incl_a_single_trailer_tonperhv,wst_4_or_less_axle_incl_a_single_trailer_tonperhv),
        -- wst_busses_with_5_or_more_axles_tonperhv = COALESCE(EXCLUDED.wst_busses_with_5_or_more_axles_tonperhv,wst_busses_with_5_or_more_axles_tonperhv),
        -- wst_3_axle_su_and_trailer_more_than_4_axles_tonperhv = COALESCE(EXCLUDED.wst_3_axle_su_and_trailer_more_than_4_axles_tonperhv,wst_3_axle_su_and_trailer_more_than_4_axles_tonperhv),
        -- wst_5_axle_single_trailer_tonperhv = COALESCE(EXCLUDED.wst_5_axle_single_trailer_tonperhv,wst_5_axle_single_trailer_tonperhv),
        -- wst_6_axle_single_trailer_tonperhv = COALESCE(EXCLUDED.wst_6_axle_single_trailer_tonperhv,wst_6_axle_single_trailer_tonperhv),
        -- wst_5_or_less_axle_multi_trailer_tonperhv = COALESCE(EXCLUDED.wst_5_or_less_axle_multi_trailer_tonperhv,wst_5_or_less_axle_multi_trailer_tonperhv),
        -- wst_6_axle_multi_trailer_tonperhv = COALESCE(EXCLUDED.wst_6_axle_multi_trailer_tonperhv,wst_6_axle_multi_trailer_tonperhv),
        -- wst_7_axle_multi_trailer_tonperhv = COALESCE(EXCLUDED.wst_7_axle_multi_trailer_tonperhv,wst_7_axle_multi_trailer_tonperhv),
        -- wst_8_or_more_axle_multi_trailer_tonperhv = COALESCE(EXCLUDED.wst_8_or_more_axle_multi_trailer_tonperhv,wst_8_or_more_axle_multi_trailer_tonperhv)
        ;
    """

    UPSERT_STRING = UPSERT_STRING.replace("nan,", "0,")
    return UPSERT_STRING

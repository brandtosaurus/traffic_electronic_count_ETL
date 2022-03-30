import sqlalchemy as sa

#### DB CONNECTION

ENGINE_URL = sa.engine.url.URL(
    "postgresql",
    username="postgres",
    password="Lin3@r1in3!431",
    host="linearline.dedicated.co.za",
    port=5432,
    database="gauteng",
)

# ENGINE_URL = sa.engine.url.URL(
#     "postgresql",
#     username="postgres",
#     password="Lin3@r1in3!431",
#     host="localhost",
#     port=5432,
#     database="gauteng",
# )

ENGINE = sa.create_engine(
    ENGINE_URL
)

OUTPUT_FILE = r"~\Desktop\Temp\rsa_traffic_counts\TEMP_E_COUNT_"

FILES_COMPLETE = r"~\Desktop\Temp\rsa_traffic_counts\RSA_FILES_COMPLETE_localhost.csv"

PATH = r"~\Desktop\Temp\rsa_traffic_counts"

PROBLEM_FILES = r"~\Desktop\Temp\rsa_traffic_counts\RSA_COUNT_PROBLEM_FILES.csv"

DATA_COLUMN_NAMES = [
    "site_id",
    "header_id",
    '"year"',
    "start_datetime",
    "end_datetime",
    "duration_min",
    "direction",
    "forward_direction_code",
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
    "rear_to_rear_headways_shorter_than_programmed_time",
    "total_light_vehicles_type21",
    "total_heavy_vehicles_type21",
    "total_vehicles_type21",
    "unknown_vehicle_error_class",
    "motorcycle",
    "light_motor_vehicles",
    "light_motor_vehicles_towing",
    "heavy_vehicle",
    "two_axle_busses",
    "two_axle_6_tyre_single_units",
    "busses_with_3_or_4_axles",
    "two_axle_6_tyre_single_unit_with_light_trailer_4_axles_max",
    "three_axle_single_unit_including_single_axle_light_trailer",
    "four_or_less_axle_including_a_single_trailer",
    "buses_with_5_or_more_axles",
    "three_axle_single_unit_and_light_trailer_more_than_4_axles",
    "five_axle_single_trailer",
    "six_axle_single_trailer",
    "five_or_less_axle_multi_trailer",
    "six_axle_multi_trailer",
    "seven_axle_multi_trailer",
    "eight_or_more_axle_multi_trailer",
    "total_light_vehicles_type30",
    "total_heavy_vehicles_type30",
    "total_vehicles_type30",
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
    "physical_lane_number",
    "forward_1_or_reverse_code_2",
    "vehicle_category",
    "vehicle_class_code_primary_scheme",
    "vehicle_class_code_secondary_scheme",
    "vehicle_speed",
    "vehicle_length",
    "site_occupancy_time_in_milliseconds",
    "chassis_height_code",
    "vehicle_following_code",
    "vehicle_tag_code",
    "trailer_count",
    "axle_count",
    "bumper_to_1st_axle_spacing",
    "sub_data_type_code_axle_spacing",
    "number_of_axles_spacings_counted",
    "axle_spacing_1_between_individual_axles_cm",
    "axle_spacing_2_between_individual_axles_cm",
    "axle_spacing_3_between_individual_axles_cm",
    "axle_spacing_4_between_individual_axles_cm",
    "axle_spacing_5_between_individual_axles_cm",
    "axle_spacing_6_between_individual_axles_cm",
    "axle_spacing_7_between_individual_axles_cm",
    "axle_spacing_8_between_individual_axles_cm",
]

HEADER_COLUMN_NAMES = [
    "header_id",
    "site_id",
    "station_name",
    "x",
    "y",
    "start_datetime",
    "end_datetime",
    "number_of_lanes",
    "type_21_count_interval_minutes",
    "type_21_programmable_rear_to_rear_headway_bin",
    "type_21_program_id",
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
    "type_10_vehicle_classification_scheme_primary",
    "type_10_vehicle_classification_scheme_secondary",
    "type_10_maximum_gap_milliseconds",
    "type_10_maximum_differential_speed",
    "type_30_summary_interval_minutes",
    "type_30_vehicle_classification_scheme",
    "type_70_summary_interval_minutes",
    "type_70_vehicle_classification_scheme",
    "type_70_maximum_gap_milliseconds",
    "type_70_maximum_differential_speed",
    "type_70_error_bin_code",
    "instrumentation_description",
    "document_url",
    "date_processed",
    "growth_rate_use",
    "total_light_positive_direction",
    "total_light_negative_direction",
    "total_light_vehicles",
    "total_heavy_positive_direction",
    "total_heavy_negative_direction",
    "total_heavy_vehicles",
    "total_short_heavy_positive_direction",
    "total_short_heavy_negative_direction",
    "total_short_heavy_vehicles",
    "total_medium_heavy_positive_direction",
    "total_medium_heavy_negative_direction",
    "total_medium_heavy_vehicles",
    "total_long_heavy_positive_direction",
    "total_long_heavy_negative_direction",
    "total_long_heavy_vehicles",
    "total_vehicles_positive_direction",
    "total_vehicles_negative_direction",
    "total_vehicles",
    "average_speed_positive_direction",
    "average_speed_negative_direction",
    "average_speed",
    "average_speed_light_vehicles_positive_direction",
    "average_speed_light_vehicles_negative_direction",
    "average_speed_light_vehicles",
    "average_speed_heavy_vehicles_positive_direction",
    "average_speed_heavy_vehicles_negative_direction",
    "average_speed_heavy_vehicles",
    "truck_split_positive_direction",
    "truck_split_negative_direction",
    "truck_split_total",
    "estimated_axles_per_truck_positive_direction",
    "estimated_axles_per_truck_negative_direction",
    "estimated_axles_per_truck_total",
    "percentage_speeding_positive_direction",
    "percentage_speeding_negative_direction",
    "percentage_speeding_total",
    "vehicles_with_rear_to_rear_headway_less_than_2sec_positive_dire",
    "vehicles_with_rear_to_rear_headway_less_than_2sec_negative_dire",
    "vehicles_with_rear_to_rear_headway_less_than_2sec_total",
    "estimated_e80_positive_direction",
    "estimated_e80_negative_direction",
    "estimated_e80_on_road",
    "adt_positive_direction",
    "adt_negative_direction",
    "adt_total",
    "adtt_positive_direction",
    "adtt_negative_direction",
    "adtt_total",
    "highest_volume_per_hour_positive_direction",
    "highest_volume_per_hour_negative_direction",
    "highest_volume_per_hour_total",
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
]

TYPE10_DATA_COLUMN_NAMES = [
'id', 
'site_id', 
'header_id', 
"year", 
'number_of_fields_associated_with_the_basic_vehicle_data', 
'data_source_code', 
'edit_code', 
'departure_date', 
'departure_time', 
'assigned_lane_number', 
'physical_lane_number', 
'forward_reverse_code', 
'vehicle_category', 
'vehicle_class_code_primary_scheme', 
'vehicle_class_code_secondary_scheme', 
'vehicle_speed', 
'vehicle_length', 
'site_occupancy_time_in_milliseconds', 
'chassis_height_code', 
'vehicle_following_code', 
'vehicle_tag_code', 
'trailer_count', 
'axle_count', 
'bumper_to_1st_axle_spacing', 
'tyre_type', 
'sub_data_type_code_vx', 
'vehicle_registration_number', 
'number_of_images', 
'image_name_1', 
'image_name_2', 
'image_name_3', 
'sub_data_type_code_sx', 
'number_of_axles_spacings_counted', 
'axle_spacing_1_between_individual_axles_cm', 
'axle_spacing_2_between_individual_axles_cm', 
'axle_spacing_3_between_individual_axles_cm', 
'axle_spacing_4_between_individual_axles_cm', 
'axle_spacing_5_between_individual_axles_cm', 
'axle_spacing_6_between_individual_axles_cm', 
'axle_spacing_7_between_individual_axles_cm', 
'axle_spacing_8_between_individual_axles_cm',
'axle_spacing_9_between_individual_axles_cm',
'axle_spacing_10_between_individual_axles_cm' ,
'axle_spacing_11_between_individual_axles_cm' ,
'axle_spacing_12_between_individual_axles_cm' ,
'axle_spacing_13_between_individual_axles_cm' ,
'axle_spacing_14_between_individual_axles_cm' ,
'axle_spacing_15_between_individual_axles_cm' ,
'axle_spacing_16_between_individual_axles_cm' ,
'axle_spacing_17_between_individual_axles_cm' ,
'axle_spacing_18_between_individual_axles_cm' ,
'axle_spacing_19_between_individual_axles_cm' ,
'axle_spacing_20_between_individual_axles_cm' ,
'sub_data_type_code_wx',
'number_of_wheel_masses',
'offset_sensor_detesction_code',
'mass_measurement_resolution'  ,
'wheel_mass_for_wheel_1',
'wheel_mass_for_wheel_2',
'wheel_mass_for_wheel_3',
'wheel_mass_for_wheel_4',
'wheel_mass_for_wheel_5',
'wheel_mass_for_wheel_6',
'wheel_mass_for_wheel_7',
'wheel_mass_for_wheel_8',
'wheel_mass_for_wheel_9',
'wheel_mass_for_wheel_10',
'wheel_mass_for_wheel_11',
'wheel_mass_for_wheel_12',
'wheel_mass_for_wheel_13',
'wheel_mass_for_wheel_14',
'wheel_mass_for_wheel_15',
'wheel_mass_for_wheel_16',
'wheel_mass_for_wheel_17',
'wheel_mass_for_wheel_18',
'wheel_mass_for_wheel_19',
'wheel_mass_for_wheel_20'
]

TYPE10_HEADER_COLUMN_NAMES = [
    'header_id', 
    'data_description', 
    'vehicle_classification_scheme_primary', 
    'vehicle_classification_scheme_secondary', 
    'maximum_gap_milliseconds', 
    'maximum_differential_speed'
]

CREATE_PARTITIONED_ELECTRONIC_DATA_TABLE = """
CREATE TABLE IF NOT EXISTS trafc.electronic_count_data_partitioned (
	id int8 NOT NULL GENERATED BY DEFAULT AS IDENTITY,
	site_id text NOT NULL,
	header_id uuid NULL,
	"year" int4 NULL,
	start_datetime timestamp NOT NULL,
	end_datetime timestamp NULL,
	duration_min int4 NOT NULL,
	direction varchar(2) NOT NULL,
	forward_direction_code int2 NULL,
	lane_number int4 NOT NULL,
	speedbin0 int4 NULL,
	speedbin1 int4 NULL,
	speedbin2 int4 NULL,
	speedbin3 int4 NULL,
	speedbin4 int4 NULL,
	speedbin5 int4 NULL,
	speedbin6 int4 NULL,
	speedbin7 int4 NULL,
	speedbin8 int4 NULL,
	speedbin9 int4 NULL,
	speedbin10 int4 NULL,
	sum_of_heavy_vehicle_speeds int4 NULL,
	short_heavy_vehicles int4 NULL,
	medium_heavy_vehicles int4 NULL,
	long_heavy_vehicles int4 NULL,
	rear_to_rear_headway_shorter_than_2_seconds int4 NULL,
	rear_to_rear_headways_shorter_than_programmed_time int4 NULL,
	total_light_vehicles_type21 int4 NULL,
	total_heavy_vehicles_type21 int4 NULL,
	total_vehicles_type21 int4 NULL,
	unknown_vehicle_error_class int4 NULL,
	motorcycle int4 NULL,
	light_motor_vehicles int4 NULL,
	light_motor_vehicles_towing int4 NULL,
	heavy_vehicle int4 NULL,
	two_axle_busses int4 NULL,
	two_axle_6_tyre_single_units int4 NULL,
	busses_with_3_or_4_axles int4 NULL,
	two_axle_6_tyre_single_unit_with_light_trailer_4_axles_max int4 NULL,
	three_axle_single_unit_including_single_axle_light_trailer int4 NULL,
	four_or_less_axle_including_a_single_trailer int4 NULL,
	buses_with_5_or_more_axles int4 NULL,
	three_axle_single_unit_and_light_trailer_more_than_4_axles int4 NULL,
	five_axle_single_trailer int4 NULL,
	six_axle_single_trailer int4 NULL,
	five_or_less_axle_multi_trailer int4 NULL,
	six_axle_multi_trailer int4 NULL,
	seven_axle_multi_trailer int4 NULL,
	eight_or_more_axle_multi_trailer int4 NULL,
	total_light_vehicles_type30 int4 NULL,
	total_heavy_vehicles_type30 int4 NULL,
	total_vehicles_type30 int4 NULL,
	number_of_error_vehicles int4 NULL,
	total_free_flowing_light_vehicles int4 NULL,
	total_following_light_vehicles int4 NULL,
	total_free_flowing_heavy_vehicles int4 NULL,
	total_following_heavy_vehicles int4 NULL,
	sum_of_inverse_of_speeds_for_free_flowing_lights int4 NULL,
	sum_of_inverse_of_speeds_for_following_lights int4 NULL,
	sum_of_inverse_of_speeds_for_free_flowing_heavies int4 NULL,
	sum_of_inverse_of_speeds_for_following_heavies int4 NULL,
	sum_of_speeds_for_free_flowing_lights int4 NULL,
	sum_of_speeds_for_following_lights int4 NULL,
	sum_of_speeds_for_free_flowing_heavies int4 NULL,
	sum_of_speeds_for_following_heavies int4 NULL,
	sum_of_squared_speeds_of_free_flowing_lights int4 NULL,
	sum_of_squared_speeds_for_following_lights int4 NULL,
	sum_of_squared_speeds_of_free_flowing_heavies int4 NULL,
	sum_of_squared_speeds_for_following_heavies int4 NULL
)
PARTITION BY RANGE (start_datetime);
"""

CREATE_TYPE10_DATA_TABLE_QRY = """
CREATE TABLE IF NOT EXISTS trafc.electronic_count_data_type_10 (
	id uuid NOT NULL default uuid_generate_v4(),
	site_id text NOT NULL,
	header_id uuid NULL,
	"year" int4 NULL,
    start_datetime timestamp NOT NULL,
	direction varchar NOT NULL,
	forward_direction_code varchar NULL,
	number_of_fields_associated_with_the_basic_vehicle_data int4 NULL,
	data_source_code int4 NULL,
	edit_code int4 NULL,
	departure_date date NOT NULL,
	departure_time time NULL,
	assigned_lane_number int4 NOT NULL,
	physical_lane_number int4 NULL,
	forward_reverse_code int2 NULL,
	vehicle_category int4 NULL,
	vehicle_class_code_primary_scheme int4 NULL,
	vehicle_class_code_secondary_scheme int4 NULL,
	vehicle_speed int4 NULL,
	vehicle_length int4 NULL,
	site_occupancy_time_in_milliseconds int4 NULL,
	chassis_height_code int4 NULL,
	vehicle_following_code int4 NULL,
	vehicle_tag_code int4 NULL,
	trailer_count int4 NULL,
	axle_count int4 NULL,
	bumper_to_1st_axle_spacing numeric NULL,
	tyre_type int4 NULL,
	sub_data_type_code_vx text NULL,
	vehicle_registration_number text NULL
	CONSTRAINT electronic_count_data_type_10_pk PRIMARY KEY (id)
)
CREATE UNIQUE INDEX electronic_count_data_type_10_id_idx ON trafc.electronic_count_data_type_10 USING btree (id);"""

CREATE_AXLE_SPACING_TABLE = """
CREATE TABLE IF NOT EXISTS trafc.electronic_count_data_type_10_axle_spacing (
    id int8 not null primary key generated by default as identity,
    data_id uuid not null,
    sub_data_type_code text,
    number_of_axles_spacings_counted int4,
    axle_spacing_number int4,
    axle_spacing_between_individual_axles_cm int4
);
create index axle_spacing_data_id_idx ON trafc.electronic_count_data_type_10_axle_spacing using btree (data_id);
"""

CREATE_WHEEL_MASS_TABLE = """
CREATE TABLE IF NOT EXISTS trafc.electronic_count_data_type_10_wheel_mass (
    id int8 not null primary key generated by default as identity,
    data_id uuid not null,
    sub_data_type_code text,
    number_of_wheel_masses int4,
    offset_sensor_detesction_code varchar NULL,
	mass_measurement_resolution varchar NULL,
    wheel_number int4,
    wheel_mass int4
);
create index wheel_mass_data_id_idx ON trafc.electronic_count_data_type_10_wheel_mass using btree (data_id);
"""

CREATE_IMAGES_TABLE = """
CREATE TABLE IF NOT EXISTS trafc.electronic_count_data_type_10_images (
    id int8 not null primary key generated by default as identity,
    data_id uuid not null,
    number_of_images text,
    image_name text
);
create index type10_images_data_id_idx ON trafc.electronic_count_data_type_10_images using btree (data_id);
"""

CREATE_TYPE10_HEADER_TABLE_QRY = """
CREATE TABLE IF NOT EXISTS trafc.electronic_count_header_type_10 (
	header_id uuid NOT NULL,
	data_description int4 NULL,
	vehicle_classification_scheme_primary int4 NULL,
	vehicle_classification_scheme_secondary int4 NULL,
	maximum_gap_milliseconds int4 NULL,
	maximum_differential_speed int4 NULL,
	CONSTRAINT electronic_count_header_type_10_pk PRIMARY KEY (header_id)
);
"""

FUNCTION_CREATEPARTITIONIFNOTEXISTS ="""
CREATE OR REPLACE FUNCTION trafc.createpartitionifnotexists(start_datetime date)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
declare yearStart date := date_trunc('year', start_datetime);
    declare yearEndExclusive date := date_trunc('year', start_datetime) + interval '1 year';
    -- We infer the name of the table from the date that it should contain
    -- E.g. a date in June 2005 should be int the table mytable_200506:
    declare tableName text := 'trafc.electronic_count_data_' || to_char(start_datetime, 'YYYY');
begin
    -- Check if the table we need for the supplied date exists.
    -- If it does not exist...:
    if to_regclass(tableName) is null then
		-- Generate a new table that acts as a partition for mytable:
		execute format('create table if not exists %I partition of trafc.electronic_count_data for values from (%L) to (%L)', tableName, yearStart, yearEndExclusive);
		-- Unfortunatelly Postgres forces us to define index for each table individually:
		execute format('create unique index on %I (site_id,start_datetime,lane_number)', tableName);
		   -- Generate indexes to speed up queries.
		execute format('create index on  %I (site_id, start_datetime, direction)', tableName);
        execute format('create index on  %I (site_id, year, direction)', tableName);
		execute format('create index on  %I (header_id)', tableName);
    end if;
end;
$function$
;
"""

CREATE_DATA_VIEW = """
CREATE VIEW IF NOT EXISTS trafc.v_electronic_count_data_partitioned
AS SELECT *  FROM trafc.electronic_count_data_partitioned ecdp;
   """

RULE_AUTOCALL_FUNCTION = """
CREATE RULE IF NOT EXISTS autocall_createpartitionifnotexists AS
    ON INSERT TO trafc.v_electronic_count_data_partitioned DO INSTEAD ( SELECT trafc.createpartitionifnotexists((new.start_datetime)::date) AS createpartitionifnotexists;
 INSERT INTO trafc.electronic_count_data_partitioned (site_id, header_id, year, start_datetime, end_datetime, duration_min, direction, forward_direction_code, lane_number, speedbin0, speedbin1, speedbin2, speedbin3, speedbin4, speedbin5, speedbin6, speedbin7, speedbin8, speedbin9, speedbin10, sum_of_heavy_vehicle_speeds, short_heavy_vehicles, medium_heavy_vehicles, long_heavy_vehicles, rear_to_rear_headway_shorter_than_2_seconds, rear_to_rear_headways_shorter_than_programmed_time, total_light_vehicles_type21, total_heavy_vehicles_type21, total_vehicles_type21, unknown_vehicle_error_class, motorcycle, light_motor_vehicles, light_motor_vehicles_towing, heavy_vehicle, two_axle_busses, two_axle_6_tyre_single_units, busses_with_3_or_4_axles, two_axle_6_tyre_single_unit_with_light_trailer_4_axles_max, three_axle_single_unit_including_single_axle_light_trailer, four_or_less_axle_including_a_single_trailer, buses_with_5_or_more_axles, three_axle_single_unit_and_light_trailer_more_than_4_axles, five_axle_single_trailer, six_axle_single_trailer, five_or_less_axle_multi_trailer, six_axle_multi_trailer, seven_axle_multi_trailer, eight_or_more_axle_multi_trailer, total_light_vehicles_type30, total_heavy_vehicles_type30, total_vehicles_type30, number_of_error_vehicles, total_free_flowing_light_vehicles, total_following_light_vehicles, total_free_flowing_heavy_vehicles, total_following_heavy_vehicles, sum_of_inverse_of_speeds_for_free_flowing_lights, sum_of_inverse_of_speeds_for_following_lights, sum_of_inverse_of_speeds_for_free_flowing_heavies, sum_of_inverse_of_speeds_for_following_heavies, sum_of_speeds_for_free_flowing_lights, sum_of_speeds_for_following_lights, sum_of_speeds_for_free_flowing_heavies, sum_of_speeds_for_following_heavies, sum_of_squared_speeds_of_free_flowing_lights, sum_of_squared_speeds_for_following_lights, sum_of_squared_speeds_of_free_flowing_heavies, sum_of_squared_speeds_for_following_heavies, physical_lane_number, forward_1_or_reverse_code_2, vehicle_category, vehicle_class_code_primary_scheme, vehicle_class_code_secondary_scheme, vehicle_speed, vehicle_length, site_occupancy_time_in_milliseconds, chassis_height_code, vehicle_following_code, vehicle_tag_code, trailer_count, axle_count, bumper_to_1st_axle_spacing, sub_data_type_code_axle_spacing, number_of_axles_spacings_counted, axle_spacing_1_between_individual_axles_cm, axle_spacing_2_between_individual_axles_cm, axle_spacing_3_between_individual_axles_cm, axle_spacing_4_between_individual_axles_cm, axle_spacing_5_between_individual_axles_cm, axle_spacing_6_between_individual_axles_cm, axle_spacing_7_between_individual_axles_cm, axle_spacing_8_between_individual_axles_cm)
  VALUES (new.site_id, new.header_id, new.year, new.start_datetime, new.end_datetime, new.duration_min, new.direction, new.forward_direction_code, new.lane_number, new.speedbin0, new.speedbin1, new.speedbin2, new.speedbin3, new.speedbin4, new.speedbin5, new.speedbin6, new.speedbin7, new.speedbin8, new.speedbin9, new.speedbin10, new.sum_of_heavy_vehicle_speeds, new.short_heavy_vehicles, new.medium_heavy_vehicles, new.long_heavy_vehicles, new.rear_to_rear_headway_shorter_than_2_seconds, new.rear_to_rear_headways_shorter_than_programmed_time, new.total_light_vehicles_type21, new.total_heavy_vehicles_type21, new.total_vehicles_type21, new.unknown_vehicle_error_class, new.motorcycle, new.light_motor_vehicles, new.light_motor_vehicles_towing, new.heavy_vehicle, new.two_axle_busses, new.two_axle_6_tyre_single_units, new.busses_with_3_or_4_axles, new.two_axle_6_tyre_single_unit_with_light_trailer_4_axles_max, new.three_axle_single_unit_including_single_axle_light_trailer, new.four_or_less_axle_including_a_single_trailer, new.buses_with_5_or_more_axles, new.three_axle_single_unit_and_light_trailer_more_than_4_axles, new.five_axle_single_trailer, new.six_axle_single_trailer, new.five_or_less_axle_multi_trailer, new.six_axle_multi_trailer, new.seven_axle_multi_trailer, new.eight_or_more_axle_multi_trailer, new.total_light_vehicles_type30, new.total_heavy_vehicles_type30, new.total_vehicles_type30, new.number_of_error_vehicles, new.total_free_flowing_light_vehicles, new.total_following_light_vehicles, new.total_free_flowing_heavy_vehicles, new.total_following_heavy_vehicles, new.sum_of_inverse_of_speeds_for_free_flowing_lights, new.sum_of_inverse_of_speeds_for_following_lights, new.sum_of_inverse_of_speeds_for_free_flowing_heavies, new.sum_of_inverse_of_speeds_for_following_heavies, new.sum_of_speeds_for_free_flowing_lights, new.sum_of_speeds_for_following_lights, new.sum_of_speeds_for_free_flowing_heavies, new.sum_of_speeds_for_following_heavies, new.sum_of_squared_speeds_of_free_flowing_lights, new.sum_of_squared_speeds_for_following_lights, new.sum_of_squared_speeds_of_free_flowing_heavies, new.sum_of_squared_speeds_for_following_heavies, new.physical_lane_number, new.forward_1_or_reverse_code_2, new.vehicle_category, new.vehicle_class_code_primary_scheme, new.vehicle_class_code_secondary_scheme, new.vehicle_speed, new.vehicle_length, new.site_occupancy_time_in_milliseconds, new.chassis_height_code, new.vehicle_following_code, new.vehicle_tag_code, new.trailer_count, new.axle_count, new.bumper_to_1st_axle_spacing, new.sub_data_type_code_axle_spacing, new.number_of_axles_spacings_counted, new.axle_spacing_1_between_individual_axles_cm, new.axle_spacing_2_between_individual_axles_cm, new.axle_spacing_3_between_individual_axles_cm, new.axle_spacing_4_between_individual_axles_cm, new.axle_spacing_5_between_individual_axles_cm, new.axle_spacing_6_between_individual_axles_cm, new.axle_spacing_7_between_individual_axles_cm, new.axle_spacing_8_between_individual_axles_cm);
);
"""

CREATE_HEADER_VIEW = """
CREATE VIEW IF NOT EXISTS trafc.v_electronic_count_header_import
AS SELECT *  FROM trafc.electronic_count_header echn;
"""

RULE_AUTOCALL_IMPORT_HEADER = """
CREATE RULE IF NOT EXISTS autocall_importheaderdata AS
    ON INSERT TO trafc.v_electronic_count_header_import DO INSTEAD  INSERT INTO trafc.electronic_count_header (site_id, station_name, x, y, start_datetime, end_datetime, number_of_lanes, type_21_count_interval_minutes, type_21_programmable_rear_to_rear_headway_bin, type_21_program_id, speedbin1, speedbin2, speedbin3, speedbin4, speedbin5, speedbin6, speedbin7, speedbin8, speedbin9, speedbin10, type_10_vehicle_classification_scheme_primary, type_10_vehicle_classification_scheme_secondary, type_10_maximum_gap_milliseconds, type_10_maximum_differential_speed, type_30_summary_interval_minutes, type_30_vehicle_classification_scheme, type_70_summary_interval_minutes, type_70_vehicle_classification_scheme, type_70_maximum_gap_milliseconds, type_70_maximum_differential_speed, type_70_error_bin_code, header_id, instrumentation_description, document_url, date_processed, growth_rate_use)
  VALUES (new.site_id, new.station_name, new.x, new.y, new.start_datetime, new.end_datetime, new.number_of_lanes, new.type_21_count_interval_minutes, new.type_21_programmable_rear_to_rear_headway_bin, new.type_21_program_id, new.speedbin1, new.speedbin2, new.speedbin3, new.speedbin4, new.speedbin5, new.speedbin6, new.speedbin7, new.speedbin8, new.speedbin9, new.speedbin10, new.type_10_vehicle_classification_scheme_primary, new.type_10_vehicle_classification_scheme_secondary, new.type_10_maximum_gap_milliseconds, new.type_10_maximum_differential_speed, new.type_30_summary_interval_minutes, new.type_30_vehicle_classification_scheme, new.type_70_summary_interval_minutes, new.type_70_vehicle_classification_scheme, new.type_70_maximum_gap_milliseconds, new.type_70_maximum_differential_speed, new.type_70_error_bin_code, new.header_id, new.instrumentation_description, new.document_url, new.date_processed, new.growth_rate_use);
"""
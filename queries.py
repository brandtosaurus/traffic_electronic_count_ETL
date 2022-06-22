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

CREATE_ELECTRONIC_HEADER_TABLE = """
CREATE TABLE IF NOT EXISTS trafc.electronic_count_header (
	id int8 NOT NULL GENERATED BY DEFAULT AS IDENTITY,
	header_id uuid NOT NULL,
	site_id text NOT NULL,
	station_name text NULL,
	x text NULL,
	y text NULL,
	start_datetime timestamp NOT NULL,
	end_datetime timestamp NOT NULL,
	number_of_lanes int4 NULL,
	type_21_count_interval_minutes int4 NULL,
	type_21_programmable_rear_to_rear_headway_bin int4 NULL,
	type_21_program_id int4 NULL,
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
	type_10_vehicle_classification_scheme_primary int4 NULL,
	type_10_vehicle_classification_scheme_secondary int4 NULL,
	type_10_maximum_gap_milliseconds int4 NULL,
	type_10_maximum_differential_speed int4 NULL,
	type_30_summary_interval_minutes int4 NULL,
	type_30_vehicle_classification_scheme int4 NULL,
	type_70_summary_interval_minutes int4 NULL,
	type_70_vehicle_classification_scheme int4 NULL,
	type_70_maximum_gap_milliseconds int4 NULL,
	type_70_maximum_differential_speed int4 NULL,
	type_70_error_bin_code int4 NULL,
	instrumentation_description text NULL,
	document_url text NULL,
	date_processed date NULL,
	growth_rate_use text NULL,
	total_light_positive_direction int4 NULL,
	total_light_negative_direction int4 NULL,
	total_light_vehicles int4 NULL,
	total_heavy_positive_direction int4 NULL,
	total_heavy_negative_direction int4 NULL,
	total_heavy_vehicles int4 NULL,
	total_short_heavy_positive_direction int4 NULL,
	total_short_heavy_negative_direction int4 NULL,
	total_short_heavy_vehicles int4 NULL,
	total_medium_heavy_positive_direction int4 NULL,
	total_medium_heavy_negative_direction int4 NULL,
	total_medium_heavy_vehicles int4 NULL,
	total_long_heavy_positive_direction int4 NULL,
	total_long_heavy_negative_direction int4 NULL,
	total_long_heavy_vehicles int4 NULL,
	total_vehicles_positive_direction int4 NULL,
	total_vehicles_negative_direction int4 NULL,
	total_vehicles int4 NULL,
	average_speed_positive_direction numeric NULL,
	average_speed_negative_direction numeric NULL,
	average_speed numeric NULL,
	average_speed_light_vehicles_positive_direction numeric NULL,
	average_speed_light_vehicles_negative_direction numeric NULL,
	average_speed_light_vehicles numeric NULL,
	average_speed_heavy_vehicles_positive_direction numeric NULL,
	average_speed_heavy_vehicles_negative_direction numeric NULL,
	average_speed_heavy_vehicles numeric NULL,
	truck_split_positive_direction varchar NULL,
	truck_split_negative_direction varchar NULL,
	truck_split_total varchar NULL,
	estimated_axles_per_truck_positive_direction int4 NULL,
	estimated_axles_per_truck_negative_direction int4 NULL,
	estimated_axles_per_truck_total int4 NULL,
	percentage_speeding_positive_direction numeric NULL,
	percentage_speeding_negative_direction numeric NULL,
	percentage_speeding_total numeric NULL,
	vehicles_with_rear_to_rear_headway_less_than_2sec_positive_dire int4 NULL,
	vehicles_with_rear_to_rear_headway_less_than_2sec_negative_dire int4 NULL,
	vehicles_with_rear_to_rear_headway_less_than_2sec_total int4 NULL,
	estimated_e80_positive_direction numeric NULL,
	estimated_e80_negative_direction numeric NULL,
	estimated_e80_on_road numeric NULL,
	adt_positive_direction int4 NULL,
	adt_negative_direction int4 NULL,
	adt_total int4 NULL,
	adtt_positive_direction int4 NULL,
	adtt_negative_direction int4 NULL,
	adtt_total int4 NULL,
	highest_volume_per_hour_positive_direciton int4 NULL,
	highest_volume_per_hour_negative_direciton int4 NULL,
	highest_volume_per_hour_total int4 NULL,
	"15th_highest_volume_per_hour_positive_direction" int4 NULL,
	"15th_highest_volume_per_hour_negative_direction" int4 NULL,
	"15th_highest_volume_per_hour_total" int4 NULL,
	"30th_highest_volume_per_hour_positive_direction" int4 NULL,
	"30th_highest_volume_per_hour_negative_direction" int4 NULL,
	"30th_highest_volume_per_hour_total" int4 NULL,
	"15th_percentile_speed_positive_direction" numeric NULL,
	"15th_percentile_speed_negative_direction" numeric NULL,
	"15th_percentile_speed_total" numeric NULL,
	"85th_percentile_speed_positive_direction" numeric NULL,
	"85th_percentile_speed_negative_direction" numeric NULL,
	"85th_percentile_speed_total" numeric NULL,
	"year" int4 NULL
);
CREATE INDEX electronic_count_header_header_id_idx ON trafc.electronic_count_header USING btree (header_id);
CREATE INDEX electronic_count_header_id_idx ON trafc.electronic_count_header USING btree (id);
CREATE INDEX electronic_count_header_site_id_idx ON trafc.electronic_count_header USING btree (site_id);
CREATE UNIQUE INDEX electronic_count_header_un ON trafc.electronic_count_header USING btree (site_id, start_datetime, end_datetime);
CREATE INDEX electronic_count_header_year_idx ON trafc.electronic_count_header USING btree (year);
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

CREATE_TYPE10_DATA_TABLE_QRY = """
CREATE TABLE IF NOT EXISTS trafc.electronic_count_data_type10 (
	data_id uuid NOT NULL default uuid_generate_v4(),
	site_id text NOT NULL,
	header_id uuid NULL,
	"year" int4 NULL,
	start_datetime timestamp not null,
	direction varchar not null,
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
	tyre_type int4 NULL
	CONSTRAINT electronic_count_data_type_10_pk PRIMARY KEY (id)
)
CREATE UNIQUE INDEX electronic_count_data_type10_id_idx ON trafc.electronic_count_data_type10 USING btree (data_id);"""

CREATE_AXLE_SPACING_TABLE_SX = """
CREATE TABLE IF NOT EXISTS trafc.electronic_count_data_type_10_axle_spacing (
    id int8 not null primary key generated by default as identity,
    data_id uuid not null,
    sub_data_type_code text,
    axle_spacing_number int4,
    axle_spacing_between_individual_axles_cm int4
);
create index axle_spacing_data_id_idx ON trafc.electronic_count_data_type_10_axle_spacing using btree (data_id);
"""

CREATE_AXLE_GROUP_CONFIG_TABLE_CX = """
CREATE TABLE IF NOT EXISTS trafc.traffic_e_type10_axle_group_configuration (
	id int8 NOT NULL GENERATED ALWAYS AS IDENTITY,
	type10_id uuid NOT NULL,
	sub_data_type_code varchar NULL,
	axle_group_number int2 NULL,
	group_axle_count int2 NULL,
	CONSTRAINT traffic_e_type10_axle_group_configuration_pk PRIMARY KEY (id)
);
CREATE INDEX traffic_e_type10_axle_group_configuration_type10_id_idx ON trafc.traffic_e_type10_axle_group_configuration USING btree (type10_id);"""

CREATE_AXLE_GROUP_MASS_TABLE_GX = """
CREATE TABLE IF NOT EXISTS trafc.traffic_e_type10_axle_group_mass (
	id int8 NOT NULL GENERATED ALWAYS AS IDENTITY,
	type10_id uuid NOT NULL,
	sub_data_type_code varchar NULL,
	offset_sensor_detection_code varchar NULL,
	mass_measurement_resolution_kg float4 NULL,
	axle_group_mass_number int2 NULL,
	axle_group_mass float4 NULL,
	CONSTRAINT traffic_e_type10_axle_group_mass_pk PRIMARY KEY (id)
);
CREATE INDEX traffic_e_type10_axle_group_mass_type10_id_idx ON trafc.traffic_e_type10_axle_group_mass USING btree (type10_id);"""

CREATE_AXLE_MASS_TABLE_AX = """
CREATE TABLE IF NOT EXISTS trafc.traffic_e_type10_axle_mass (
	id int8 NOT NULL GENERATED ALWAYS AS IDENTITY,
	type10_id uuid NOT NULL,
	sub_data_type_code varchar NULL,
	offset_sensor_detection_code varchar NULL,
	mass_measurement_resolution_kg float4 NULL,
	axle_mass_number int2 NULL,
	axle_mass float4 NULL,
	CONSTRAINT traffic_e_type10_axle_mass_pk PRIMARY KEY (id)
);
CREATE INDEX traffic_e_type10_axle_mass_type10_id_idx ON trafc.traffic_e_type10_axle_mass USING btree (type10_id);"""

CREATE_AXLE_SPACING_TABLE_SX = """
CREATE TABLE IF NOT EXISTS trafc.traffic_e_type10_axle_spacing (
	id int8 NOT NULL GENERATED ALWAYS AS IDENTITY,
	type10_id uuid NOT NULL,
	sub_data_type_code varchar NULL,
	axle_spacing_number int2 NULL,
	axle_spacing_cm float4 NULL,
	CONSTRAINT traffic_e_type10_axle_spacing_pk PRIMARY KEY (id)
);
CREATE INDEX traffic_e_type10_axle_spacing_type10_id_idx ON trafc.traffic_e_type10_axle_spacing USING btree (type10_id);"""

CREATE_IMAGES_TABLE_VX = """
CREATE TABLE IF NOT EXISTS trafc.traffic_e_type10_identification_data_images (
	id int8 NOT NULL GENERATED ALWAYS AS IDENTITY,
	type10_id uuid NOT NULL,
	sub_data_type_code varchar NULL,
	vehicle_registration_number text NULL,
	image_number int2 NULL,
	image_name text NULL,
	CONSTRAINT traffic_e_type10_identification_data_images_pk PRIMARY KEY (id)
);
CREATE INDEX traffic_e_type10_identification_data_images_type10_id_idx ON trafc.traffic_e_type10_identification_data_images USING btree (type10_id);"""

CREATE_TYRE_TABLE_TX = """
REATE TABLE IF NOT EXISTS trafc.traffic_e_type10_tyre (
	id int8 NOT NULL GENERATED ALWAYS AS IDENTITY,
	type10_id uuid NOT NULL,
	sub_data_type_code varchar NULL,
	tyre_number int2 NULL,
	tyre_code float4 NULL,
	CONSTRAINT traffic_e_type10_tyre_pk PRIMARY KEY (id)
);
CREATE INDEX traffic_e_type10_tyre_type10_id_idx ON trafc.traffic_e_type10_tyre USING btree (type10_id);"""

CREATE_WHEEL_MASS_TABLE_WX = """
CREATE TABLE IF NOT EXISTS trafc.traffic_e_type10_wheel_mass (
	id int8 NOT NULL GENERATED ALWAYS AS IDENTITY,
	type10_id uuid NOT NULL,
	sub_data_type_code varchar NULL,
	offset_sensor_detection_code varchar NULL,
	mass_measurement_resolution_kg float4 NULL,
	wheel_mass_number int2 NULL,
	wheel_mass float4 NULL,
	CONSTRAINT traffic_e_type10_wheel_mass_pk PRIMARY KEY (id)
);
CREATE INDEX traffic_e_type10_wheel_mass_type10_id_idx ON trafc.traffic_e_type10_wheel_mass USING btree (type10_id);"""

ADD_COLUMNS_TO_HEADER = """
alter table trafc.electronic_count_header add column mean_equivalent_axle_mass numeric;
alter table trafc.electronic_count_header add column mean_equivalent_axle_mass_positive_direction numeric;
alter table trafc.electronic_count_header add column mean_equivalent_axle_mass_negative_direction numeric;
alter table trafc.electronic_count_header add column mean_axle_spacing numeric;
alter table trafc.electronic_count_header add column mean_axle_spacing_positive_direction numeric;
alter table trafc.electronic_count_header add column mean_axle_spacing_negative_direction numeric;
alter table trafc.electronic_count_header add column e80_per_axle numeric;
alter table trafc.electronic_count_header add column e80_per_axle_positive_direction numeric;
alter table trafc.electronic_count_header add column e80_per_axle_negative_direction numeric;
alter table trafc.electronic_count_header add column olhv numeric;
alter table trafc.electronic_count_header add column olhv_positive_direction numeric;
alter table trafc.electronic_count_header add column olhv_negative_direction numeric;
alter table trafc.electronic_count_header add column olhv_percent numeric;
alter table trafc.electronic_count_header add column olhv_percent_positive_direction numeric;
alter table trafc.electronic_count_header add column olhv_percent_negative_direction numeric;
alter table trafc.electronic_count_header add column tonnage_generated numeric;
alter table trafc.electronic_count_header add column tonnage_generated_positive_direction numeric;
alter table trafc.electronic_count_header add column tonnage_generated_negative_direction numeric;
alter table trafc.electronic_count_header add column olton numeric;
alter table trafc.electronic_count_header add column olton_positive_direction numeric;
alter table trafc.electronic_count_header add column olton_negative_direction numeric;
alter table trafc.electronic_count_header add column olton_percent numeric;
alter table trafc.electronic_count_header add column olton_percent_positive_direction numeric;
alter table trafc.electronic_count_header add column olton_percent_negative_direction numeric;
alter table trafc.electronic_count_header add column ole80 numeric;
alter table trafc.electronic_count_header add column ole80_positive_direction numeric;
alter table trafc.electronic_count_header add column ole80_negative_direction numeric;
alter table trafc.electronic_count_header add column ole80_percent numeric;
alter table trafc.electronic_count_header add column ole80_percent_positive_direction numeric;
alter table trafc.electronic_count_header add column ole80_percent_negative_direction numeric;
alter table trafc.electronic_count_header add column xe80 numeric;
alter table trafc.electronic_count_header add column xe80_positive_direction numeric;
alter table trafc.electronic_count_header add column xe80_negative_direction numeric;
alter table trafc.electronic_count_header add column xe80_percent numeric;
alter table trafc.electronic_count_header add column xe80_percent_positive_direction numeric;
alter table trafc.electronic_count_header add column xe80_percent_negative_direction numeric;
alter table trafc.electronic_count_header add column e80_per_day numeric;
alter table trafc.electronic_count_header add column e80_per_day_positive_direction numeric;
alter table trafc.electronic_count_header add column e80_per_day_negative_direction numeric;
alter table trafc.electronic_count_header add column e80_per_heavy_vehicle numeric;
alter table trafc.electronic_count_header add column e80_per_heavy_vehicle_positive_direction numeric;
alter table trafc.electronic_count_header add column e80_per_heavy_vehicle_negative_direction numeric;
alter table trafc.electronic_count_header add column worst_steering_single_axle_cnt numeric;
alter table trafc.electronic_count_header add column worst_steering_single_axle_olhv_perc numeric;
alter table trafc.electronic_count_header add column worst_steering_single_axle_tonperhv numeric;
alter table trafc.electronic_count_header add column worst_steering_double_axle_cnt numeric;
alter table trafc.electronic_count_header add column worst_steering_double_axle_olhv_perc numeric;
alter table trafc.electronic_count_header add column worst_steering_double_axle_tonperhv numeric;
alter table trafc.electronic_count_header add column worst_non_steering_single_axle_cnt numeric;
alter table trafc.electronic_count_header add column worst_non_steering_single_axle_olhv_perc numeric;
alter table trafc.electronic_count_header add column worst_non_steering_single_axle_tonperhv numeric;
alter table trafc.electronic_count_header add column worst_non_steering_double_axle_cnt numeric;
alter table trafc.electronic_count_header add column worst_non_steering_double_axle_olhv_perc numeric;
alter table trafc.electronic_count_header add column worst_non_steering_double_axle_tonperhv numeric;
alter table trafc.electronic_count_header add column worst_triple_axle_cnt numeric;
alter table trafc.electronic_count_header add column worst_triple_axle_olhv_perc numeric;
alter table trafc.electronic_count_header add column worst_triple_axle_tonperhv numeric;
alter table trafc.electronic_count_header add column bridge_formula_cnt numeric;
alter table trafc.electronic_count_header add column bridge_formula_olhv_perc numeric;
alter table trafc.electronic_count_header add column bridge_formula_tonperhv numeric;
alter table trafc.electronic_count_header add column gross_formula_cnt numeric;
alter table trafc.electronic_count_header add column gross_formula_olhv_perc numeric;
alter table trafc.electronic_count_header add column gross_formula_tonperhv numeric;
alter table trafc.electronic_count_header add column total_avg_cnt numeric;
alter table trafc.electronic_count_header add column total_avg_olhv_perc numeric;
alter table trafc.electronic_count_header add column total_avg_tonperhv numeric;
alter table trafc.electronic_count_header add column worst_steering_single_axle_cnt_positive_direciton numeric;
alter table trafc.electronic_count_header add column worst_steering_single_axle_olhv_perc_positive_direciton numeric;
alter table trafc.electronic_count_header add column worst_steering_single_axle_tonperhv_positive_direciton numeric;
alter table trafc.electronic_count_header add column worst_steering_double_axle_cnt_positive_direciton numeric;
alter table trafc.electronic_count_header add column worst_steering_double_axle_olhv_perc_positive_direciton numeric;
alter table trafc.electronic_count_header add column worst_steering_double_axle_tonperhv_positive_direciton numeric;
alter table trafc.electronic_count_header add column worst_non_steering_single_axle_cnt_positive_direciton numeric;
alter table trafc.electronic_count_header add column worst_non_steering_single_axle_olhv_perc_positive_direciton numeric;
alter table trafc.electronic_count_header add column worst_non_steering_single_axle_tonperhv_positive_direciton numeric;
alter table trafc.electronic_count_header add column worst_non_steering_double_axle_cnt_positive_direciton numeric;
alter table trafc.electronic_count_header add column worst_non_steering_double_axle_olhv_perc_positive_direciton numeric;
alter table trafc.electronic_count_header add column worst_non_steering_double_axle_tonperhv_positive_direciton numeric;
alter table trafc.electronic_count_header add column worst_triple_axle_cnt_positive_direciton numeric;
alter table trafc.electronic_count_header add column worst_triple_axle_olhv_perc_positive_direciton numeric;
alter table trafc.electronic_count_header add column worst_triple_axle_tonperhv_positive_direciton numeric;
alter table trafc.electronic_count_header add column bridge_formula_cnt_positive_direciton numeric;
alter table trafc.electronic_count_header add column bridge_formula_olhv_perc_positive_direciton numeric;
alter table trafc.electronic_count_header add column bridge_formula_tonperhv_positive_direciton numeric;
alter table trafc.electronic_count_header add column gross_formula_cnt_positive_direciton numeric;
alter table trafc.electronic_count_header add column gross_formula_olhv_perc_positive_direciton numeric;
alter table trafc.electronic_count_header add column gross_formula_tonperhv_positive_direciton numeric;
alter table trafc.electronic_count_header add column total_avg_cnt_positive_direciton numeric;
alter table trafc.electronic_count_header add column total_avg_olhv_perc_positive_direciton numeric;
alter table trafc.electronic_count_header add column total_avg_tonperhv_positive_direciton numeric;
alter table trafc.electronic_count_header add column worst_steering_single_axle_cnt_negative_direciton numeric;
alter table trafc.electronic_count_header add column worst_steering_single_axle_olhv_perc_negative_direciton numeric;
alter table trafc.electronic_count_header add column worst_steering_single_axle_tonperhv_negative_direciton numeric;
alter table trafc.electronic_count_header add column worst_steering_double_axle_cnt_negative_direciton numeric;
alter table trafc.electronic_count_header add column worst_steering_double_axle_olhv_perc_negative_direciton numeric;
alter table trafc.electronic_count_header add column worst_steering_double_axle_tonperhv_negative_direciton numeric;
alter table trafc.electronic_count_header add column worst_non_steering_single_axle_cnt_negative_direciton numeric;
alter table trafc.electronic_count_header add column worst_non_steering_single_axle_olhv_perc_negative_direciton numeric;
alter table trafc.electronic_count_header add column worst_non_steering_single_axle_tonperhv_negative_direciton numeric;
alter table trafc.electronic_count_header add column worst_non_steering_double_axle_cnt_negative_direciton numeric;
alter table trafc.electronic_count_header add column worst_non_steering_double_axle_olhv_perc_negative_direciton numeric;
alter table trafc.electronic_count_header add column worst_non_steering_double_axle_tonperhv_negative_direciton numeric;
alter table trafc.electronic_count_header add column worst_triple_axle_cnt_negative_direciton numeric;
alter table trafc.electronic_count_header add column worst_triple_axle_olhv_perc_negative_direciton numeric;
alter table trafc.electronic_count_header add column worst_triple_axle_tonperhv_negative_direciton numeric;
alter table trafc.electronic_count_header add column bridge_formula_cnt_negative_direciton numeric;
alter table trafc.electronic_count_header add column bridge_formula_olhv_perc_negative_direciton numeric;
alter table trafc.electronic_count_header add column bridge_formula_tonperhv_negative_direciton numeric;
alter table trafc.electronic_count_header add column gross_formula_cnt_negative_direciton numeric;
alter table trafc.electronic_count_header add column gross_formula_olhv_perc_negative_direciton numeric;
alter table trafc.electronic_count_header add column gross_formula_tonperhv_negative_direciton numeric;
alter table trafc.electronic_count_header add column total_avg_cnt_negative_direciton numeric;
alter table trafc.electronic_count_header add column total_avg_olhv_perc_negative_direciton numeric;
alter table trafc.electronic_count_header add column total_avg_tonperhv_negative_direciton numeric;
"""

CREATE_HSWIM_HEADER_TABLE = """CREATE TABLE IF NOT EXISTS trafc.electronic_count_header_hswim (
header_id uuid not null primary key,
egrl_percent numeric,
egrw_percent numeric,
mean_equivalent_axle_mass numeric,
mean_equivalent_axle_mass_positive_direction numeric,
mean_equivalent_axle_mass_negative_direction numeric,
mean_axle_spacing numeric,
mean_axle_spacing_positive_direction numeric,
mean_axle_spacing_negative_direction numeric,
e80_per_axle numeric,
e80_per_axle_positive_direction numeric,
e80_per_axle_negative_direction numeric,
olhv numeric,
olhv_positive_direction numeric,
olhv_negative_direction numeric,
olhv_percent numeric,
olhv_percent_positive_direction numeric,
olhv_percent_negative_direction numeric,
tonnage_generated numeric,
tonnage_generated_positive_direction numeric,
tonnage_generated_negative_direction numeric,
olton numeric,
olton_positive_direction numeric,
olton_negative_direction numeric,
olton_percent numeric,
olton_percent_positive_direction numeric,
olton_percent_negative_direction numeric,
ole80 numeric,
ole80_positive_direction numeric,
ole80_negative_direction numeric,
ole80_percent numeric,
ole80_percent_positive_direction numeric,
ole80_percent_negative_direction numeric,
xe80 numeric,
xe80_positive_direction numeric,
xe80_negative_direction numeric,
xe80_percent numeric,
xe80_percent_positive_direction numeric,
xe80_percent_negative_direction numeric,
e80_per_day numeric,
e80_per_day_positive_direction numeric,
e80_per_day_negative_direction numeric,
e80_per_heavy_vehicle numeric,
e80_per_heavy_vehicle_positive_direction numeric,
e80_per_heavy_vehicle_negative_direction numeric,
worst_steering_single_axle_cnt numeric,
worst_steering_single_axle_olhv_perc numeric,
worst_steering_single_axle_tonperhv numeric,
worst_steering_double_axle_cnt numeric,
worst_steering_double_axle_olhv_perc numeric,
worst_steering_double_axle_tonperhv numeric,
worst_non_steering_single_axle_cnt numeric,
worst_non_steering_single_axle_olhv_perc numeric,
worst_non_steering_single_axle_tonperhv numeric,
worst_non_steering_double_axle_cnt numeric,
worst_non_steering_double_axle_olhv_perc numeric,
worst_non_steering_double_axle_tonperhv numeric,
worst_triple_axle_cnt numeric,
worst_triple_axle_olhv_perc numeric,
worst_triple_axle_tonperhv numeric,
bridge_formula_cnt numeric,
bridge_formula_olhv_perc numeric,
bridge_formula_tonperhv numeric,
gross_formula_cnt numeric,
gross_formula_olhv_perc numeric,
gross_formula_tonperhv numeric,
total_avg_cnt numeric,
total_avg_olhv_perc numeric,
total_avg_tonperhv numeric,
worst_steering_single_axle_cnt_positive_direciton numeric,
worst_steering_single_axle_olhv_perc_positive_direciton numeric,
worst_steering_single_axle_tonperhv_positive_direciton numeric,
worst_steering_double_axle_cnt_positive_direciton numeric,
worst_steering_double_axle_olhv_perc_positive_direciton numeric,
worst_steering_double_axle_tonperhv_positive_direciton numeric,
worst_non_steering_single_axle_cnt_positive_direciton numeric,
worst_non_steering_single_axle_olhv_perc_positive_direciton numeric,
worst_non_steering_single_axle_tonperhv_positive_direciton numeric,
worst_non_steering_double_axle_cnt_positive_direciton numeric,
worst_non_steering_double_axle_olhv_perc_positive_direciton numeric,
worst_non_steering_double_axle_tonperhv_positive_direciton numeric,
worst_triple_axle_cnt_positive_direciton numeric,
worst_triple_axle_olhv_perc_positive_direciton numeric,
worst_triple_axle_tonperhv_positive_direciton numeric,
bridge_formula_cnt_positive_direciton numeric,
bridge_formula_olhv_perc_positive_direciton numeric,
bridge_formula_tonperhv_positive_direciton numeric,
gross_formula_cnt_positive_direciton numeric,
gross_formula_olhv_perc_positive_direciton numeric,
gross_formula_tonperhv_positive_direciton numeric,
total_avg_cnt_positive_direciton numeric,
total_avg_olhv_perc_positive_direciton numeric,
total_avg_tonperhv_positive_direciton numeric,
worst_steering_single_axle_cnt_negative_direciton numeric,
worst_steering_single_axle_olhv_perc_negative_direciton numeric,
worst_steering_single_axle_tonperhv_negative_direciton numeric,
worst_steering_double_axle_cnt_negative_direciton numeric,
worst_steering_double_axle_olhv_perc_negative_direciton numeric,
worst_steering_double_axle_tonperhv_negative_direciton numeric,
worst_non_steering_single_axle_cnt_negative_direciton numeric,
worst_non_steering_single_axle_olhv_perc_negative_direciton numeric,
worst_non_steering_single_axle_tonperhv_negative_direciton numeric,
worst_non_steering_double_axle_cnt_negative_direciton numeric,
worst_non_steering_double_axle_olhv_perc_negative_direciton numeric,
worst_non_steering_double_axle_tonperhv_negative_direciton numeric,
worst_triple_axle_cnt_negative_direciton numeric,
worst_triple_axle_olhv_perc_negative_direciton numeric,
worst_triple_axle_tonperhv_negative_direciton numeric,
bridge_formula_cnt_negative_direciton numeric,
bridge_formula_olhv_perc_negative_direciton numeric,
bridge_formula_tonperhv_negative_direciton numeric,
gross_formula_cnt_negative_direciton numeric,
gross_formula_olhv_perc_negative_direciton numeric,
gross_formula_tonperhv_negative_direciton numeric,
total_avg_cnt_negative_direciton numeric,
total_avg_olhv_perc_negative_direciton numeric,
total_avg_tonperhv_negative_direciton numeric
);
"""
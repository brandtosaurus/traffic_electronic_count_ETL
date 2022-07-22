import sqlalchemy as sa
import psycopg2

#### DB CONNECTION
DB_NAME = "gauteng"
DB_USER = "postgres"
DB_PASS = "Lin3@r1in3!431"
DB_HOST = "linearline.dedicated.co.za"
DB_PORT = "5432"

#  DB Table Names for pandas.to_df
TRAFFIC_SCHEMA = 'trafc'
MAIN_TBL_NAME = "electronic_count_data_partitioned"
HEADER_TBL_NAME = "electronic_count_header"
LANES_TBL_NAME = "traffic_lane_configuration"
TYPE_21_TBL_NAME = "electronic_count_data_type_21"
TYPE_30_TBL_NAME = "electronic_count_data_type_30"
TYPE_60_TBL_NAME = "electronic_count_data_type_60"
TYPE_70_TBL_NAME = "electronic_count_data_type_70"

TYPE_10_TBL_NAME = "electronic_count_data_type_10"
WX_TABLE = "traffic_e_type10_wheel_mass"
AX_TABLE = "traffic_e_type10_axle_mass"
GX_TABLE = "traffic_e_type10_axle_group_mass"
SX_TABLE = "traffic_e_type10_axle_spacing"
TX_TABLE = "traffic_e_type10_tyre"
CX_TABLE = "traffic_e_type10_axle_group_configuration"
VX_TABLE = "traffic_e_type10_identification_data_images"

OUTPUT_FILE = r"C:" + r"\FTP\import_results\rsa_traffic_counts"
FILES_COMPLETE = r"C:" + r"\FTP\import_results\rsa_traffic_counts\RSA_FILES_COMPLETE.csv"
PATH = r"C:" + r"\FTP"
PROBLEM_FILES = r"C:" + r"\FTP\import_results\rsa_traffic_counts\RSA_COUNT_PROBLEM_FILES.csv"

DATA_IMPORTANT_COLUMNS = [
    "site_id",
    "header_id",
    "start_datetime",
    "end_datetime",
    "lane_number"
    ]

ELECTRONIC_COUNT_DATA_TYPE21_NAME_CHANGE = {
    'duration_min':'duration_of_summary',
    'speedbin0':'number_of_vehicles_in_speedbin_0',
    'speedbin1':'number_of_vehicles_in_speedbin_1', 
    'speedbin2':'number_of_vehicles_in_speedbin_2', 
    'speedbin3':'number_of_vehicles_in_speedbin_3', 
    'speedbin4':'number_of_vehicles_in_speedbin_4',
    'speedbin5':'number_of_vehicles_in_speedbin_5', 
    'speedbin6':'number_of_vehicles_in_speedbin_6', 
    'speedbin7':'number_of_vehicles_in_speedbin_7', 
    'speedbin8':'number_of_vehicles_in_speedbin_8' ,
    'speedbin9':'number_of_vehicles_in_speedbin_9',
    'speedbin10':'number_of_vehicles_in_speedbin_10',
    'short_heavy_vehicles':'number_of_short_heavy_vehicles',
    'medium_heavy_vehicles':'number_of_medium_heavy_vehicles', 
    'long_heavy_vehicles':'number_of_long_heavy_vehicles',
    'rear_to_rear_headway_shorter_than_2_seconds':'number_of_rear_to_rear_headway_shorter_than_2_seconds'
    }

# Database Connection
ENGINE_URL = sa.engine.URL.create(
    "postgresql",
    username=DB_USER,
    password=DB_PASS,
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
)

ENGINE = sa.create_engine(
    ENGINE_URL, pool_pre_ping=True
)

CONN = psycopg2.connect(
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASS,
    host=DB_HOST,
    port=DB_PORT)
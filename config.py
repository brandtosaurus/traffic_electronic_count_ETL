from sqlalchemy import create_engine

#### DB CONNECTION
ENGINE = create_engine(
    "postgresql://postgres:Lin3@r1in3!431@linearline.dedicated.co.za:5432/gauteng"
)

# engine = create_engine(
#     "postgresql://postgres:$admin@localhost:5432/asset_management_master"
# )

OUTPUT_FILE = r"~\Desktop\Temp\rsa_traffic_counts\TEMP_E_COUNT_"

FILES_COMPLETE = r"~\Desktop\Temp\rsa_traffic_counts\RSA_FILES_COMPLETE_localhost.csv"

PATH = r"~\Desktop\Temp\rsa_traffic_counts"

PROBLEM_FILES = r"~\Desktop\Temp\rsa_traffic_counts\RSA_COUNT_PROBLEM_FILES.csv"

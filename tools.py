import pandas as pd
from pandasql import sqldf
import config
from sqlalchemy.dialects.postgresql import insert
import zipfile
from typing import List
import csv
import os
import zipfile
from io import StringIO


#### DATA TOOLS ####
def is_zip(path: str) -> bool:
    for filename in path:
        return zipfile.is_zipfile(filename)

def getfiles(path: str) -> List[str]:
    print("COLLECTING FILES......")
    src = []
    for root, dirs, files in os.walk(path):
        for name in files:
            if (
                name.endswith(".RSA")
                or name.endswith(".rsa")
                or name.endswith(".rsv")
                or name.endswith(".RSV")
            ):
                p = os.path.join(root, name)
                src.append(p)
    src = list(set(src))
    return src

def to_df(file: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(file, header=None)
        df = df[0].str.split("\s+|,\s+|,", expand=True)
        return df
    except Exception as e:
        df = pd.read_csv(file, header=None, sep='\s+')
        df = df[0].str.split("\s+|,\s+|,", expand=True)
        return df
    except:
        df = pd.read_csv(file, header=None, sep='\t')
        df = df[0].str.split("\s+|,\s+|,", expand=True)
        return df

def get_direction(lane_number, df: pd.DataFrame) -> pd.DataFrame:
    filt = df[1] == lane_number
    df = df.where(filt)
    df = df[2].dropna()
    df = int(df)
    return df

def get_lane_position(lane_number, df: pd.DataFrame) -> pd.DataFrame:
    filt = df[1] == lane_number
    df = df.where(filt)
    df = df[3].dropna()
    df = str(df)
    return df

def join_header_id(d2, header):
    if d2 is None:
        pass
    else:
        data = data_join(d2, header)
        data.drop("station_name", axis=1, inplace=True)
        data["start_datetime"] = data["start_datetime"].astype("datetime64[ns]")
        d2["start_datetime"] = d2["start_datetime"].astype("datetime64[ns]")
        data = data.merge(
            d2, how="outer", on=["site_id", "start_datetime", "lane_number"]
        )
    return data

def data_join(data: pd.DataFrame, header: pd.DataFrame) -> pd.DataFrame:
    if data is None:
        pass
    elif data.empty:
        pass
    else:
        data = pd.DataFrame(data)
        data = join(header, data)
    return data

def join(header: pd.DataFrame, data: pd.DataFrame) -> pd.DataFrame:
    if data.empty:
        df = pd.DataFrame()
    else:
        q = """
        SELECT header.header_id, header.station_name, data.*
        FROM header
        LEFT JOIN data ON data.start_datetime WHERE data.start_datetime >= header.start_datetime AND data.end_datetime <= header.end_datetime;
        """
        q2 = """UPDATE data set header_id = (SELECT header_id from header WHERE data.start_datetime >= header.start_datetime AND data.counttime_end <= header.enddate)"""
        pysqldf = lambda q: sqldf(q, globals())
        df = sqldf(q, locals())
        df = pd.DataFrame(df)
    return df

def postgres_upsert(table, conn, keys, data_iter):
    data = [dict(zip(keys, row)) for row in data_iter]

    insert_statement = insert(table.table).values(data)
    upsert_statement = insert_statement.on_conflict_do_update(
        constraint=f"{table.table.name}_pkey",
        set_={c.key: c for c in insert_statement.excluded},
    )
    conn.execute(upsert_statement)

def push_to_db(df, table, subset) -> None:
    try:
        df.to_sql(
            table,
            con=config.ENGINE,
            schema="trafc",
            if_exists="append",
            index=False,
            method=psql_insert_copy,
        )
    except Exception:
        df = df.drop_duplicates(subset=subset)
        df.to_sql(
            table,
            con=config.ENGINE,
            schema="trafc",
            if_exists="append",
            index=False,
            method=psql_insert_copy,
        )

def create_database_tables():
    conn=config.CONN
    cur = conn.cursor()
    cur.execute(q.CREATE_AXLE_GROU_MASS_GX)
    cur.execute(q.CREATE_AXLE_GROUP_CONFIG_TABLE_CX)
    cur.execute(q.CREATE_AXLE_MASS_TABLE_AX)
    cur.execute(q.CREATE_AXLE_GROUP_MASS_TABLE_GX)
    cur.execute(q.CREATE_AXLE_SPACING_TABLE_SX)
    cur.execute(q.CREATE_IMAGES_TABLE_VX)
    cur.execute(q.CREATE_TYRE_TABLE_TX)
    cur.execute(q.CREATE_WHEEL_MASS_TABLE_WX)
    cur.commit()
    cur.close()

def psql_insert_copy(table, conn, keys, data_iter):
    """
    Execute SQL statement inserting data

    Parameters
    ----------
    table : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list of str
        Column names
    data_iter : Iterable that iterates the values to be inserted
    """
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ", ".join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = "{}.{}".format(table.schema, table.name)
        else:
            table_name = table.name

        sql = "COPY {} ({}) FROM STDIN WITH CSV".format(table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)

def save_to_temp_csv(df: pd.DataFrame, label: str):
    if not os.path.exists(os.path.expanduser(config.OUTPUT_FILE + label + ".csv")):
        df.to_csv(
            os.path.expanduser(config.OUTPUT_FILE + label + ".csv"),
            mode="a",
            header=True,
            index=False,
        )
    else:
        df.to_csv(
            os.path.expanduser(config.OUTPUT_FILE + label + ".csv"),
            mode="a",
            header=False,
            index=False,
        )

def push_to_partitioned_table(df, table, subset) -> None:
    yr = data['year'].unique()[0]
    print(yr)
    try:
        df.to_sql(
            table+'_'+str(yr),
            con=config.ENGINE,
            schema="trafc",
            if_exists="append",
            index=False,
            method=psql_insert_copy,
        )
    except Exception:
        df = df.drop_duplicates(subset=subset)
        df.to_sql(
            table+'_'+yr,
            con=config.ENGINE,
            schema="trafc",
            if_exists="append",
            index=False,
            method=psql_insert_copy,
        )

def postgres_upsert(table, conn, keys, data_iter):
    data = [dict(zip(keys, row)) for row in data_iter]

    insert_statement = insert(table.table).values(data)
    upsert_statement = insert_statement.on_conflict_do_update(
        constraint=f"{table.table.name}_un",
        set_={c.key: c for c in insert_statement.excluded},
    )
    conn.execute(upsert_statement)

def create_database_tables():
    conn=config.CONN
    cur = conn.cursor()
    cur.execute(q.CREATE_AXLE_GROU_MASS_GX)
    cur.execute(q.CREATE_AXLE_GROUP_CONFIG_TABLE_CX)
    cur.execute(q.CREATE_AXLE_MASS_TABLE_AX)
    cur.execute(q.CREATE_AXLE_GROUP_MASS_TABLE_GX)
    cur.execute(q.CREATE_AXLE_SPACING_TABLE_SX)
    cur.execute(q.CREATE_IMAGES_TABLE_VX)
    cur.execute(q.CREATE_TYRE_TABLE_TX)
    cur.execute(q.CREATE_WHEEL_MASS_TABLE_WX)
    cur.commit()
    cur.close()

def dropDuplicatesDoSomePostProcesingAndSaveCsv(csv_label: str):
    df = pd.DataFrame()
    for i in pd.read_csv(
        os.path.expanduser(
            f"~\Desktop\Temp\rsa_traffic_counts\TEMP_E_COUNT_{csv_label}.csv"
        ),
        chunksize=50000,
        error_bad_lines=False,
        low_memory=False,
    ):
        df = pd.concat([df, i])
        df = df.drop_duplicates()
    cols = df.columns.tolist()
    df2 = pd.DataFrame(columns=cols)
    df2.to_csv(
        os.path.expanduser(
            f"~\Desktop\Temp\rsa_traffic_counts\TEMP_E_COUNT_{csv_label}.csv"
        ),
        header=True,
    )
    df.to_csv(
        os.path.expanduser(
            f"~\Desktop\Temp\rsa_traffic_counts\TEMP_E_COUNT_{csv_label}.csv"
        ),
        mode="a",
        header=False,
    )


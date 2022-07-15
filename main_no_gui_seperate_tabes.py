import tqdm
import pandas as pd
import multiprocessing as mp
from psycopg2.errors import UniqueViolation

import rsa_data_summary as rd
import rsa_data_wim as wim
import rsa_headers as rh
import config
import queries as q
import tools

#### MAIN EXECUTABLE
def main(files: str):
    partition_table_columns = list(pd.read_sql_query("SELECT * from trafc.electronic_count_data_partitioned limit 1",config.ENGINE).columns)
    electronic_count_data_type_21_columns = list(pd.read_sql_query("SELECT * from trafc.electronic_count_data_type_21 limit 1",config.ENGINE).columns)
    electronic_count_data_type_30_columns = list(pd.read_sql_query("SELECT * from trafc.electronic_count_data_type_30 limit 1",config.ENGINE).columns)
    electronic_count_data_type_60_columns = list(pd.read_sql_query("SELECT * from trafc.electronic_count_data_type_60 limit 1",config.ENGINE).columns)
    electronic_count_data_type_70_columns = list(pd.read_sql_query("SELECT * from trafc.electronic_count_data_type_70 limit 1",config.ENGINE).columns)
    header_columns = list(pd.read_sql_query("SELECT * from trafc.electronic_count_header limit 1",config.ENGINE).columns)

    for file in files:
        df = tools.to_df(file)
        H = rh.Headers(df, file)
        header = H.header
        lanes = H.lanes

        D = rd.Data(df, header)
        data = D.dtype21

        if header is None:
            pass
        else:
            try:
                header = rd.Data.header_calcs(header, data, 21)
                header = header[header.columns.intersection(header_columns)]
                try:
                    header.to_sql("electronic_count_header",
                            con=config.ENGINE,
                            schema="trafc",
                            if_exists="append",
                            index=False,
                            method=tools.psql_insert_copy,
                        )
                except UniqueViolation:
                    pass
            except AttributeError:
                pass

        d2 = data
        if d2 is None:
            pass
        else:
            d2.rename(columns={'duration_min':'duration_of_summary',
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
                }, inplace=True)
            d2 = d2[d2.columns.intersection(electronic_count_data_type_21_columns)]
            try:
                d2.to_sql("electronic_count_data_type_21",
                        con=config.ENGINE,
                        schema="trafc",
                        if_exists="append",
                        index=False,
                        method=tools.psql_insert_copy,
                    )
            except UniqueViolation:
                pass

        d2 = D.dtype70
        if d2 is None:
            pass
        else:
            data = rd.merge_summary_dataframes(d2, data)
            d2 = d2[d2.columns.intersection(electronic_count_data_type_70_columns)]
            try:
                d2.to_sql("electronic_count_data_type_70",
                    con=config.ENGINE,
                    schema="trafc",
                    if_exists="append",
                    index=False,
                    method=tools.psql_insert_copy,
                )
            except UniqueViolation:
                pass

        d2 = D.dtype30
        if d2 is None:
            pass
        else:
            data = rd.merge_summary_dataframes(d2, data)

        data = data[data.columns.intersection(partition_table_columns)]
        if data is None:
            pass
        else:
            try:
                data.to_sql("electronic_count_data_partitioned",
                        con=config.ENGINE,
                        schema="trafc",
                        if_exists="append",
                        index=False,
                        method=tools.psql_insert_copy,
                    )
            except UniqueViolation:
                pass

        print('DONE WITH '+file)



##################################################################################################################################
##################################################################################################################################


if __name__ == "__main__":

    files = tools.getfiles(config.PATH)

    ########### ATTEMPT AT GETTING A SINGLE PROGRESS BAR ###########
    pool = mp.Pool(int(mp.cpu_count()))
    for _ in tqdm.tqdm(pool.imap_unordered(main, files), total=len(files)):
        pass
    pool.close()
    pool.join()

########### FOR TYPE 10's ONLY ###########
#    pool = mp.Pool(int(mp.cpu_count()))
#    for _ in tqdm.tqdm(pool.imap_unordered(main_type10, files), total=len(files)):
#        pass
#    pool.close()
#    pool.join()

    print("COMPLETE")

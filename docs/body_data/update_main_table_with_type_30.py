import docs.queries as q
import config
import pandas as pd
import multiprocessing as mp
import traceback
import tqdm
import gc


cs1 = pd.read_sql_query(q.SELECT_CLASSIFICAITON_SCHEME_1, config.ENGINE)
cs5 = pd.read_sql_query(q.SELECT_CLASSIFICAITON_SCHEME_5, config.ENGINE)
cs8 = pd.read_sql_query(q.SELECT_CLASSIFICAITON_SCHEME_8, config.ENGINE)


def get_list_of_headers():
    """
    It returns a list of strings, each string being a unique header_id from a table in a database.
    :return: A list of strings.
    """
    select_qry = """
    select distinct header_id from trafc.electronic_count_data_partitioned
    where light_motor_vehicles is null
    ;
    """
    df = pd.read_sql(select_qry, config.ENGINE)
    df.dropna(subset=["header_id"], inplace=True)
    return df["header_id"].astype(str).to_list()


def main(header_id):
    """
    It takes a list of header_ids, reads the data from the type 30 table, groups it by site_id,
    start_datetime, lane_number, classification_scheme, and class_number, and sums the
    number_of_vehicles. Then it iterates through the grouped dataframe and updates the main table with
    the summed number_of_vehicles

    :param list_of_headers: list of header_ids that are in the main table but not in the t30 table
    """
    df30 = pd.read_sql(
        f"""
        select * from trafc.electronic_count_data_type_30 where header_id = '{header_id}';
        """,
        config.ENGINE,
    )

    df30 = df30.groupby(
        [
            "site_id",
            "start_datetime",
            "lane_number",
            "classification_scheme",
            "class_number",
        ]
    )["number_of_vehicles"].sum()

    for index, row in df30.iteritems():
        site_id = index[0]
        start_datetime = index[1]
        lane_number = index[2]
        classification_scheme = index[3]
        class_number = index[4]
        number_of_vehicles = row
        if classification_scheme == 1:
            update_qry = f"""
                update trafc.electronic_count_data_partitioned set
                {cs1['vehicle'].at[class_number].lower().replace(" ","_").replace("-","_").replace(",","").replace("(","").replace(")","")} = {number_of_vehicles}
                where site_id = '{site_id}'
                and start_datetime = '{start_datetime}'
                and lane_number = {lane_number}
                and {cs1['vehicle'].at[class_number].lower().replace(" ","_").replace("-","_").replace(",","").replace("(","").replace(")","")} is null;
            """
        elif classification_scheme == 5:
            update_qry = f"""
                update trafc.electronic_count_data_partitioned set 
                {cs5['vehicle'].at[class_number].lower().replace(" ","_").replace("-","_").replace(",","").replace("(","").replace(")","")} = {number_of_vehicles}
                where site_id = '{site_id}' 
                and start_datetime = '{start_datetime}'
                and lane_number = {lane_number}
                and {cs5['vehicle'].at[class_number].lower().replace(" ","_").replace("-","_").replace(",","").replace("(","").replace(")","")} is null;            
            """
        elif classification_scheme == 8:
            update_qry = f"""
                update trafc.electronic_count_data_partitioned set 
                {cs8['vehicle'].at[class_number].lower().replace(" ","_").replace("-","_").replace(",","").replace("(","").replace(")","")} = {number_of_vehicles}
                where site_id = '{site_id}' 
                and start_datetime = '{start_datetime}'
                and lane_number = {lane_number}
                and {cs8['vehicle'].at[class_number].lower().replace(" ","_").replace("-","_").replace(",","").replace("(","").replace(")","")} is null;            
            """
        else:
            pass
        try:
            with config.CONN as conn:
                cur = conn.cursor()
                cur.execute(update_qry)
                conn.commit()
        except Exception as e:
            print(e)
            print(update_qry)
            traceback.print_exc()
            conn.rollback()
            conn.close()
            gc.collect()
            break


def upd_main_with_t30():
    """
    It takes a list of header_ids, and for each header_id, it takes the data from the
    electronic_count_data_type_30 table, groups it by site_id, start_datetime, lane_number,
    classification_scheme, and class_number, and sums the number_of_vehicles column. Then, it updates
    the electronic_count_data_partitioned table with the summed number_of_vehicles for each
    classification_scheme and class_number.
    """
    select_qry = """
    select distinct header_id from trafc.electronic_count_data_partitioned
    where light_motor_vehicles is null
    ;
    """

    df = pd.read_sql(select_qry, config.ENGINE)
    df.dropna(subset=["header_id"], inplace=True)
    list = df.header_id.astype(str).to_list()
    cs1 = pd.read_sql_query(q.SELECT_CLASSIFICAITON_SCHEME_1, config.ENGINE)
    cs5 = pd.read_sql_query(q.SELECT_CLASSIFICAITON_SCHEME_5, config.ENGINE)
    cs8 = pd.read_sql_query(q.SELECT_CLASSIFICAITON_SCHEME_8, config.ENGINE)
    for header_id in list:
        df30 = pd.read_sql(
            f"""
            select * from trafc.electronic_count_data_type_30 where header_id = '{header_id}';
            """,
            config.ENGINE,
        )

        df30 = df30.groupby(
            [
                "site_id",
                "start_datetime",
                "lane_number",
                "classification_scheme",
                "class_number",
            ]
        )["number_of_vehicles"].sum()

        for index, row in df30.iteritems():
            site_id = index[0]
            start_datetime = index[1]
            lane_number = index[2]
            classification_scheme = index[3]
            class_number = index[4]
            number_of_vehicles = row
            if classification_scheme == 1:
                update_qry = f"""
                    update trafc.electronic_count_data_partitioned set
                    {cs1['vehicle'].at[class_number].lower().replace(to_replace=[" ","-"],value="_").replace(to_replace=[",","(",")"],value="")} = {number_of_vehicles}
                    where site_id = '{site_id}'
                    and start_datetime = '{start_datetime}'
                    and lane_number = {lane_number}
                    and {cs1['vehicle'].at[class_number].lower().replace(to_replace=[" ","-"],value="_").replace(to_replace=[",","(",")"],value="")} is null;
                """
            elif classification_scheme == 5:
                update_qry = f"""
                    update trafc.electronic_count_data_partitioned set 
                    {cs5['vehicle'].at[class_number].lower().replace(to_replace=[" ","-"],value="_").replace(to_replace=[",","(",")"],value="")} = {number_of_vehicles}
                    where site_id = '{site_id}' 
                    and start_datetime = '{start_datetime}'
                    and lane_number = {lane_number}
                    and {cs5['vehicle'].at[class_number].lower().replace(to_replace=[" ","-"],value="_").replace(to_replace=[",","(",")"],value="")} is null;            
                """
            elif classification_scheme == 8:
                update_qry = f"""
                    update trafc.electronic_count_data_partitioned set 
                    {cs8['vehicle'].at[class_number].lower().replace(to_replace=[" ","-"],value="_").replace(to_replace=[",","(",")"],value="")} = {number_of_vehicles}
                    where site_id = '{site_id}' 
                    and start_datetime = '{start_datetime}'
                    and lane_number = {lane_number}
                    and {cs8['vehicle'].at[class_number].lower().replace(to_replace=[" ","-"],value="_").replace(to_replace=[",","(",")"],value="")} is null;            
                """
            else:
                pass
            try:
                with config.CONN as conn:
                    cur = conn.cursor()
                    cur.execute(update_qry)
                    conn.commit()
            except Exception as e:
                print(e)
                print(update_qry)
                traceback.print_exc()
                conn.rollback()
                conn.close()
                gc.collect()
                continue


def run_multiprocess(list_of_headers):
    """
    It takes a path, gets all the files in that path, creates a pool of processes equal to the number of
    CPU cores, and then runs the main function on each file in the path.

    :param path: The path to the folder containing the files you want to process
    """
    try:
        pool = mp.Pool(int(mp.cpu_count()))
        for _ in tqdm.tqdm(
            pool.imap_unordered(main, list_of_headers), total=len(list_of_headers)
        ):
            pass
        pool.close()
        pool.join()
    except Exception as exc:
        traceback.print_exc()
        print(exc)


if __name__ == "__main__":
    print("Running...")
    list_of_headers = get_list_of_headers()
    run_multiprocess(list_of_headers)
    print("Done")

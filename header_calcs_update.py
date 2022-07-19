import rsa_data_wim as wim
import queries as q
import config

import pandas as pd

class Upsert():
    def __init__(self) -> None:
        Upsert.main(self.get_headers_to_update())

    def get_headers_to_update(self):
        print('fetching headers to update')
        self.header_ids = pd.read_sql_query(q.GET_HSWIM_HEADER_IDS, config.ENGINE)
        # return self.header_ids

    def main(self):
        for self.header_id in list(self.header_ids['header_id'].astype(str)):
            SELECT_TYPE10_QRY, AXLE_SPACING_SELECT_QRY, WHEEL_MASS_SELECT_QRY = wim.wim_stations_header_insert_qrys(header_id)
            self.df, self.df2, self.df3 = wim.wim_stations_header_insert_dfs(SELECT_TYPE10_QRY, AXLE_SPACING_SELECT_QRY, WHEEL_MASS_SELECT_QRY)
            if self.df2 is None or self.df3 is None:
                pass
            else:
                print(f'working on {self.header_id}')
                self.self.insert_string = wim.wim_stations_header_upsert(self.header_id, self.df, self.df2, self.df3)
                with config.ENGINE.connect() as conn:
                    print(f'upserting {self.header_id}')
                    conn.execute(self.insert_string)
                    print('COMPLETE')

if __name__ == "__main__":
    upsert = Upsert()

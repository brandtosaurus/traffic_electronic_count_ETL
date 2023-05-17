import pandas as pd
import traceback
import gc
import os
import csv
import config


def get_head(df: pd.DataFrame, file: str) -> pd.DataFrame:
    """
    It returns a dataframe that contains all rows where the first column is either H0, S0, I0, S1,
    D0, D1, D3, L0, L1, or where the first column is either 21, 22, 70, 30, 31, 60 and the third
    column is less than 80, or where the first column is 10 and the second column is 1, 8, 5, 9, 01,
    08, 05, or 09

    :param df: the dataframe to be processed
    :return: A dataframe
    """
    try:
        df = (
            pd.DataFrame(
                df.loc[
                    (df[0].isin(["H0", "S0", "I0", "S1", "D0", "D1", "D3", "L0", "L1"]))
                    | (
                        (df[0].isin(["21", "22", "70", "30", "31", "60"]))
                        & (
                            df.loc[df[0].isin(["21", "22", "70", "30", "31", "60"]), 2]
                            .fillna("0")
                            .astype(int)
                            < 80
                        )
                    )
                    | (
                        (df[0].isin(["10"]))
                        & (df[1].isin(["1", "8", "5", "9", "01", "08", "05", "09"]))
                    )
                ]
            )
            .dropna(axis=1, how="all")
            .reset_index(drop=True)
            .copy()
        )
        return df
    except KeyError as exc:
        traceback.print_exc(exc)
        with open(os.path.expanduser(config.FILES_FAILED), "a", newline="") as f:
            write = csv.writer(f)
            write.writerows([[file]])
        gc.collect()
        pass

def get_summary_data(df: pd.DataFrame, file: str) -> pd.DataFrame:
    """
    It takes a dataframe, filters it, and returns a dataframe

    :param df: pd.DataFrame
    :type df: pd.DataFrame
    :return: A dataframe
    """
    try:
        df = (
            pd.DataFrame(
                df.loc[
                    (
                        ~df[0].isin(
                            [
                                "H0",
                                "H9",
                                "S0",
                                "I0",
                                "S1",
                                "D0",
                                "D1",
                                "D3",
                                "L0",
                                "L1",
                            ]
                        )
                    )
                    & (
                        (df[0].isin(["21", "22", "70", "30", "31", "60"]))
                        & (df[1].isin(["0", "1", "2", "3", "4"]))
                        & (
                            df.loc[df[0].isin(["21", "22", "70", "30", "31", "60"]), 2]
                            .fillna("0")
                            .astype(int)
                            > 80
                        )
                    )
                ]
            )
            .dropna(axis=1, how="all")
            .reset_index(drop=True)
            .copy()
        )
        df = df.dropna(axis=0, how="all").reset_index(drop=True)
        return df
    except TypeError as exc:
        print(f"gat_data func: check filtering and file {file}")
        print(exc)
    except Exception as exc:
        traceback.print_exc(exc)
        with open(os.path.expanduser(config.FILES_FAILED), "a", newline="") as f:
            write = csv.writer(f)
            write.writerows([[file]])
        gc.collect()

def get_indv_data(df: pd.DataFrame, file: str) -> pd.DataFrame:
    """
    It takes a dataframe, filters it, and returns a dataframe

    :param df: pd.DataFrame
    :type df: pd.DataFrame
    :return: A dataframe
    """
    try:
        df = (
            pd.DataFrame(
                df.loc[
                    (
                        (df[0].isin(["10"]))
                        & (
                            ~df[1].isin(
                                ["1", "8", "5", "9", "01", "08", "05", "09"]
                            )
                        )
                        & (
                            df.loc[df[0].isin(["10"]), 4].fillna("0").astype(int)
                            > 80
                        )
                        & (
                            ~df[0].isin(
                                [
                                    "H0",
                                    "H9",
                                    "S0",
                                    "I0",
                                    "S1",
                                    "D0",
                                    "D1",
                                    "D3",
                                    "L0",
                                    "L1",
                                ]
                            )
                        )
                    )
                ]
            )
            .dropna(axis=1, how="all")
            .reset_index(drop=True)
            .copy()
        )
        df = df.dropna(axis=0, how="all").reset_index(drop=True)
        return df
    except TypeError as exc:
        print(f"gat_data func: check filtering and file {file}")
        print(exc)
    except Exception as exc:
        traceback.print_exc(exc)
        with open(os.path.expanduser(config.FILES_FAILED), "a", newline="") as f:
            write = csv.writer(f)
            write.writerows([[file]])
        gc.collect()

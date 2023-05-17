import pandas as pd
import config
import gc
import traceback
import csv
import os


def to_df(file) -> pd.DataFrame:  # CSV to DataFrame
    """
    It takes a CSV file, reads it in as a DataFrame, splits the first column on a variety of
    delimiters, and returns the resulting DataFrame.
    :return: A DataFrame
    """
    try:
        df = pd.read_csv(file, header=None, dtype=str, sep="\s+\t", engine="python")
        df = df[0].str.split("\s+|,\s+|,|;|;\s+", expand=True)
        df = df.dropna(axis=1, how="all")
        return df
    except Exception:
        traceback.print_exc()
        with open(
            os.path.expanduser(config.FILES_FAILED), "a", newline="", encoding=None
        ) as f:
            write = csv.writer(f)
            write.writerows([[file]])
        gc.collect()

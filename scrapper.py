import pandas as pd
import numpy as np
import yfinance as yf
import os
from tqdm import tqdm


def get_eq_stk_info(link):
    df_stks = pd.read_csv(link)

    # lower columns
    cols = df_stks.columns
    cols_lower = [col.strip().lower().replace(" ", "_") for col in cols]
    df_stks.columns = cols_lower

    # Filter Equity Stocks
    df_stks = df_stks[df_stks["series"] == "EQ"].copy()
    df_stks.reset_index(drop=True, inplace=True)

    return df_stks


def save_df(data, tick):
    curr_directory = os.getcwd()
    csv_data_path = os.path.join(curr_directory, "data", "csv_files")
    parquet_data_path = os.path.join(curr_directory, "data", "parquet_files")

    if not os.path.exists(csv_data_path):
        os.makedirs(csv_data_path)

    if not os.path.exists(parquet_data_path):
        os.makedirs(parquet_data_path)

    csv_file_path = os.path.join(csv_data_path, tick)
    parquet_file_path = os.path.join(parquet_data_path, tick)

    # to_csv
    data.to_csv(f"{csv_file_path}.csv", index=False)

    # to_parquet
    data.to_parquet(f"{parquet_file_path}.parquet")


def get_historical_data(symbols):
    # symbols_str = " ".join(symbols)

    # yf.download(
    #     symbols_str, start="1990-01-01", end=None, group_by="ticker"
    # )

    for tick in tqdm(symbols, desc="Files Saved", total=len(symbols)):
        stk = yf.Ticker(tick)
        df = stk.history(period="max", prepost=True, rounding=True)
        save_df(df, tick)
        

def main():
    stks_info_link = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
    df_eq_stks = get_eq_stk_info(stks_info_link)
    eq_symbols = df_eq_stks["symbol"].values
    eq_symbols = [eq + ".NS" for eq in eq_symbols]
    get_historical_data(eq_symbols)


if __name__ == "__main__":
    main()

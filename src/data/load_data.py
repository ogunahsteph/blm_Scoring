# Import modules
import re
import os
import sys
# import math
import joblib
import pickle
import datetime as dt
from IPython.display import display

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
# import matplotlib.ticker as ticker
import seaborn as sns

# import pymysql
import requests
from tzlocal import get_localzone
# from scipy.stats import mode
import mlflow
# import urlparse


# Import custom modules
sys.path.append(os.path.join(os.getcwd(), ""))

from src.utilities.db import *
from sql.bloom import *


# Parameters
date_in_scope = pd.Timestamp.today()
# date_in_scope = pd.Timestamp('2023-10-12')
# date_in_scope = pd.Timestamp.today() - (pd.Timestamp.today() - pd.Timestamp('2022-10-27'))
refresh_date = date_in_scope.strftime('%Y-%m-%d')
extract_end_date = pd.Timestamp(refresh_date) + dt.timedelta(days=-1)
meta_extract_start_date = extract_end_date + dt.timedelta(days=-30)
created_at = date_in_scope.strftime('%Y-%m-%d %H:%M:%S')
record_added_to_warehouse_on_timestamp = pd.Timestamp.today().strftime("%Y-%m-%d %H:%M:%S:%f")
# refresh_date = (pd.Timestamp.today()).strftime('%Y-%m-%d')
# extract_end_date = pd.Timestamp(refresh_date) + dt.timedelta(days=-1)
# meta_extract_start_date = extract_end_date + dt.timedelta(days=-31)
# created_at = (pd.Timestamp.today()).strftime('%Y-%m-%d %H:%M:%S')
# record_added_to_warehouse_on_timestamp = (pd.Timestamp.today()).strftime("%Y-%m-%d %H:%M:%S:%f")

local_tz = get_localzone()
execution_date = str(dt.datetime.now().replace(tzinfo=local_tz))


# Functions
def convert_to_parquet(config_path, p, f):
    # Load configurations
    config = read_params(config_path)
    project_dir = config["project_dir"]
    raw_data_path = config["raw_data_config"][f"{p}_raw_data_{f}"]
    raw_data_path_parquet = config["raw_data_config"][f"{p}_raw_data_parquet"]

    # Load snapshot
    if f == 'csv':
        df = pd.read_csv(project_dir + raw_data_path)
    elif f == "excel":
        df = pd.DataFrame()
        df_sheets = pd.ExcelFile(project_dir + raw_data_path)
        for sheet_name in df_sheets.sheet_names:
            df = pd.concat([df, df_sheets.parse(sheet_name)], axis=0, ignore_index=True)

    # Save snapshot
    df.to_parquet(project_dir + raw_data_path_parquet, index=False)


def query_dwh(sql, dwh_credentials, prefix, project_dir):
    """
    load data by executing SQL script
    input: SQL script 
    output: pandas dataframe 
    """

    conn = db_connection(dwh_credentials, prefix, project_dir)
    df = pd.read_sql(sql, conn)

    return df


def pull_data(config_path, sql, prefix, p, refresh, d):
    """
    pull data from Metabase DB
    input: SQL script and connection object 
    output: pandas dataframe
    """
    # Load configurations
    config = read_params(config_path)
    project_dir = config["project_dir"]
    raw_data_path = config[f"{d}_data_config"][f"{p}_raw_data_parquet"]
    dwh_credentials = config["db_credentials"]

    if refresh == True:
        # Logs
        print(f'Currently pulling {p} data set ...')

        # Start time
        start = dt.datetime.now()

        # Pull data set
        df = query_dwh(sql, dwh_credentials, prefix, project_dir)

        # Logs
        print(f'Time taken is {(dt.datetime.now() - start).seconds} seconds ...')

        # Save snapshot
        df.to_parquet(project_dir + raw_data_path, index=False)
    else:
        # Logs
        print(f'Currently loading {p} data set ...')

        # Start time
        start = dt.datetime.now()

        # Load snapshot
        df = pd.read_parquet(project_dir + raw_data_path)

        # Logs
        print(f'Time taken is {(dt.datetime.now() - start).seconds} seconds ...')
    
    # Logs
    if p in ['tss_validate_push', 'sr_validate_push']:
        display(df.sample(1))
        print('---------------------------------------------------------------------------------------------------------------------------------------')
    else:
        display(df.sample(2))
        print('---------------------------------------------------------------------------------------------------------------------------------------')
    
    return df


# Run code
if __name__ == "__main__":
    # Parameter arguments
    args = argparse.ArgumentParser()
    args.add_argument("--config", default="params.yaml")
    parsed_args = args.parse_args()

    # Run
    df_lftsv = pull_data(parsed_args.config, lftsv_sql(), 'DWH', 'lftsv', read_params(parsed_args.config)["refresh_config"][f"weekly_refresh"], 'raw')
    df_td = pull_data(parsed_args.config, td_sql(extract_end_date), 'DWH', 'td', read_params(parsed_args.config)["refresh_config"][f"weekly_refresh"], 'raw')
    df_waiver = pull_data(parsed_args.config, waiver_sql(extract_end_date), 'DWH', 'waiver', read_params(parsed_args.config)["refresh_config"][f"weekly_refresh"], 'raw')
    df_bcsv = pull_data(parsed_args.config, bcsv_sql(), 'DWH', 'bcsv', read_params(parsed_args.config)["refresh_config"][f"weekly_refresh"], 'raw')
    # df_meta = pull_data(parsed_args.config, metabase_sql(), 'DWH', 'metabase', read_params(parsed_args.config)["refresh_config"][f"weekly_refresh"], 'raw')
    df_meta_trxn = pull_data(parsed_args.config, metabase_trxn_sql(), 'DWH', 'metabase_trxn', read_params(parsed_args.config)["refresh_config"][f"weekly_refresh"], 'raw')
    df_meta_amount = pull_data(parsed_args.config, metabase_amount_sql(), 'DWH', 'metabase_amount', read_params(parsed_args.config)["refresh_config"][f"meta_amount_refresh"], 'raw')
    df_idm = pull_data(parsed_args.config, idm_sql(), 'DWH', 'idm', read_params(parsed_args.config)["refresh_config"][f"weekly_refresh"], 'raw')
    df_blacklist = pull_data(parsed_args.config, blacklist_sql(), 'DWH', 'blacklist', read_params(parsed_args.config)["refresh_config"][f"weekly_refresh"], 'raw')
    df_blacklist_new = pull_data(parsed_args.config, blacklist_new_sql(), 'DWH', 'blacklist_new', read_params(parsed_args.config)["refresh_config"][f"weekly_refresh"], 'raw')
    df_sr_21_limits = pull_data(parsed_args.config, sr_21_limits_sql(), 'DWH', 'sr_21_limits', read_params(parsed_args.config)["refresh_config"][f"weekly_refresh"], 'raw')
    df_sr_latest_refresh_limits = pull_data(parsed_args.config, sr_latest_refresh_limits_sql(), 'DWH', 'sr_latest_refresh_limits', read_params(parsed_args.config)["refresh_config"][f"weekly_refresh"], 'raw')
    df_rmv = pull_data(parsed_args.config, rmv_sql(), 'DWH', 'rmv', read_params(parsed_args.config)["refresh_config"][f"weekly_refresh"], 'raw')
    df_ftd = pull_data(parsed_args.config, ftd_sql(), 'DWH', 'ftd', read_params(parsed_args.config)["refresh_config"][f"weekly_refresh"], 'raw')
    df_lrr = pull_data(parsed_args.config, lrr_sql(), 'DWH', 'lrr', read_params(parsed_args.config)["refresh_config"][f"weekly_refresh"], 'raw')
    df_rein = pull_data(parsed_args.config, rein_sql(), 'DWH', 'rein', read_params(parsed_args.config)["refresh_config"][f"weekly_refresh"], 'raw')
    df_sr_rein_last_limits = pull_data(parsed_args.config, sr_rein_last_limits_sql(), 'DWH', 'sr_rein_last_limits', read_params(parsed_args.config)["refresh_config"][f"weekly_refresh"], 'raw')
    # df_ftd_lftsv = pull_data(parsed_args.config, lrr_sql(), 'DWH', 'ftd_lftsv', read_params(parsed_args.config)["refresh_config"][f"weekly_refresh"], 'raw')
    # df_ftd_lftsv_behaviour = pull_data(parsed_args.config, lrr_sql(), 'DWH', 'ftd_lftsv_behaviour', read_params(parsed_args.config)["refresh_config"][f"weekly_refresh"], 'raw')
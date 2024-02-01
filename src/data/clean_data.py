# Import modules
import os
import sys


# Import custom modules
sys.path.append(os.path.join(os.getcwd(), ""))

from src.data.load_data import *


# Functions
def clean_dataset(config_path, p):
    """
    pull data from DWH
    input: SQL script and connection object 
    output: pandas dataframe
    """
    # Load configurations
    config = read_params(config_path)
    project_dir = config["project_dir"]
    raw_data_path = config["raw_data_config"][f"{p}_raw_data_parquet"]
    clean_data_path = config["processed_data_config"][f"{p}_clean_data_parquet"]
    features_in_scope = config["clean_data_config"][f"{p}_features_in_scope"]
    int_to_string_cols = config["clean_data_config"][f"{p}_int_to_string_cols"]
    test_accounts = config["test_accounts_config"]["store_number"]

    # Conditional
    if p in ["lftsv"]:
        non_null_cols = config["clean_data_config"][f"{p}_non_null_cols"]

    # Conditional
    if p in ["lftsv", "td", "metabase", "metabase_trxn", "metabase_amount", "bcsv"]:
        datetime_cols = config["clean_data_config"][f"{p}_datetime_cols"]
        logs_cols = config["clean_data_config"][f"{p}_logs_cols"]
        
    # Conditional
    if p in ["lftsv", "sr_latest_refresh_limits", "td"]:
        string_to_float_cols = config["clean_data_config"][f"{p}_string_to_float_cols"]
    
    # Logs
    print(f'\nCurrently cleaning {p} data set ...')
    
    # Load snapshot
    df = pd.read_parquet(project_dir + raw_data_path)

    # Convert column headers to lower case, replace whitespaces and strip whitespaces
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

    # Select features in scope
    df = df[features_in_scope]

    # Convert specific int/float columns to string type
    df[int_to_string_cols] = df[int_to_string_cols].astype(str)

    # Conditional
    if p in ["lftsv", "td", "metabase", "metabase_trxn", "metabase_amount", "bcsv"]:
        # Convert date columns
        df[datetime_cols] = df[datetime_cols].apply(pd.to_datetime, errors='coerce')
    
    # Conditional
    if p in ["lftsv", 'td']:
        # Convert specific string columns to integer
        df[string_to_float_cols[-1]] = df[string_to_float_cols[-1]].astype('float', errors='ignore')
    
    # Conditional
    if p in ["lftsv", "sr_latest_refresh_limits", 'td']:
        # Convert specific string columns to float, first by leaving out the non-targeted columns
        df[string_to_float_cols] = df[string_to_float_cols].apply(pd.to_numeric, errors='coerce')

    # Conditional
    if p == "lftsv":
        # Drop all loan records that have irrelevant status keys OR were never disbursed
        # df.drop(df[(df["loan_status"] == 0) |
        #            (df["loan_status"] == 100) |
        #            (df["loan_status"] == 400) |
        #            (df["loan_status"] == 500)].index,
        #         inplace=True)

        # Replace None type datatype with Nan values across entire df
        df.fillna(np.nan, inplace=True)
        
        # Drop all rows where loan id and disbursement date is blank
        for col in non_null_cols:
            df = df.loc[df[col].notnull()]
        
        # Handle missing values
        # df.fillna(np.nan, inplace=True)
        
        # Convert specific string columns to float, first by leaving out the non-targeted columns
        df[string_to_float_cols] = df[string_to_float_cols].apply(pd.to_numeric, errors='coerce')

        # Clean up mobile number column
        df[["client_mobile_number", "temp"]] = df["client_mobile_number"].astype("str").str.split(".", expand=True)
        df.drop(["temp"], axis=1, inplace=True)
        
        # Sort out minor Mifos errors relating to loans with status 700
        df.loc[(df["loan_status"] == 700) & (df["closed_on_date"].isnull()), "closed_on_date"] = df["expected_matured_on_date"]
    elif p == "metabase":
        # Trim df to only contain past 30 days trx
        # df = df[df["transaction_time"] > dt.datetime.now() - pd.to_timedelta("31day")]

        # Drop any rows with store number being blank
        df = df[df["store_number"].notnull()]

        # Drop any duplicate transactions
        df = df[~df["transaction_id"].duplicated()]

        # Strip transaction time column to only include Y-m-d
        # This converts datetime column to string
        df["transaction_time"] = df["transaction_time"].apply(lambda x: x.strftime("%Y-%m-%d"))
    elif p == "bcsv":
        # Drop any duplicate store numbers
        df = df.drop_duplicates(subset=['store_number'], keep='last')
    elif p == "idm":
        # Drop any rows with national id being blank
        df = df[df["national_id"].notnull()]

        # Drop any rows with IDM recommendation being blank
        df = df[df["idm_recommendation"].notnull()]

        # Remove any whitespaces that may cause issues for specific column
        df["national_id"] = df["national_id"].apply(lambda x: x.split(".")[0])
        df["national_id"] = df["national_id"].apply(lambda x: x.split(" ")[0])
    elif p == "blacklist":
        df.rename(columns={features_in_scope[0]: "store_number", features_in_scope[1]: "national_id"}, inplace=True)
    elif p == "sr_21_limits":
        df.fillna(0, inplace=True)
    elif p == "sr_latest_refresh_limits":
        # Drop any duplicate store numbers
        df = df.drop_duplicates(subset="store_number", keep="first")
        df.fillna(0, inplace=True)
    elif p == "metabase_trxn":
        df["actual_trx_days"].fillna(0, inplace=True)
        df["most_recent_trx_date_past_30_days"] = df["most_recent_trx_date_past_30_days"].apply(lambda x: x.strftime("%Y-%m-%d"))
        df["last_trx_date"] = df["last_trx_date"].apply(lambda x: x.strftime("%Y-%m-%d"))
    elif p == "td":
        # Convert specific string columns to float, first by leaving out the non-targeted columns
        df[string_to_float_cols] = df[string_to_float_cols].astype('float')
    elif p in ['ftd', 'lrr', 'rein']:
        # Drop any duplicate store numbers
        df = df.drop_duplicates(subset=['store_number'], keep='last')
    elif p == 'rmv':
        # Replace NA values with 0
        df.fillna(0, inplace=True) 
    elif p == 'metabase_amount':
        df.set_index(datetime_cols, inplace=True)
        df_resample_wide = df.groupby([pd.Grouper(freq='D'), 'store_number'])['trx_val'].sum().unstack('store_number').fillna(0)
        df_wide = df_resample_wide.melt(ignore_index=False, value_name='trx_val').reset_index()
        # display(df_wide[df_wide['store_number'] == '000030'].iloc[20:40])
        df = df_wide.groupby(['store_number'])['trx_val'].median().reset_index()
        df['approx_30_days_trx_val'] = df['trx_val'] * 30
        df.drop(columns=['trx_val'], inplace=True)

    # Conditional
    if p in ["lftsv", "metabase", "metabase_trxn"]:
        # Convert transaction time column to datetime
        df[datetime_cols] = df[datetime_cols].apply(pd.to_datetime, errors='coerce')
    
    # Drop test accounts
    if "store_number" in df.columns:
        df = df[~df['store_number'].isin(test_accounts)]

    # Save snapshot
    df.to_parquet(project_dir + clean_data_path, index=False)
    
    # Conditional
    if p in ["lftsv", "metabase"]:
        # Logs
        print('Analysis start date {}'.format(df[logs_cols].min()))
        print('Analysis latest date {}'.format(df[logs_cols].max()))
    
    # Logs
    if p in ['lrr']:
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
    df_lftsv_clean = clean_dataset(parsed_args.config, 'lftsv')
    df_td_clean = clean_dataset(parsed_args.config, 'td')
    df_waiver_clean = clean_dataset(parsed_args.config, 'waiver')
    df_bcsv_clean = clean_dataset(parsed_args.config, 'bcsv')
    # df_meta_clean = clean_dataset(parsed_args.config, 'metabase')
    df_meta_trxn_clean = clean_dataset(parsed_args.config, 'metabase_trxn')
    df_meta_amount_clean = clean_dataset(parsed_args.config, 'metabase_amount')
    df_idm_clean = clean_dataset(parsed_args.config, 'idm')
    df_blacklist_clean = clean_dataset(parsed_args.config, 'blacklist')
    df_blacklist_new_clean = clean_dataset(parsed_args.config, 'blacklist_new')
    df_sr_21_limits_clean = clean_dataset(parsed_args.config, 'sr_21_limits')
    df_sr_latest_refresh_limits_clean = clean_dataset(parsed_args.config, 'sr_latest_refresh_limits')
    df_rmv_clean = clean_dataset(parsed_args.config, 'rmv')
    df_ftd_clean = clean_dataset(parsed_args.config, 'ftd')
    df_lrr_clean = clean_dataset(parsed_args.config, 'lrr')
    df_rein_clean = clean_dataset(parsed_args.config, 'rein')
    sr_rein_last_limits_clean = clean_dataset(parsed_args.config, 'sr_rein_last_limits')
    # df_ftd_lftsv_clean = clean_dataset(parsed_args.config, 'ftd_lftsv')
    # df_ftd_lftsv_behaviour_clean = clean_dataset(parsed_args.config, 'ftd_lftsv_behaviour')
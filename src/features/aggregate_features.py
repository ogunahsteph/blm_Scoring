# Import modules
import os
import sys


# Import custom modules
sys.path.append(os.path.join(os.getcwd(), ""))

from src.features.lftsv_feature_engineering import *


# Functions

# LFTSV
# ===========

def disbursed_amount_snapshot(agg_summary, df, term_frequencies, dd=0):
    for tf in term_frequencies:
        # get loans per term frequency
        df_tf = df[(df['term_frequency'] == tf) & (df['disbursed_on_date'] <= (date_in_scope + dt.timedelta(days=dd)))]

        if dd == 0:
            latest_tf_loan = df_tf.loc[df_tf.groupby('store_number')['loan_rank'].idxmax(), ['store_number', 'principal_disbursed']].rename(columns = {"principal_disbursed": f"latest_{tf}_loan"})

            agg_summary = agg_summary.merge(latest_tf_loan, on=['store_number'], how='left')
            agg_summary[f'latest_{tf}_loan'] = agg_summary[f'latest_{tf}_loan'].fillna(0)
        elif dd < 0:
            latest_tf_loan = df_tf.loc[df_tf.groupby('store_number')['loan_rank'].idxmax(), ['store_number', 'principal_disbursed']].rename(columns = {"principal_disbursed": f"snapshot_3m_{tf}_loan"})

            agg_summary = agg_summary.merge(latest_tf_loan, on=['store_number'], how='left')
            agg_summary[f'snapshot_3m_{tf}_loan'] = agg_summary[f'snapshot_3m_{tf}_loan'].fillna(0)
    
    return agg_summary


def get_max_principal_disbursed(df):
    # Aggregate maximum principal disbursed for each client id
    max_principal_amount = (df.groupby("store_number")["principal_disbursed"].max().rename("max_principal_amount").reset_index())
    
    return max_principal_amount


def get_max_principal_disbursement_date(df):
     # Get df for when a customer got their max loan principal
    max_principal_dates = df.sort_values(["principal_disbursed", 'disbursed_on_date'], ascending=[False, False]).groupby("store_number").first().reset_index()
    
    # Trim df to only remain with relevant columns
    max_principal_dates = max_principal_dates[["store_number", "disbursed_on_date"]]

    # Rename column to make it clearer
    max_principal_dates.rename(columns = {"disbursed_on_date": "max_loan_disbursement_date"}, inplace = True)
    
    return max_principal_dates


def get_agg_good_loans_old(df):
    # Aggregate of loans well paid or in good standing
    agg_good_loans = (df.loc[(df["loan_repayment_status"] == "closed_early_repayment") |
                             (df["loan_repayment_status"] == "closed_early_repayment_overpaid") |
                             (df["loan_repayment_status"] == "closed_on_time") |
                             (df["loan_repayment_status"] == "closed_on_time_overpaid") |
                             (df["loan_repayment_status"] == "current_active")]).groupby("store_number")["loan_id_product_concat"].aggregate("count").rename("count_good_loans").reset_index()
    
    return agg_good_loans


def get_agg_good_loans(df):
    agg_good_loans = (df.loc[((df['total_repayment_vs_principal_amount'] > 1) & (df['days_diff_maturity_max_trans'] <= 15)) |
                             (df['loan_repayment_status'] == 'current_active')]).groupby(["store_number"], as_index=False).agg({"loan_id_product_concat": "count"})
    agg_good_loans.rename(columns = {"loan_id_product_concat": "count_good_loans"}, inplace = True)

    return agg_good_loans


def get_inference_label(good_loans_repayment_ratio_threshold, target_col):
    """
    Function to assess the weight to be assigned based on good loans repayment ratio i.e num of loans paid within tenure\
    for customers who qualify for limit stabilization
    
    Inputs:   
    1) The inference column indicating whether rules are to be relaxed OR not,
    2) Good loans repayment ratio tracking column i.e. ratio of num of loans paid within tenure vs total num loans taken
    
    Outputs:
    A column assigning the assigned weight for good loans repayment ratio
    """
    
    # Conditions
    conditions = [target_col.ge(good_loans_repayment_ratio_threshold),
                  target_col.lt(good_loans_repayment_ratio_threshold)]
    
    # Choices
    choices = ["relax_rules",
               "No_rules_relaxed"]
    
    # Inference column
    inference_col = np.select(conditions, choices)
    
    return inference_col


def get_weight_dpd(dpd_col, inference_col, inference_col_target):
    """
    Function to assess the weight to be assigned based on days past due for customers who qualify for limit stabilization
    
    Inputs:   
    1) The inference column indicating whether rules are to be relaxed OR not,
    2) Days past due tracking column i.e. num of days past due
    
    Outputs:
    A column assigning the assigned weight for days past due
    """   
    # Conditions
    conditions = [inference_col.str.match(inference_col_target) & dpd_col.lt(30),
                  inference_col.str.match(inference_col_target) & dpd_col.ge(30) & dpd_col.lt(35),
                  inference_col.str.match(inference_col_target) & dpd_col.ge(35) & dpd_col.lt(38),
                  inference_col.str.match(inference_col_target) & dpd_col.ge(38) & dpd_col.lt(41),
                  dpd_col.gt(41)]
    
    # Choices
    choices = [1,
               0.9,
               0.8,
               0.7,
               0]
    
    # Weight DPD feature
    weight_dpd_col = np.select(conditions, choices)
    
    return weight_dpd_col


def get_max_days_past_due(df):
    max_days_past_due = df.groupby("store_number")["days_past_due"].max().rename("max_days_past_due").reset_index()
    
    return max_days_past_due


def get_last_3m_records(df, min_loan_amount):
    # Last 3 months threshold
    target_analysis_period = df["disbursed_on_date"].max() - pd.DateOffset(months=3)
    
    # Filter records in scope
    loans_past_3_months = df[(df["disbursed_on_date"] >= target_analysis_period) & (df["principal_disbursed"] >= min_loan_amount)]
    
    return loans_past_3_months


def get_last_3m_loan_cnts(df, min_loan_amount):
    # Records in scope
    loans_past_3_months = get_last_3m_records(df, min_loan_amount)

    # Last 3 months loan count feature
    loan_count_past_3_months = loans_past_3_months.groupby(["store_number"]).agg({"store_number": 'size', 'term_frequency': 'mean'}) \
                                .rename(columns={'store_number': "loan_count_past_3_months", 'term_frequency': 'avg_loan_tenure'})
    loan_count_past_3_months['avg_loan_tenure'] =  round(loan_count_past_3_months['avg_loan_tenure'], 0)
    
    # Last 3 months 7 day loan count feature
    count_7_day_loans = loans_past_3_months[loans_past_3_months["term_frequency"] == 7].groupby(["store_number"]).agg({"store_number": 'size', 'principal_disbursed': ['min', 'sum', 'mean']})
    count_7_day_loans.columns = ['_'.join(col).strip('_') for col in count_7_day_loans.columns.values]
    count_7_day_loans.rename(columns={"store_number_size": "count_7_day_loans", "principal_disbursed_min": "minimum_7_day_principal_disbursed", 
                "principal_disbursed_sum": "total_sum_7_day_principal_disbursed", 'principal_disbursed_mean': 'avg_7_day_principal_disbursed'}, inplace=True)

    # Last 3 months 1 day loan count feature
    count_1_day_loans = loans_past_3_months[loans_past_3_months["term_frequency"] == 1].groupby(["store_number"]).agg({'principal_disbursed': 'sum'}) \
                        .rename(columns={"principal_disbursed": "total_sum_1_day_principal_disbursed"})

    # Merge aggregates
    merged_df = loan_count_past_3_months.merge(count_7_day_loans, how='outer', left_index=True, right_index=True)
    merged_df = merged_df.merge(count_1_day_loans, how='outer', left_index=True, right_index=True).reset_index().rename(columns={'index': 'store_number'})
    merged_df['store_number'] = merged_df['store_number'].astype('str')

    return merged_df


def get_count_7_day_loans_paid_upto_rollover(df, min_loan_amount):
    # Records in scope
    loans_past_3_months = get_last_3m_records(df, min_loan_amount)

    # Filter records in scope
    loans_past_3_months = loans_past_3_months[(loans_past_3_months["term_frequency"]==7) & 
                                              ((loans_past_3_months["loan_repayment_status"]== "closed_early_repayment") |
                                               (loans_past_3_months["loan_repayment_status"]== "closed_early_repayment_overpaid") |
                                               (loans_past_3_months["loan_repayment_status"]== "closed_on_time") |
                                               (loans_past_3_months["loan_repayment_status"]== "closed_on_time_overpaid") |
                                               (loans_past_3_months["loan_repayment_status"]== "current_active") |
                                               (loans_past_3_months["loan_repayment_status"]== "active_rollover") |
                                               (loans_past_3_months["loan_repayment_status"]== "closed_rollover") |
                                               (loans_past_3_months["loan_repayment_status"]== "closed_rollover_overpaid"))]
    
    # Count of 7 day loans paid upto rollover in last 3 months feature
    count_7_day_loans_paid_upto_rollover = loans_past_3_months.groupby(["store_number"])["client_mifos_id"].aggregate("count").rename("count_7_day_loans_paid_upto_rollover").reset_index()
    
    return count_7_day_loans_paid_upto_rollover


def get_total_outstanding(df):
    # Aggregate total outstanding for each client id
    df_outstanding = df.groupby(["store_number"]).agg({"total_outstanding": "sum" , "safaricom_loan_balance": "sum"}) # TODO
    df_outstanding.rename(columns={'total_outstanding': 'total_outstanding_sum', 'safaricom_loan_balance': 'safaricom_loan_balance_sum'}, inplace=True) # TODO
    df_outstanding['loan_balance'] = np.where(df_outstanding['safaricom_loan_balance_sum'].isna(), df_outstanding['total_outstanding_sum'], df_outstanding['safaricom_loan_balance_sum'])
    
    return df_outstanding


def lftsv_agg_summaries(config_path, tday):
    # Load configurations
    config = read_params(config_path)
    project_dir = config["project_dir"]
    engineered_features_path = config["interim_data_config"]["engineered_features_parquet"]
    term_frequencies = config["crb_limit_factor_config"]["term_frequencies"]
    snaphot_period = config["snaphot_period_config"]
    loans_summary_data_path = config["interim_data_config"]["loans_summary_data_parquet"]
    good_loans_repayment_ratio_threshold = config["good_loans_repayment_ratio_config"]
    cols_fillna = config["clean_data_config"]["lftsv_cols_fillna"]
    min_loan_amount = config["clean_data_config"]["min_loan_amount"]
    lftsv_features_in_scope_clean = config["clean_data_config"]["lftsv_features_in_scope_clean"]
    max_dpd_30_data_path = config["interim_data_config"]["max_dpd_30_data_parquet"]

    # Load snapshot
    df = pd.read_parquet(project_dir + engineered_features_path)
    print('\nAll loans:\n')
    display(df.loc[df['store_number'] == '7491730', ["store_number", "loan_rank", "principal_disbursed", "disbursed_on_date", "safaricom_loan_balance", "total_outstanding"]])
    print('---------------------------------------------------------------------------------------------------------------------------------------')

    # Sort values
    df = df.sort_values(by=["store_number", "loan_rank", "safaricom_loan_balance", "total_outstanding"], ascending=[True, True, False, False])
    
    # Latest loan record
    agg_summary = df.loc[df.groupby('store_number')['loan_rank'].idxmax()].reset_index()

    # # Get loans per term frequency
    # df_21 = df[df['term_frequency'] == 21]
    # df_7 = df[df['term_frequency'] == 7]
    # df_1 = df[df['term_frequency'] == 1]
    
    # # Latest 21-day loan
    # latest_21_loan = df_21.loc[df_21.groupby('store_number')['loan_rank'].idxmax()].reset_index()
    # latest_21_loan = latest_21_loan[['store_number', 'principal_disbursed']]
    # latest_21_loan.rename(columns = {"principal_disbursed": "latest_21_loan"}, inplace = True)

    # # Latest 7-day loan
    # latest_7_loan = df_7.loc[df_7.groupby('store_number')['loan_rank'].idxmax()].reset_index()
    # latest_7_loan = latest_7_loan[['store_number', 'principal_disbursed']]
    # latest_7_loan.rename(columns = {"principal_disbursed": "latest_7_loan"}, inplace = True)

    # # Latest 1-day loan
    # latest_1_loan = df_1.loc[df_1.groupby('store_number')['loan_rank'].idxmax()].reset_index()
    # latest_1_loan = latest_1_loan[['store_number', 'principal_disbursed']]
    # latest_1_loan.rename(columns = {"principal_disbursed": "latest_1_loan"}, inplace = True)
    
    # agg_summary = pd.merge(agg_summary, latest_21_loan, on='store_number', how='left')
    # agg_summary = pd.merge(agg_summary, latest_7_loan, on='store_number', how='left')
    # agg_summary = pd.merge(agg_summary, latest_1_loan, on='store_number', how='left')
    
    # agg_summary['latest_21_loan'] = agg_summary['latest_21_loan'].fillna(0)
    # agg_summary['latest_7_loan'] = agg_summary['latest_21_loan'].fillna(0)
    # agg_summary['latest_1_loan'] = agg_summary['latest_21_loan'].fillna(0)

    # Retrieve the latest disbursed loan per tenure
    agg_summary = disbursed_amount_snapshot(agg_summary, df, term_frequencies)

    # Retrieve the latest disbursed loan per tenure three months ago
    agg_summary = disbursed_amount_snapshot(agg_summary, df, term_frequencies, dd=snaphot_period)
    print('\nLoan snapshots:\n')
    display(agg_summary.loc[agg_summary['store_number'] == '7105817'])
    print('---------------------------------------------------------------------------------------------------------------------------------------')
    
    # Drop duplicates introduced by loan rank
    print('\nLatest loan:\n')
    display(agg_summary.loc[agg_summary['store_number'] == '7491730', ["store_number", "loan_rank", "principal_disbursed", "safaricom_loan_balance", "total_outstanding"]])
    print('---------------------------------------------------------------------------------------------------------------------------------------')
    
    # Maximum principal amount feature
    agg_summary = pd.merge(agg_summary, get_max_principal_disbursed(df), on="store_number")

    # Maximum loan disbursement date
    agg_summary = pd.merge(agg_summary, get_max_principal_disbursement_date(df), on="store_number")

    # Delete test accounts
    # agg_summary = agg_summary[agg_summary["max_principal_amount"] >= 200]
    
    # Count of good loans feature
    # agg_summary = pd.merge(agg_summary, get_agg_good_loans(df), how="outer", on="store_number")
    agg_summary = agg_summary.merge(get_agg_good_loans(df), how="outer", on="store_number")
    
    # Good loans repayment ratio faeture
    agg_summary["good_loans_repayment_ratio"] = round(agg_summary["count_good_loans"] / agg_summary["loan_count"], 2)
    agg_summary["good_loans_repayment_ratio"].fillna(0, inplace=True)
    
    # Number of days since last disbursement feature
    agg_summary["num_days_since_last_disbursement"] = pd.to_numeric((tday - agg_summary["disbursed_on_date"]).dt.days, downcast='integer')
    
    # Inference column feature
    agg_summary["inference_col"] = get_inference_label(good_loans_repayment_ratio_threshold, agg_summary["good_loans_repayment_ratio"])
    
    # Weight DPD feature
    agg_summary["weight_dpd"] = get_weight_dpd(agg_summary["days_past_due"], agg_summary["inference_col"], "relax_rules")

    # Max days past due feature
    agg_summary = pd.merge(agg_summary, get_max_days_past_due(df), how="left", on="store_number")

    # Get a slice of df where customers have a max dpd of 30 days past due
    max_dpd_30 = agg_summary[agg_summary["max_days_past_due"] <= 30]

    # Last 3 months loan count aggregate view features
    agg_summary = pd.merge(agg_summary, get_last_3m_loan_cnts(df, min_loan_amount), how="left", on="store_number")

    # Number of 7 day loans paid upto rollover feature
    agg_summary = pd.merge(agg_summary, get_count_7_day_loans_paid_upto_rollover(df, min_loan_amount), how="left", on="store_number")

    # Calculate good repayment ratios for the borrowers
    agg_summary["good_loans_repayment_ratio(7_day_loans)"] = round(agg_summary["count_7_day_loans_paid_upto_rollover"] / agg_summary["count_7_day_loans"], 2)

    # Total outstanding balance feature
    agg_summary = agg_summary.merge(get_total_outstanding(df), how="left", on=["store_number"]) # TODO

    # Impute missing values with zero
    for col in cols_fillna:
        agg_summary[col].fillna(0, inplace=True)

    # Features in scope
    agg_summary = agg_summary[lftsv_features_in_scope_clean]
    
    # Export summaries
    max_dpd_30.to_parquet(project_dir + max_dpd_30_data_path, index=False)
    agg_summary.to_parquet(project_dir + loans_summary_data_path, index=False)

    # Logs
    display(agg_summary.sample(2))
    print('---------------------------------------------------------------------------------------------------------------------------------------')
    
    return agg_summary


# Metabase
# ===========

def meta_agg_summaries(config_path):
    # Load configurations
    config = read_params(config_path)
    project_dir = config["project_dir"]
    metabase_trxn_clean_data_path = config["processed_data_config"][f"metabase_trxn_clean_data_parquet"]
    metabase_amount_clean_data_path = config["processed_data_config"][f"metabase_amount_clean_data_parquet"]
    tills_summary_data_path = config["interim_data_config"]["tills_summary_data_parquet"]

    # Load snapshot
    agg_summary = pd.read_parquet(project_dir + metabase_trxn_clean_data_path)
    df_amount = pd.read_parquet(project_dir + metabase_amount_clean_data_path)
    
    # Transaction features
    # agg_summary = df.groupby(["store_number"], as_index=False).agg({"transaction_time": ["min", "max", "nunique"]})
    # agg_summary.columns = ['_'.join(col).strip('_') for col in agg_summary.columns.values]
    # agg_summary.rename(columns={"transaction_time_min": "most_recent_trx_date_past_30_days",  #TODO
    #                             "transaction_time_max": "last_trx_date", 
    #                             "transaction_time_nunique": "actual_trx_days"},
    #                    inplace=True)
    
    # Merge data sets
    agg_summary = agg_summary.merge(df_amount, how="outer", on=["store_number"])

    # Handle missing values
    agg_summary['actual_trx_days'].fillna(0, inplace=True)
    agg_summary["actual_trx_days"] = agg_summary["actual_trx_days"].astype('int')

    # Export summaries
    agg_summary.to_parquet(project_dir + tills_summary_data_path, index=False)
    
    # Logs
    display(agg_summary.sample(2))
    
    return agg_summary


# Run code
if __name__ == "__main__":
    # Parameter arguments
    args = argparse.ArgumentParser()
    args.add_argument("--config", default="params.yaml")
    parsed_args = args.parse_args()

    # Run
    df_lftsv_aggregate = lftsv_agg_summaries(parsed_args.config, extract_end_date)
    df_meta_aggregate = meta_agg_summaries(parsed_args.config)
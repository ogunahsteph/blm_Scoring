# Import modules
import os
import sys


# Import custom modules
sys.path.append(os.path.join(os.getcwd(), ""))

from src.features.build_features import *


# Functions
def get_max_principal_disbursed(df):
    # Aggregate maximum principal disbursed for each client id
    max_principal_amount = (df.groupby("store_number")["principal_disbursed"].max().rename("max_principal_amount").reset_index())
    
    return max_principal_amount


def get_max_principal_disbursment_date(df):
     # Get df for when a customer got their max loan principal
    max_principal_dates = df.sort_values("principal_disbursed", ascending=False).groupby("store_number").first().reset_index()
    
    # Trim df to only remain with relevant columns
    max_principal_dates = max_principal_dates[["store_number", "disbursed_on_date"]]

    # Rename column to make it clearer
    max_principal_dates.rename(columns = {"disbursed_on_date": "max_loan_disbursement_date"}, inplace = True)
    
    return max_principal_dates


def get_agg_good_loans(df):
    # Aggregate of loans well paid or in good standing
    agg_good_loans = (df.loc[(df["loan_repayment_status"] == "closed_early_repayment") |
                             (df["loan_repayment_status"] == "closed_early_repayment_overpaid") |
                             (df["loan_repayment_status"] == "closed_on_time") |
                             (df["loan_repayment_status"] == "closed_on_time_overpaid") |
                             (df["loan_repayment_status"] == "current_active")]).groupby("store_number")["loan_id_product_concat"].aggregate("count").rename("count_good_loans").reset_index()
    
    return agg_good_loans


def get_max_days_past_due(df):
    # Aggregate maximum dpd for each client

    max_days_past_due = (df.groupby("store_number")["days_past_due"].max().rename("max_days_past_due").reset_index())
    
    return max_days_past_due


def get_count_7_day_loans(loans_past_3_months):
    #get summary of number of 7 day loans a customer has taken

    count_7_day_loans = (loans_past_3_months.groupby("store_number")["count_7_day_loans"].max().rename("count_7_day_loans").reset_index())
        
    return count_7_day_loans


def get_count_7_day_loans_paid_upto_rollover(loans_past_3_months):
    #merge df to get number of 7 day loans paid upto rollover
    count_7_day_loans_paid_upto_rollover = (loans_past_3_months.loc[loans_past_3_months["term_frequency"]==7].loc[
                                           (loans_past_3_months["loan_repayment_status"]== "closed_early_repayment")|
                                           (loans_past_3_months["loan_repayment_status"]== "closed_early_repayment_overpaid")|
                                           (loans_past_3_months["loan_repayment_status"]== "closed_on_time")|
                                           (loans_past_3_months["loan_repayment_status"]== "closed_on_time_overpaid")|
                                           (loans_past_3_months["loan_repayment_status"]== "current_active")|
                                           (loans_past_3_months["loan_repayment_status"]== "active_rollover")|
                                           (loans_past_3_months["loan_repayment_status"]== "closed_rollover")|
                                           (loans_past_3_months["loan_repayment_status"]== "closed_rollover_overpaid")
]).groupby("store_number")["client_mifos_id"].aggregate("count").rename("count_7_day_loans_paid_upto_rollover").reset_index()
    
    return count_7_day_loans_paid_upto_rollover


def get_minimum_7_day_principal_disbursed(loans_past_3_months):
    # get minimum amount disbursed for a 7 day loan
    minimum_7_day_principal_disbursed = loans_past_3_months.loc[loans_past_3_months["term_frequency"]==7].groupby("store_number")["principal_disbursed"].min().rename("minimum_7_day_principal_disbursed").reset_index()
    
    return minimum_7_day_principal_disbursed


def get_total_sum_7_day_principal_disbursed(loans_past_3_months):
    # get minimum amount disbursed for a 7 day loan
    total_sum_7_day_principal_disbursed = loans_past_3_months.loc[loans_past_3_months["term_frequency"]==7].groupby("store_number")["principal_disbursed"].sum().rename("total_sum_7_day_principal_disbursed").reset_index()
    
    return total_sum_7_day_principal_disbursed


def get_avg_7_day_principal_disbursed(loans_past_3_months):
    #get the average 7 day loan principal ==> proxy for 7 day limit
    avg_7_day_principal_disbursed = loans_past_3_months.loc[loans_past_3_months["term_frequency"]==7].groupby("store_number")["principal_disbursed"].mean().rename("avg_7_day_principal_disbursed").reset_index()
    
    return avg_7_day_principal_disbursed


def approximate_21_day_limit(df):
    """
    """
    
    seven_day_limit = df["avg_7_day_principal_disbursed"]
    seven_day_limit_factor = 0.17
    twenty_one_day_limit_factor = 0.5
    product_cap = 200000
    zero = 0
    
    operation = (np.ceil(((df["avg_7_day_principal_disbursed"]*twenty_one_day_limit_factor)/seven_day_limit_factor)/100) * 100).astype(int)
    
    conditions = [
        seven_day_limit.eq(zero),
        operation.le(product_cap),
        operation.gt(product_cap),
    ]
    
    choices = [
        zero,
        operation,
        product_cap,
    ]
    
    limit_col = np.select(conditions, choices)
    
    return limit_col


def agg_summaries(config_path, df, tday):
    # Load configurations
    config = read_params(config_path)
    loans_summary_data_path = config["interim_data_config"]["loans_summary_data_parquet"]
    #inference_summary_data_path = config["interim_data_config"]["inference_summary_data_parquet"]

    # Return df with two most recent loans for each client id
    # agg_summary = df.groupby("client_id").head(2).reset_index()
    
    # Return df with the most recent loan for each borrower
    # agg_summary = df.groupby("store_number").head(1).reset_index()
    agg_summary = df.loc[df.groupby('store_number')['loan_rank'].idxmax()].reset_index()
    
    # Trim df to only relevant columns
    target_cols = ["client_mobile_number", "store_number", "loan_count", "loan_status", "term_frequency",
                   "principal_disbursed", "principal_repaid", "disbursed_on_date",
                   "expected_matured_on_date", "closed_on_date", "due_date_fixed",
                   "days_past_due", "bloom_version", "loan_repayment_status"]
    agg_summary = agg_summary[target_cols]
    
    # Aggregate maximum principal disbursed for each client id
    agg_summary = pd.merge(agg_summary, get_max_principal_disbursed(df), on="store_number")
    
    # Delete test accounts
    agg_summary = agg_summary[agg_summary["max_principal_amount"] >= 200]
    
    # Get feature for when a customer got their max loan principal
    agg_summary = pd.merge(agg_summary, get_max_principal_disbursment_date(df), on="store_number")
    
    # Merge agg_summary & agg good loans
    agg_summary = pd.merge(agg_summary, get_agg_good_loans(df), how="outer", on="store_number")
    
    # Merge agg_summary & max days past due
    agg_summary = pd.merge(agg_summary, get_max_days_past_due(df), how="outer", on="store_number")
    
    # Fill rest of missing values with zeros
    cols_fillna = ['count_good_loans']
    for col in cols_fillna:
        agg_summary[col].fillna(0, inplace=True)
    
    # Calculate good repayment ratios for the borrowers
    agg_summary["good_loans_repayment_ratio"] = round(agg_summary["count_good_loans"] / agg_summary["loan_count"], 2)
    
    # Calculate num days since last disbursement
    agg_summary["num_days_since_last_disbursement"] = pd.to_numeric((tday - agg_summary["disbursed_on_date"]).dt.days, downcast='integer')
    
    #delete customers without a store number
    agg_summary = agg_summary.loc[agg_summary["store_number"].notnull()]
    
    # Export summaries
    #agg_summary.to_parquet(loans_summary_data_path, index=False)
    #agg_summary.loc[agg_summary["store_number"].notnull()][["store_number","inference_col"]].to_parquet(inference_summary_data_path, index=False)
    
    #get a slice of df where customers have a max dpd of 30 days past due
    max_dpd_30 = agg_summary.loc[agg_summary["max_days_past_due"]<=30]
    max_dpd_30_list = list(max_dpd_30["store_number"].unique())

    # get slice of df for loans over the past 3 months i.e 90 days
    current_period = df["disbursed_on_date"].max()
    target_analysis_period = current_period - pd.DateOffset(months=3)
    min_loan_amount = 200

    loans_past_3_months = df.loc[(df["disbursed_on_date"]>=target_analysis_period)&
                                  (df["principal_disbursed"]>=min_loan_amount)
                                   ]
    
    #drop rows where store number is null
    loans_past_3_months = loans_past_3_months.loc[loans_past_3_months["store_number"].notnull()]
    
    #calculate num of loans taken past 3 months
    loans_past_3_months["loan_count_past_3_months"] = loans_past_3_months.groupby("store_number")["store_number"].transform('size')
    
    #create loan count column i.e adds a new column that captures the num of 7-day loans a customer has taken
    loans_past_3_months["count_7_day_loans"] = loans_past_3_months.loc[loans_past_3_months["term_frequency"]==7].groupby("store_number")["store_number"].transform('size')
    
    #fill rest of missing values with zeros
    cols_fillna = ["count_7_day_loans"]
    # replace 'NaN' with zero in these columns
    for col in cols_fillna:
        loans_past_3_months[col].fillna(0,inplace=True)
    
    #get list of all store numbers
    loans_past_3_months_list = list(loans_past_3_months["store_number"].unique())
    
    #get aggregate summary with avg loan tenure
    loans_past_3_months_summary = round(loans_past_3_months.groupby("store_number")["term_frequency"].aggregate("mean").rename("avg_loan_tenure").reset_index(), 0)
    
    # Merge agg_summary & agg good loans
    loans_past_3_months_summary = pd.merge(loans_past_3_months_summary, get_count_7_day_loans(loans_past_3_months), how="left", on="store_number")
    
    # Merge agg_summary & agg good loans
    loans_past_3_months_summary = pd.merge(loans_past_3_months_summary, get_count_7_day_loans_paid_upto_rollover(loans_past_3_months), how="left", on="store_number")

    #fill rest of missing values with zeros
    cols_fillna = ["count_7_day_loans_paid_upto_rollover"]
    # replace 'NaN' with zero in these columns
    for col in cols_fillna:
        loans_past_3_months_summary[col].fillna(0,inplace=True)
    
#calculate good repayment ratios for the borrowers
    loans_past_3_months_summary["good_loans_repayment_ratio(7_day_loans)"] = round(loans_past_3_months_summary["count_7_day_loans_paid_upto_rollover"]/loans_past_3_months_summary["count_7_day_loans"], 2)
    
    #fill rest of missing values with zeros
    cols_fillna = ["good_loans_repayment_ratio(7_day_loans)"]
    # replace 'NaN' with zero in these columns
    for col in cols_fillna:
        loans_past_3_months_summary[col].fillna(0,inplace=True)
        
     # Merge agg_summary & agg good loans
    loans_past_3_months_summary = pd.merge(loans_past_3_months_summary, get_minimum_7_day_principal_disbursed(loans_past_3_months), how="left", on="store_number")   
        
    #fill rest of missing values with zeros
    cols_fillna = ["minimum_7_day_principal_disbursed"]
    # replace 'NaN' with zero in these columns
    for col in cols_fillna:
        loans_past_3_months_summary[col].fillna(0,inplace=True)
        
     # Merge agg_summary & agg good loans
    loans_past_3_months_summary = pd.merge(loans_past_3_months_summary, get_total_sum_7_day_principal_disbursed(loans_past_3_months), how="left", on="store_number") 
        
    #fill rest of missing values with zeros
    cols_fillna = ["total_sum_7_day_principal_disbursed"]
    # replace 'NaN' with zero in these columns
    for col in cols_fillna:
        loans_past_3_months_summary[col].fillna(0,inplace=True)
        
     # Merge agg_summary & agg good loans
    loans_past_3_months_summary = pd.merge(loans_past_3_months_summary, get_avg_7_day_principal_disbursed(loans_past_3_months), how="left", on="store_number") 
    
    #fill rest of missing values with zeros
    cols_fillna = ["avg_7_day_principal_disbursed"]
    # replace 'NaN' with zero in these columns
    for col in cols_fillna:
        loans_past_3_months_summary[col].fillna(0,inplace=True)
        
    #execute function
    loans_past_3_months_summary["21_day_limit"] = approximate_21_day_limit(loans_past_3_months_summary)
    
    # Logs
    display(loans_past_3_months_summary.sample(2))
    
    return loans_past_3_months_summary


# Run code
if __name__ == "__main__":
    # Parameter arguments
    args = argparse.ArgumentParser()
    args.add_argument("--config", default="params.yaml")
    parsed_args = args.parse_args()

    # Run
    sql = processed_data_sql()
    df = pull_raw_data(parsed_args.config, sql)
    df = lftsv_feature_engineering(parsed_args.config, df, tday)
    df_aggregate = agg_summaries(parsed_args.config, df, tday)
# Import modules
import os
import sys


# Import custom modules
sys.path.append(os.path.join(os.getcwd(), ""))

from src.features.aggregate_features import *


# Functions
def merge_inference_data(agg_summary, loans_summary_data_path):
    # Load data frame with store number inferences
    inference_df = pd.read_parquet(loans_summary_data_path, columns=["store_number", "inference_col"])

    # Drop any rows with store number being blank
    inference_df = inference_df.loc[inference_df["store_number"].notnull()]

    # Change data type
    inference_df["store_number"] = inference_df["store_number"].astype("str")
    
    # Merge data frame to add inference column
    agg_summary = agg_summary.merge(inference_df, how="left", on="store_number")
    
    # Handle missing values
    cols_fillna = ["inference_col"]
    for col in cols_fillna:
        agg_summary[col].fillna("No_rules_relaxed", inplace=True) # TODO
    
    return agg_summary


def recency_check(config, inference_col, recency_col, inference_col_target, no_rules_relaxed_recency_threshold, rules_relaxed_recency_threshold):
    """
    Function to assess whether store number has recently been trading up to a certain allowed threshold 
        i.e. 5 days for those that don't qualify for limit stabilization and upto 7 days for those that qualify
    
    Inputs:   
    1) The inference column indicating whether rules are to be relaxed OR not,
    2) Recency tracking column i.e. num of days since store number last had a transaction
    3) Recency thresholds
    
    Outputs:
    A column denoting a boolean yes OR no wrt to whether a customer met the required recency threshold
    """

    # Load configurations
    transaction_boolean_accepted = config["recency_config"]["transaction_boolean_accepted"]
    transaction_boolean_rejected = config["recency_config"]["transaction_boolean_rejected"]

    # Conditions
    conditions = [recency_col.le(no_rules_relaxed_recency_threshold),
                  inference_col.str.match(inference_col_target) & recency_col.le(rules_relaxed_recency_threshold),
                  recency_col.gt(no_rules_relaxed_recency_threshold)]
    
    # Choices
    choices = [transaction_boolean_accepted,
               transaction_boolean_accepted,
               transaction_boolean_rejected]
    
    # Recency feature
    new_col = np.select(conditions, choices)
    
    return new_col


def weight_till_recency_func(inference_col, recency_col, inference_col_target, no_rules_relaxed_recency_threshold, rules_relaxed_recency_threshold, choices):
    """
    Function to assess the weight to be assigned based on till recency for customers who qualify for limit stabilization
    
    Inputs:   
    1) The inference column indicating whether rules are to be relaxed OR not,
    2) Recency tracking column i.e. num of days since store number last had a transaction
    
    Outputs:
    A column assigning the assigned weight for till recency
    """

    # Conditions
    conditions = [recency_col.le(no_rules_relaxed_recency_threshold),
                  inference_col.str.match(inference_col_target) & recency_col.le(no_rules_relaxed_recency_threshold),
                  inference_col.str.match(inference_col_target) & recency_col.gt(no_rules_relaxed_recency_threshold) & recency_col.le(no_rules_relaxed_recency_threshold + 1),
                  inference_col.str.match(inference_col_target) & recency_col.gt(no_rules_relaxed_recency_threshold + 1) & recency_col.le(rules_relaxed_recency_threshold),
                  recency_col.gt(rules_relaxed_recency_threshold)]
    
    # Recency weight feature
    weight_till_recency_col = np.select(conditions, choices)
    
    return weight_till_recency_col


def meta_feature_engineering(config_path, extract_end_date):
    # Load configurations
    config = read_params(config_path)
    project_dir = config["project_dir"]
    loans_summary_data_path = config["interim_data_config"]["loans_summary_data_parquet"]
    tills_summary_data_path = config["interim_data_config"]["tills_summary_data_parquet"]
    trxn_summary_data_path = config["interim_data_config"]["trxn_summary_data_parquet"]
    no_rules_relaxed_recency_threshold = config["recency_config"]["no_rules_relaxed_recency_threshold"]
    rules_relaxed_recency_threshold = config["recency_config"]["rules_relaxed_recency_threshold"]
    recency_weights = config["recency_config"]["recency_weights"]

    # Load snapshot
    agg_summary = pd.read_parquet(project_dir + tills_summary_data_path)

    # Expected transaction days feature
    agg_summary["expected_trx_days"] = ( (agg_summary["last_trx_date"] - agg_summary["most_recent_trx_date_past_30_days"]).dt.days ) + 1
    agg_summary["expected_trx_days"].fillna(0, inplace=True)
    print('\nExpected transaction days feature sample:\n', agg_summary.loc[agg_summary['store_number'] == '105714', ['store_number', 'last_trx_date', 'most_recent_trx_date_past_30_days', 'expected_trx_days']], sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')

    # Coerce data type
    agg_summary["actual_trx_days"] = agg_summary["actual_trx_days"].astype('int')
    agg_summary["expected_trx_days"] = agg_summary["expected_trx_days"].astype('int')
    
    # Consistency feature
    agg_summary["page_active_days"] = round(agg_summary["actual_trx_days"] / agg_summary["expected_trx_days"], 2)
    agg_summary["page_active_days"].fillna(0, inplace=True)
    print('\nConsistency feature sample:\n', agg_summary.loc[agg_summary['store_number'] == '105714', ['store_number', 'expected_trx_days', 'actual_trx_days', 'page_active_days']], sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')

    # Add inference column
    agg_summary = merge_inference_data(agg_summary, project_dir + loans_summary_data_path)

    # Number of days since last transaction feature
    agg_summary["days_since_last_trx"] = (extract_end_date - agg_summary["last_trx_date"]).dt.days # TODO
    print('\nNumber of days since last transaction feature sample:\n', agg_summary.loc[agg_summary['store_number'] == '105714', ['store_number', 'days_since_last_trx', 'last_trx_date']], sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')
    
    # If transacted within the last 5 days feature
    agg_summary["transacted_last_5_days"] = recency_check(config, agg_summary["inference_col"], agg_summary["days_since_last_trx"], "relax_rules", no_rules_relaxed_recency_threshold, rules_relaxed_recency_threshold)
    
    # Recency weights feature
    agg_summary["weight_till_recency"] = weight_till_recency_func(agg_summary["inference_col"], agg_summary["days_since_last_trx"], "relax_rules", no_rules_relaxed_recency_threshold, rules_relaxed_recency_threshold, recency_weights)

    # Remove duplicate rows from till_data
    agg_summary = agg_summary[~agg_summary.duplicated()]

    # Export summaries
    agg_summary.to_parquet(project_dir + trxn_summary_data_path, index=False)
    
    # Logs
    display(agg_summary.sample(2))
    print('---------------------------------------------------------------------------------------------------------------------------------------')
    
    return agg_summary


# Run code
if __name__ == "__main__":
    # Parameter arguments
    args = argparse.ArgumentParser()
    args.add_argument("--config", default="params.yaml")
    parsed_args = args.parse_args()

    # Run
    df_meta_features = meta_feature_engineering(parsed_args.config, extract_end_date)
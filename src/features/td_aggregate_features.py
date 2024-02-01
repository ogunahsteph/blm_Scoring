# Import modules
import os
import sys


# Import custom modules
sys.path.append(os.path.join(os.getcwd(), ""))

from src.features.td_feature_engineering import *


# Functions
def td_agg_summaries(config_path):
    # Load configurations
    config = read_params(config_path)
    project_dir = config["project_dir"]
    engineered_features_path = config["interim_data_config"]["td_engineered_features_parquet"]
    summary_data_path = config["interim_data_config"]["td_summary_data_parquet"]

    # Load snapshot
    df = pd.read_parquet(project_dir + engineered_features_path)

    # Maximum transaction date feature
    agg_summary = df.groupby(['loan_id_product_concat'], as_index=False)['transaction_date'].max()
    agg_summary = agg_summary.rename(columns={'transaction_date': 'max_transaction_date'})

    # Coerce data type
    agg_summary['max_transaction_date'] = pd.to_datetime(agg_summary['max_transaction_date'], errors='coerce')

    # Export summaries
    agg_summary.to_parquet(project_dir + summary_data_path, index=False)

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
    df_td_features = td_agg_summaries(parsed_args.config)
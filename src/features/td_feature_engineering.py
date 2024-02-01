# Import modules
import os
import sys


# Import custom modules
sys.path.append(os.path.join(os.getcwd(), ""))

from src.data.clean_data import *


# Functions
def td_feature_engineering(config_path):
    # Load configurations
    config = read_params(config_path)
    project_dir = config["project_dir"]
    td_clean_data = config["processed_data_config"]["td_clean_data_parquet"]
    engineered_features_path = config["interim_data_config"]["td_engineered_features_parquet"]

    # Load snapshot
    df = pd.read_parquet(project_dir + td_clean_data)
    
    # Loan id + bloom version feature
    df["loan_id_product_concat"] = (df["mifos_loan_id"].astype("str") + "-" + df["bloom_version"].astype("str")).astype("str")

    # Export summaries
    df.to_parquet(project_dir + engineered_features_path, index=False)

    # Logs
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
    df_td_features = td_feature_engineering(parsed_args.config)
# Import modules
import os
import sys


# Import custom modules
sys.path.append(os.path.join(os.getcwd(), ""))

from src.models.limit_stabilisation_engine import *


# Function
def rejection_reasons_old(rejection_reasons_config, good_loans_repayment_ratio, days_past_due, till_recency, till_consistency, inference_col, idm_recommendation, loan_count, num_days_since_last_disbursement, initial_21_day_limit, initial_7_day_limit, initial_1_day_limit, kyc_completeness_check):
    """
    """
    # Load configurations
    repayment_ratio_response = rejection_reasons_config['repayment_ratio_response']
    dpd_response = rejection_reasons_config['dpd_response']
    till_recency_response = rejection_reasons_config['till_recency_response']
    till_consistency_response = rejection_reasons_config['till_consistency_response']
    idm_recommendation_response = rejection_reasons_config['idm_recommendation_response']
    num_days_since_last_disbursement_response = rejection_reasons_config['num_days_since_last_disbursement_response']
    trivial_limits_cut_off = rejection_reasons_config['trivial_limits_cut_off']
    kyc_check = rejection_reasons_config['kyc_check']
    
    # Conditions
    conditions = [loan_count.gt(0) & good_loans_repayment_ratio.lt(0.6),
                  days_past_due.gt(30) & inference_col.str.match("No_rules_relaxed"),
                  days_past_due.gt(41) & inference_col.str.match("relax_rules"),
                  till_recency.str.match("No"),
                  till_consistency.lt(0.7) & inference_col.str.match("No_rules_relaxed"),
                  till_consistency.lt(0.49) & inference_col.str.match("relax_rules"),
                  loan_count.lt(6) & idm_recommendation.str.match("Reject"),
                  num_days_since_last_disbursement.gt(180),
                  initial_21_day_limit.lt(1000) | initial_7_day_limit.lt(1000) | initial_7_day_limit.lt(200),
                  kyc_completeness_check.str.match("KYC incomplete")]
    
    # Choices
    choices = [repayment_ratio_response,
               dpd_response,
               dpd_response,
               till_recency_response,
               till_consistency_response,
               till_consistency_response,
               idm_recommendation_response,
               num_days_since_last_disbursement_response,
               trivial_limits_cut_off,
               kyc_check]
    
    # Rejection reason feature
    reasons_col = np.select(conditions, choices)
    
    return reasons_col


def rejection_reasons(rejection_reasons_config, good_loans_repayment_ratio, days_past_due, till_recency, till_consistency, inference_col, idm_recommendation, loan_count, num_days_since_last_disbursement, initial_21_day_limit, initial_7_day_limit, initial_1_day_limit, is_iprs_validated, src_crdt_score, max_global_limit, previous_max_global_limit, multiple_limits):
    # Load configurations
    repayment_ratio_response = rejection_reasons_config['repayment_ratio_response']
    dpd_response = rejection_reasons_config['dpd_response']
    till_recency_response = rejection_reasons_config['till_recency_response']
    till_consistency_response = rejection_reasons_config['till_consistency_response']
    idm_recommendation_response = rejection_reasons_config['idm_recommendation_response']
    num_days_since_last_disbursement_response = rejection_reasons_config['num_days_since_last_disbursement_response']
    credit_score_response = rejection_reasons_config['credit_score_response']
    risk_appetite_response = rejection_reasons_config['risk_appetite_response']
    trivial_limits_cut_off = rejection_reasons_config['trivial_limits_cut_off']
    single_limit_per_national_ID_response = rejection_reasons_config['single_limit_per_national_ID_response']
    kyc_national_id_check = rejection_reasons_config['kyc_national_id_check']
    
    # Conditions
    conditions = [loan_count.gt(0) & good_loans_repayment_ratio.lt(0.6),
                  days_past_due.gt(30) & inference_col.str.match("No_rules_relaxed"),
                  days_past_due.gt(41) & inference_col.str.match("relax_rules"),
                  till_recency.str.match("No"),
                  till_consistency.lt(0.7) & inference_col.str.match("No_rules_relaxed"),
                  till_consistency.lt(0.49) & inference_col.str.match("relax_rules"),
                  loan_count.lt(6) & idm_recommendation.str.match("Reject"),
                  num_days_since_last_disbursement.gt(180),
                  src_crdt_score.ge(0) & src_crdt_score.le(477),
                  max_global_limit.lt(previous_max_global_limit) & max_global_limit.gt(0),
                  initial_21_day_limit.lt(1000) | initial_7_day_limit.lt(1000) | initial_7_day_limit.lt(200),
                  multiple_limits.str.match("True"),
                  is_iprs_validated.str.match("False")]
    
    # Choices
    choices = [repayment_ratio_response,
               dpd_response,
               dpd_response,
               till_recency_response,
               till_consistency_response,
               till_consistency_response,
               idm_recommendation_response,
               num_days_since_last_disbursement_response,
               credit_score_response,
               risk_appetite_response,
               trivial_limits_cut_off,
               single_limit_per_national_ID_response,
               kyc_national_id_check]
    
    # Rejection reason feature
    reasons_col = np.select(conditions, choices)
    
    return reasons_col


def include_accounts_to_be_blacklisted(df, blacklist_new_clean_data_parquet, model_index, model_start_date, refresh_date, created_at):
    # Load data frame with merchants to be blacklisted
    blacklist_merchants = pd.read_parquet(blacklist_new_clean_data_parquet)

    # Non scored customers
    blacklist_merchants = blacklist_merchants[~(blacklist_merchants['store_number'].isin(df['store_number']))]
    
    # Zeroize limits and blacklist
    blacklist_merchants['blacklist_flag'] = 1
    blacklist_merchants['final_21_limit'] = 0
    blacklist_merchants['final_7_limit'] = 0
    blacklist_merchants['final_1_limit'] = 0

    # Tag rejection reason
    blacklist_merchants["rules_summary_narration"] = 'risk criteria not met:C4'
    blacklist_merchants[["rules_summary_narration", "limit_reason"]] = blacklist_merchants["rules_summary_narration"].astype("str").str.split(":", expand=True)

    # Add model version and created at fetaures
    blacklist_merchants = add_model_version_and_create_date(blacklist_merchants, model_index, model_start_date, refresh_date, created_at)

    # Adding the non scored accounts into the dataframe
    df = pd.concat([df, blacklist_merchants], axis=0, ignore_index=True)
    
    return df


def include_excluded_first_time_df(df, excluded_first_time_data_path, model_index, model_start_date, refresh_date, created_at):
    # Load data frame with customer details
    excluded_first_time = pd.read_parquet(excluded_first_time_data_path)

    # Non scored customers
    excluded_first_time = excluded_first_time[~(excluded_first_time['store_number'].isin(df['store_number']))]
    
    # Zeroize limits
    excluded_first_time['blacklist_flag'] = 0
    excluded_first_time['final_21_limit'] = 0
    excluded_first_time['final_7_limit'] = 0
    excluded_first_time['final_1_limit'] = 0

    # Tag rejection reason
    excluded_first_time["rules_summary_narration"] = 'risk criteria not met:C4'
    excluded_first_time[["rules_summary_narration", "limit_reason"]] = excluded_first_time["rules_summary_narration"].astype("str").str.split(":", expand=True)

    # Add model version and created at fetaures
    excluded_first_time = add_model_version_and_create_date(excluded_first_time, model_index, model_start_date, refresh_date, created_at)

    # Adding the non scored accounts into the dataframe
    df = pd.concat([df, excluded_first_time], axis=0, ignore_index=True)
    
    return df


def include_non_scored_accounts(df, bcsv_clean_data_parquet, model_index, model_start_date, refresh_date, created_at):
    # Load data frame with customer details
    customer_details = pd.read_parquet(bcsv_clean_data_parquet)

    # Non scored customers
    customer_details = customer_details[~(customer_details['store_number'].isin(df['store_number']))]
    
    # Zeroize limits
    customer_details['blacklist_flag'] = 0
    customer_details['final_21_limit'] = 0
    customer_details['final_7_limit'] = 0
    customer_details['final_1_limit'] = 0

    # Tag rejection reason
    customer_details["rules_summary_narration"] = 'No till activity:B3'
    customer_details[["rules_summary_narration", "limit_reason"]] = customer_details["rules_summary_narration"].astype("str").str.split(":", expand=True)

    # Add model version and created at fetaures
    customer_details = add_model_version_and_create_date(customer_details, model_index, model_start_date, refresh_date, created_at)

    # Adding the non scored accounts into the dataframe
    df = pd.concat([df, customer_details], axis=0, ignore_index=True)
    
    return df


def include_test_accounts(test_accounts_config, df, model_index, model_start_date, refresh_date, created_at):
    # Load configurations
    store_number = test_accounts_config['store_number']
    blacklist_flag = test_accounts_config['blacklist_flag']
    final_21_limit = test_accounts_config['final_21_limit']
    final_7_limit = test_accounts_config['final_7_limit']
    final_1_limit = test_accounts_config['final_1_limit']
    
    # Including the test accounts with new limits assigned
    test_accounts_limits = pd.DataFrame({'store_number': store_number,
                                         'blacklist_flag': blacklist_flag,
                                         'final_21_limit': final_21_limit,
                                         'final_7_limit': final_7_limit,
                                         'final_1_limit': final_1_limit})
    
    # Add model version and created at fetaures
    test_accounts_limits = add_model_version_and_create_date(test_accounts_limits, model_index, model_start_date, refresh_date, created_at)
    
    # Adding the test accounts into the dataframe
    df = pd.concat([df, test_accounts_limits], axis=0, ignore_index=True)

    return df


# def insert_dwh(sql, dwh_credentials, prefix, project_dir):
#     """
#     load data by executing SQL script
#     input: SQL script 
#     output: pandas dataframe 
#     """

#     conn = db_connection(dwh_credentials, prefix, project_dir)
#     df.to_sql(table_name, conn, schema, if_exists='append', index=False, chunksize, dtype, method)

#     return df


def write_results_to_db(dwh_credentials, prefix, project_dir, df, table, insert_query, push_limits):
    # Get connection object
    conn = db_upload_connection(dwh_credentials, prefix, project_dir)
    cursor = conn.cursor()

    # Prepare data set
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))

    # SQL query to execute
    query = insert_query % (table, cols)

    # Upload
    if push_limits == True:
        try:
            extras.execute_values(cursor, query, tuples)
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
            cursor.close()
            return 1
        
        # Exit
        print("The dataframe is inserted")
        cursor.close()
    else:
        # Exit
        print(f"The dataframe is NOT inserted to {table} because push_limits flag = {push_limits}")
        cursor.close()


def trigger_bloom_limit_refresh_push(airflow_credentials, upload_data_config, model_label_config, prefix, project_dir, execution_date, refresh_date, push_limits):
    """
    connect to on-premise dwh
    input: None
    output: connection string
    """
    
    # Load configurations
    airflow_dag_url = upload_data_config['airflow_dag_url']
    headers_content_type = upload_data_config['headers_content_type']
    headers_accept = upload_data_config['headers_accept']
    conf_is_initial_run = upload_data_config['conf_is_initial_run']
    verify = upload_data_config['verify']
    airflow_dag_url = upload_data_config['airflow_dag_url']
    model_index = model_label_config["model_index"]
    model_start_date_y = model_label_config["model_start_date_y"]
    model_start_date_m = model_label_config["model_start_date_m"]
    model_start_date_d = model_label_config["model_start_date_d"]

    # Parameters
    model_start_date = dt.datetime(int(model_start_date_y), int(model_start_date_m), int(model_start_date_d))

    # Decrypt credentials
    airflow_credentials_decrypted = decrypt_credentials(airflow_credentials, prefix, project_dir)
    host = airflow_credentials_decrypted[f'{prefix}_HOST']
    user = airflow_credentials_decrypted[f'{prefix}_USER']
    password = airflow_credentials_decrypted[f'{prefix}_PASSWORD']

    # Logs
    print(f'Uploading limits from model version {label_model(model_index, model_start_date, refresh_date)} to Safaricom table')
    
    # Trigger Airflow DAG
    if push_limits == True:
        response = requests.post(url=f'{host}{airflow_dag_url}',
                                headers={'Content-type': f'{headers_content_type}',
                                         'Accept': f'{headers_accept}'},
                                json={"execution_date": execution_date,
                                      "conf": {'model_version': label_model(model_index, model_start_date, refresh_date),
                                               'is_initial_run': conf_is_initial_run}},
                                auth=requests.auth.HTTPBasicAuth(f"{user}", f'{password}'),
                                verify=verify)
        
        # Print response
        print(response.status_code)
        print(response.text)
        
        return response
    else:
        # Exit
        print(f"Update Safaricom table pipeline is NOT triggered because push_limits flag = {push_limits}")


def push_data(config_path, prefix, p, execution_date, refresh_date, record_added_to_warehouse_on_timestamp, created_at):
    # Load configurations
    config = read_params(config_path)
    project_dir = config["project_dir"]
    scored_limits_risk_review_data_path = config["processed_data_config"]["scored_limits_risk_review_data_parquet"]
    model_label_config = config["model_label_config"]
    model_index = config["model_label_config"]["model_index"]
    model_start_date_y = config["model_label_config"]["model_start_date_y"]
    model_start_date_m = config["model_label_config"]["model_start_date_m"]
    model_start_date_d = config["model_label_config"]["model_start_date_d"]
    upload_data_config = config["upload_data_config"]
    rejection_reasons_config = config["rejection_reasons_config"]
    excluded_first_time_data_path = config["processed_data_config"]["excluded_first_time_data_parquet"]
    bcsv_clean_data_path = config["processed_data_config"]["bcsv_clean_data_parquet"]
    blacklist_new_clean_data_path = config["processed_data_config"]["blacklist_new_clean_data_parquet"]
    test_accounts_config = config["test_accounts_config"]
    cols_fillna = upload_data_config["cols_fillna"]
    to_string_cols = upload_data_config["to_string_cols"]
    to_float_cols = upload_data_config["to_float_cols"]
    p_file_name = f"{p}_validate_push"
    push_limits = config["refresh_config"][f"{p_file_name}_limits"]
    update_model_version = config["refresh_config"][f"update_model_version"]
    dwh_credentials = config["db_credentials"]
    insert_query = upload_data_config["insert_query"]
    scored_limits_approved_data_path = config["processed_data_config"][f"{p}_scored_limits_approved_data_parquet"]

    # Conditional
    if p in ['tss', 'sr']:
        db_features_in_scope = upload_data_config[f"db_features_in_scope_{p}"]
        table = upload_data_config[f"table_{p}"]
        
    # Parameters
    model_start_date = dt.datetime(int(model_start_date_y), int(model_start_date_m), int(model_start_date_d))

    # Load snapshot
    df = pd.read_parquet(project_dir + scored_limits_risk_review_data_path)

    # Tag rejection reason
    # df["rules_summary_narration"] = rejection_reasons(rejection_reasons_config, df["good_loans_repayment_ratio"], df["days_past_due"], df["transacted_last_5_days"], df["page_active_days"], df["inference_col"], df["idm_recommendation"], df["loan_count"], df["num_days_since_last_disbursement"], df["limit_21_day"], df["limit_7_day"], df["limit_1_day"], df["KYC Completeness Check"])
    df["rules_summary_narration"] = rejection_reasons(rejection_reasons_config, df["good_loans_repayment_ratio"], df["days_past_due"], df["transacted_last_5_days"], df["page_active_days"], df["inference_col"], df["idm_recommendation"], df["loan_count"], df["num_days_since_last_disbursement"], df["limit_21_day"], df["limit_7_day"], df["limit_1_day"], df["is_iprs_validated"], df["src_crdt_score"], df['max_global_limit'], df['previous_max_global_limit'], df['multiple_limits'])
    df.loc[(df["rules_summary_narration"] == "0") & (df["blacklist_flag"] == 1), "rules_summary_narration"] = "part of Mifos recon list:E1"
    df.loc[(df["rules_summary_narration"] == "0") & (df["blacklist_flag"] == 0), "rules_summary_narration"] = "all rules passed:F1"
    df.loc[(df["rules_summary_narration"] == "all rules passed:G1") & (df["max_global_limit"] == 0), "rules_summary_narration"] = "risk criteria not met:C4"
    df[["rules_summary_narration", "limit_reason"]] = df["rules_summary_narration"].astype("str").str.split(":", expand=True)

    # Conditional
    if p in ['sr', 'saf']:
        
        # Include accounts to be blacklisted
        df = include_accounts_to_be_blacklisted(df, project_dir + blacklist_new_clean_data_path, model_index, model_start_date, refresh_date, created_at)
        
        # Exempted totally new customers who have not taken any loan but have failed our rules
        df = include_excluded_first_time_df(df, project_dir + excluded_first_time_data_path, model_index, model_start_date, refresh_date, created_at)

        # Zeroize non scored accounts
        df = include_non_scored_accounts(df, project_dir + bcsv_clean_data_path, model_index, model_start_date, refresh_date, created_at)

        # Add test accounts
        df = include_test_accounts(test_accounts_config, df, model_index, model_start_date, refresh_date, created_at)
    # elif p in ['tss']:
    #     # Add model version and created at fetaures
    #     df = add_model_version_and_create_date(df, model_index, model_start_date, refresh_date, created_at)

    # Update model version
    if update_model_version == True:
        df["model_version"] = label_model(model_index, model_start_date, refresh_date)

    # Add record added to warehouse on timestamp column
    df["record_added_to_warehouse_on_timestamp"] = record_added_to_warehouse_on_timestamp

    # Fill missing values
    for col in cols_fillna:
        df[col].fillna(0, inplace=True)
    
    # Fill missing values
    df['days_since_last_trx'].fillna(-4, inplace=True)
    
    # Coerce data type to string
    for col in to_string_cols:
        df[col] = df[col].astype('str')
    
    # Coerce data type to float
    df[to_float_cols] = df[to_float_cols].astype('float')
    
    # # Convert store_number col to string type
    # df['final_21_limit'] = df['final_21_limit'].fillna(0)
    # df['final_7_limit'] = df['final_7_limit'].fillna(0)
    # df['final_1_limit'] = df['final_1_limit'].fillna(0)

    # df["store_number"] = df["store_number"].astype(str)
    # df["most_recent_trx_date_past_30_days"] = df["most_recent_trx_date_past_30_days"].astype(str)
    # df["last_trx_date"] = df["last_trx_date"].astype(str)
    # df["national_id"] = df["national_id"].astype(str)
    # df["disbursed_on_date"] = df["disbursed_on_date"].astype(str)
    # df["expected_matured_on_date"] = df["expected_matured_on_date"].astype(str)
    # df["closed_on_date"] = df["closed_on_date"].astype(str)
    # df["due_date_fixed"] = df["due_date_fixed"].astype(str)
    # df["max_loan_disbursement_date"] = df["max_loan_disbursement_date"].astype(str)

    # df["due_date_fixed"] = df["due_date_fixed"].astype(str)
    # df["due_date_fixed"] = df["due_date_fixed"].astype(str)
    # df["created_at"] = df["created_at"].astype(str)

    # df['days_since_last_trx'] = df['days_since_last_trx'].astype(int)
    # df['weight_dpd'] = df['weight_dpd'].astype(float)

    # df["adjusted_loan_count"] = df["adjusted_loan_count"].astype(float)
    # df["blacklist_flag"] = df["blacklist_flag"].astype(float)
    # df["due_date_fixed"] = df["due_date_fixed"].astype(str)

    # Export snapshot
    df.to_parquet(project_dir + scored_limits_approved_data_path, index=False)

    # Upload limits
    if p in ["tss", "sr"]:
        # Features in scope
        df_in_scope = df[db_features_in_scope]
        
        # Push results to DWH
        write_results_to_db(dwh_credentials, prefix, project_dir, df_in_scope, table, insert_query, push_limits)

        # Validate push
        pull_data(config_path, validate_push().format(p, table), prefix, p_file_name, True, 'interim')
    elif p in ['saf']:
        # Trigger Airflow job to push results to Safaricom table
        trigger_bloom_limit_refresh_push(dwh_credentials, upload_data_config, model_label_config, prefix, project_dir, execution_date, refresh_date, push_limits)
        # df["model_version"] = label_model(model_index, model_start_date, refresh_date)

    # Logs
    if p in ['tss', 'sr']:
        print('\n')
        display(df_in_scope.sample(2))
        print('---------------------------------------------------------------------------------------------------------------------------------------')
        return df_in_scope
    else:
        print('\n')
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
    df_tss = push_data(parsed_args.config, "DWH", "tss", execution_date, refresh_date, record_added_to_warehouse_on_timestamp, created_at)
    df_sr = push_data(parsed_args.config, "DWH", "sr", execution_date, refresh_date, record_added_to_warehouse_on_timestamp, created_at)
    df_saf = push_data(parsed_args.config, "AIRFLOW", "saf", execution_date, refresh_date, record_added_to_warehouse_on_timestamp, created_at)
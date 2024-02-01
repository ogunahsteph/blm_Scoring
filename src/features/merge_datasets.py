# Import modules
import os
import sys


# Import custom modules
sys.path.append(os.path.join(os.getcwd(), ""))

from src.features.meta_feature_engineering import *


# Functions
def merge_customer_details_data(agg_summary, bcsv_clean_data_path, previous_summaries_path):
    # Load data frame with customer details
    customer_details = pd.read_parquet(bcsv_clean_data_path)
    previous_summaries_details = pd.read_parquet(previous_summaries_path)

    # Preparation
    previous_summaries_details['store_number'] = previous_summaries_details['store_number'].astype('str')
    previous_summaries_details = previous_summaries_details[['store_number', 'is_iprs_validated']]
    previous_summaries_details.rename(columns={'is_iprs_validated': 'previous_is_iprs_validated'}, inplace=True)

    # Filter IPRS validated clients
    # customer_details = customer_details[customer_details['is_iprs_validated'] == 'True']
    
    # Merge data frame to add customer details
    customer_details = customer_details.merge(previous_summaries_details, how="left", on="store_number")
    customer_details['previous_is_iprs_validated'] = customer_details['previous_is_iprs_validated'].fillna('False', inplace=True)


    # Merge data frame to add customer details
    agg_summary = agg_summary.merge(customer_details, how="left", on="store_number")

    # Rearrange column order
    agg_summary_cols = agg_summary.columns.to_list()
    agg_summary_cols = agg_summary_cols[-1:] + agg_summary_cols[:-1]
    agg_summary = agg_summary[agg_summary_cols]

    return agg_summary


def merge_last_limits_data(agg_summary, last_limits, rein_cohort):
    # Load data frames
    last_limits_details = pd.read_parquet(last_limits)
    print(last_limits_details[last_limits_details['store_number'] == '968526'])
    
    rein_cohort_details = pd.read_parquet(rein_cohort)
    print(rein_cohort_details[rein_cohort_details['store_number'] == '968526'])

    last_limits_details_in_scope = last_limits_details[last_limits_details['loan_mifos_id'].isin(rein_cohort_details['loan_mifos_id'])]
    print(last_limits_details_in_scope[last_limits_details_in_scope['store_number'] == '968526'])
    
    agg_summary_enhanced = agg_summary.merge(last_limits_details_in_scope, how='left', on="store_number")
    agg_summary_enhanced.drop(columns=["loan_mifos_id"], inplace=True)
    agg_summary_enhanced["rein_7_limit"].fillna(0, inplace=True)
    print(agg_summary_enhanced[agg_summary_enhanced['store_number'] == '968526'][['store_number', 'rein_7_limit']])

    return agg_summary_enhanced


def merge_repayments_details_data(lftsv_engineered_features_path, rmv_clean, agg_summary, td_summary_data_path, waiver_clean_data_path):
    # Load data frame with customer details
    loans = pd.read_parquet(lftsv_engineered_features_path)

    # Filter loan versions in scope
    loans = loans[(loans['bloom_version'] == 2)]

    # Filter loan status in scope
    loans = loans[(loans['loan_status'] == 300) | (loans['loan_status'] == 600) | (loans['loan_status'] == 700)]
    
    # Loan id + bloom version feature
    loans["loan_id_product_concat"] = (loans["loan_mifos_id"].astype("str") + "-" + loans["bloom_version"].astype("str")).astype("str")

    # Drop all duplicated rows
    loans = loans.loc[~loans["loan_id_product_concat"].duplicated()]
    
    # calculating total expected repayment by due date and by end rollover date
    loans['total_expected_repayment_by_due_date'] = loans['principal_disbursed'] + loans['interest_charged']
    loans['total_expected_repayment_by_end_rollover_date'] = loans['principal_disbursed'] + loans['interest_charged'] + loans['fee_charges_charged']
    # loans['total_expected_repayment_by_dpd5'] = loans['principal_disbursed'] + loans['interest_charged'] + loans['fee_charges_charged'] + loans['penalty_charges_charged']

    # Features in scope
    loans = loans[['loan_id_product_concat', 'loan_mifos_id', 'principal_disbursed', 'store_number', "due_date_fixed", "end_rollover_date_fixed", "expected_dpd90", "fee_charges_charged", "penalty_charges_charged", "loan_status", "total_waived", "days_past_due", "interest_charged", "total_expected_repayment_by_due_date", "total_expected_repayment_by_end_rollover_date"]] #"expected_dpd5", "total_expected_repayment_by_dpd5"

    # Merge data sets
    # df = loans.merge(lftsv_agg, how="left", on=['store_number'])
    td_agg = pd.read_parquet(td_summary_data_path)
    td_agg = td_agg[['loan_id_product_concat', 'max_transaction_date']]
    
    waiver_df = pd.read_parquet(waiver_clean_data_path)
    waiver_df.rename(columns={'mifos_loan_id': 'loan_mifos_id'}, inplace=True)
    # waiver_df["waived_amount"].fillna(0, inplace=True)
    
    df = loans.merge(rmv_clean, how="left", on=["loan_mifos_id"])
    df = df.merge(waiver_df, how="left", on=["loan_mifos_id"])
    df = df.merge(td_agg, how="left", on=["loan_id_product_concat"])
    df = df.merge(agg_summary[['store_number', 'any_bloom2_1day']], how="left", on=["store_number"])
    
    df["waived_amount"].fillna(0, inplace=True)

    return df


def rm_hurdle_rates_old(agg_summary, lftsv_engineered_features_path, rmv_clean, td_summary_data_path, waiver_clean_data_path):
    # Merge data sets
    df = merge_repayments_details_data(lftsv_engineered_features_path, rmv_clean, agg_summary, td_summary_data_path, waiver_clean_data_path)
    
    # Repayment rate by due date feature
    df_dd = df[df['due_date_fixed'] <= extract_end_date]

    df_dd["repayments_by_dd_vs_principal"] = df_dd['repayment_amount_by_due_date'] / df_dd['principal_disbursed']
    
    # Bloom 2 1D
    df_dd_bloom2_1day = df_dd[df_dd['any_bloom2_1day'] == True].groupby(['store_number'], as_index=False).agg({'repayments_by_dd_vs_principal': ['min', 'mean'], 'repayment_amount_by_due_date': 'sum', 'principal_disbursed': 'sum'})
    df_dd_bloom2_1day.columns = ['_'.join(col).strip('_') for col in df_dd_bloom2_1day.columns.values]
    df_dd_bloom2_1day['repayments_by_dd_sum_vs_principal_sum'] = df_dd_bloom2_1day['repayment_amount_by_due_date_sum'] / df_dd_bloom2_1day['principal_disbursed_sum']
    df_dd_bloom2_1day["due_date_rm_ge_rm_add_back_old"] = np.where(df_dd_bloom2_1day["repayments_by_dd_vs_principal_mean"] >= 1.0048, 1, 0)
    df_dd_bloom2_1day["due_date_100_check"] = np.where(df_dd_bloom2_1day["repayments_by_dd_vs_principal_mean"] >= 1.0048, 1, 0)

    # Bloom 2 Mixed portfolio
    df_dd_mixed_portfolio = df_dd[df_dd['any_bloom2_1day'] == False].groupby(['store_number'], as_index=False).agg({'repayments_by_dd_vs_principal': ['min', 'mean'], 'repayment_amount_by_due_date': 'sum', 'principal_disbursed': 'sum'})
    df_dd_mixed_portfolio.columns = ['_'.join(col).strip('_') for col in df_dd_mixed_portfolio.columns.values]
    df_dd_mixed_portfolio['repayments_by_dd_sum_vs_principal_sum'] = df_dd_mixed_portfolio['repayment_amount_by_due_date_sum'] / df_dd_mixed_portfolio['principal_disbursed_sum']
    df_dd_mixed_portfolio["due_date_rm_ge_rm_add_back_old"] = np.where(df_dd_mixed_portfolio["repayments_by_dd_vs_principal_mean"] >= 1.02, 1, 0)
    df_dd_mixed_portfolio["due_date_100_check"] = np.where(df_dd_mixed_portfolio["repayments_by_dd_vs_principal_mean"] >= 1.00, 1, 0)

    # Merge
    df_dd = pd.concat([df_dd_bloom2_1day, df_dd_mixed_portfolio], axis=0, ignore_index=True)
    df_dd["due_date_rm_ge_rm_1d_old"] = np.where(df_dd["repayments_by_dd_vs_principal_mean"] >= 1.00, 1, 0)
    


    # Repayment rate by end rollover date feature
    df_red = df[(df['end_rollover_date_fixed'] <= extract_end_date) & (df['days_past_due'] > 0)]
    df_red["repayments_by_erd_vs_principal"] = df_red['repayment_amount_by_rllvr_date'] / df_red['principal_disbursed']
    df_red["repayments_by_erd_vs_total_expected_repayment_by_erd"] = df_red['repayment_amount_by_rllvr_date'] / df_red['total_expected_repayment_by_end_rollover_date']
    # df_red = df_red.groupby(['store_number'], as_index=False).agg({'repayments_by_erd_vs_total_expected_repayment_by_erd': 'mean'})
    
    # Bloom 2 1D
    df_red_bloom2_1day = df_red[df_red['any_bloom2_1day'] == True].groupby(['store_number'], as_index=False).agg({'repayments_by_erd_vs_principal': ['min', 'mean'], 'repayments_by_erd_vs_total_expected_repayment_by_erd': 'mean', 'repayment_amount_by_rllvr_date': 'sum', 'principal_disbursed': 'sum'})
    df_red_bloom2_1day.columns = ['_'.join(col).strip('_') for col in df_red_bloom2_1day.columns.values]
    df_red_bloom2_1day['repayments_by_erd_sum_vs_principal_sum'] = df_red_bloom2_1day['repayment_amount_by_rllvr_date_sum'] / df_red_bloom2_1day['principal_disbursed_sum']
    df_red_bloom2_1day["rllvr_date_rm_ge_rm_add_back_old"] = np.where(df_red_bloom2_1day["repayments_by_erd_vs_principal_mean"] >= 1.0096, 1, 0)
    df_red_bloom2_1day["rllvr_date_102_check"] = np.where(df_red_bloom2_1day["repayments_by_erd_vs_principal_mean"] >= 1.0096, 1, 0)


    # Bloom 2 Mixed portfolio
    df_red_mixed_portfolio = df_red[df_red['any_bloom2_1day'] == False].groupby(['store_number'], as_index=False).agg({'repayments_by_erd_vs_principal': ['min', 'mean'], 'repayments_by_erd_vs_total_expected_repayment_by_erd': 'mean', 'repayment_amount_by_rllvr_date': 'sum', 'principal_disbursed': 'sum'})
    df_red_mixed_portfolio.columns = ['_'.join(col).strip('_') for col in df_red_mixed_portfolio.columns.values]
    df_red_mixed_portfolio['repayments_by_erd_sum_vs_principal_sum'] = df_red_mixed_portfolio['repayment_amount_by_rllvr_date_sum'] / df_red_mixed_portfolio['principal_disbursed_sum']
    df_red_mixed_portfolio["rllvr_date_rm_ge_rm_add_back_old"] = np.where(df_red_mixed_portfolio["repayments_by_erd_vs_principal_mean"] >= 1.04, 1, 0) 
    df_red_mixed_portfolio["rllvr_date_102_check"] = np.where(df_red_mixed_portfolio["repayments_by_erd_vs_principal_mean"] >= 1.02, 1, 0)
    # df_red_mixed_portfolio["due_date_rm_ge_rm_1d"] = np.where(df_red_mixed_portfolio["repayments_by_erd_vs_total_expected_repayment_by_erd_mean"] >= 1.00, 1, 0)
    
    # Merge
    df_red = pd.concat([df_red_bloom2_1day, df_red_mixed_portfolio], axis=0, ignore_index=True)
    # df_red["rllvr_date_rm_ge_rm_limit_increase"] = np.where(df_red["repayments_by_erd_vs_principal_mean"] >= 1.02, 1, 0)
    # df_red["due_date_rm_ge_rm_1d"] = np.where(df_red["repayments_by_erd_vs_total_expected_repayment_by_erd_mean"] >= 1.00, 1, 0)



    # Repayment rate by dpd90 date feature
    df_dpd90 = df[df['expected_dpd90'] <= extract_end_date]
    df_dpd90["repayments_by_dpd90_vs_principal"] = df_dpd90['repayment_amount_by_dpd90'] / df_dpd90['principal_disbursed']
    df_dpd90 = df_dpd90.groupby(['store_number'], as_index=False).agg({'repayments_by_dpd90_vs_principal': ['min', 'mean'], 'repayment_amount_by_dpd90': 'sum', 'principal_disbursed': 'sum'})
    df_dpd90.columns = ['_'.join(col).strip('_') for col in df_dpd90.columns.values]
    df_dpd90['repayments_by_dpd90_sum_vs_principal_sum'] = df_dpd90['repayment_amount_by_dpd90_sum'] / df_dpd90['principal_disbursed_sum']
    # df_dpd90["dpd90_rm_ge104p"] = np.where(df_dpd90["repayments_by_dpd90_vs_principal_min"] >= 1.04, 1, 0)
    # df_dpd90["dpd90_rm_ge104p"] = np.where(df_dpd90["repayments_by_dpd90_sum_vs_principal_sum"] >= 1.04, 1, 0)
    # df_dpd90["dpd90_rm_ge104p"] = np.where(df_dpd90["repayments_by_dpd90_vs_principal_mean"] >= 1.04, 1, 0)

    
    # Merge data sets
    final = df_dd[['store_number', 'repayments_by_dd_vs_principal_mean', 'due_date_rm_ge_rm_add_back_old', 'due_date_rm_ge_rm_1d_old', 'due_date_100_check']].merge(df_red[['store_number', 'repayments_by_erd_vs_principal_mean', 'rllvr_date_rm_ge_rm_add_back_old', 'rllvr_date_102_check']], on=['store_number'], how='left')
    # final = final.merge(df_dpd90[['store_number', 'dpd90_rm_ge104p']], on=['store_number'], how='left')
    final['due_date_rm_ge_rm_add_back_old'] = final['due_date_rm_ge_rm_add_back_old'].fillna(1)
    final['due_date_rm_ge_rm_1d_old'] = final['due_date_rm_ge_rm_add_back_old'].fillna(1)
    final['rllvr_date_rm_ge_rm_add_back_old'] = final['rllvr_date_rm_ge_rm_add_back_old'].fillna(1)

    # Merge data sets
    df = agg_summary.merge(final, how="left", on=["store_number"])

    # Flag never borrowed clients
    df['never_borrowed_flag_old'] = np.where(df['due_date_rm_ge_rm_add_back_old'].isna(), 1, 0)

    return df



def rm_hurdle_rates_new(agg_summary, lftsv_engineered_features_path, rmv_clean, td_summary_data_path, waiver_clean_data_path):
    # Merge data sets
    df = merge_repayments_details_data(lftsv_engineered_features_path, rmv_clean, agg_summary, td_summary_data_path, waiver_clean_data_path)
    
    # df['total_waived_amount'] = df['waived_amount'] + df['total_waived']
    # df['total_expected_repayment_by_end_rollover_date'] = df['total_expected_repayment_by_end_rollover_date'] - df['total_waived_amount']
    
    # Repayment rate by due date feature
    df_dd = df[df['due_date_fixed'] <= extract_end_date]
    
    
    df_dd['hurdle_rate_by_due_date'] = df_dd['repayment_amount_by_due_date'] / df_dd['principal_disbursed']
    df_dd['hurdle_rate_by_end_rollover_date'] = df_dd['repayment_amount_by_rllvr_date'] / df_dd['principal_disbursed']
    df_dd['hurdle_rate_by_dpd30'] = df_dd['repayment_amount_by_dpd30'] / df_dd['principal_disbursed']


    df_dd["repayments_by_dd_vs_principal"] = df_dd['repayment_amount_by_due_date'] / df_dd['principal_disbursed']
    df_dd["repayments_by_dd_vs_total_expected_repayment_by_dd"] = df_dd['repayment_amount_by_due_date'] / df_dd['total_expected_repayment_by_due_date']
    df_dd = df_dd.groupby(['store_number'], as_index=False).agg({'repayments_by_dd_vs_total_expected_repayment_by_dd': ['min', 'mean'], 'repayments_by_dd_vs_principal': 'mean', 'hurdle_rate_by_due_date': 'mean', 'hurdle_rate_by_end_rollover_date': 'mean', 'hurdle_rate_by_dpd30': 'mean'})
    df_dd.columns = ['_'.join(col).strip('_') for col in df_dd.columns.values]
    df_dd["due_date_rm_ge_rm_add_back_new"] = np.where(df_dd["repayments_by_dd_vs_total_expected_repayment_by_dd_mean"] >= 0.98, 1, 0)
    # df_dd["rllvr_date_rm_ge_rm_1d_new"] = np.where(df_dd["repayments_by_dd_vs_total_expected_repayment_by_dd_mean"] >= 1.00, 1, 0)
 
    # Repayment rate by end rollover date feature
    df_red = df[(df['end_rollover_date_fixed'] <= extract_end_date)]
    df_red["repayments_by_erd_vs_principal"] = df_red['repayment_amount_by_rllvr_date'] / df_red['principal_disbursed']
    df_red["repayments_by_erd_vs_total_expected_repayment_by_erd"] = df_red['repayment_amount_by_rllvr_date'] / df_red['total_expected_repayment_by_end_rollover_date']
    df_red = df_red.groupby(['store_number'], as_index=False).agg({'repayments_by_erd_vs_total_expected_repayment_by_erd': ['min', 'mean'], 'repayments_by_erd_vs_principal': 'mean'})
    df_red.columns = ['_'.join(col).strip('_') for col in df_red.columns.values]
    df_red["rllvr_date_rm_ge_rm_add_back_new"] = np.where(df_red["repayments_by_erd_vs_total_expected_repayment_by_erd_mean"] >= 0.98, 1, 0)
    
    df_red["rllvr_date_rm_ge_rm_1d_new"] = np.where(df_red["repayments_by_erd_vs_total_expected_repayment_by_erd_mean"] >= 1.00, 1, 0)
    
    # Repayment rate by dpd5 date feature
    # df_dpd5 = df[(df['expected_dpd5'] <= extract_end_date)]
    # df_dpd5["repayments_by_dpd5_vs_principal"] = df_dpd5['repayment_amount_by_dpd5'] / df_dpd5['principal_disbursed']
    # df_dpd5["repayments_by_dpd5_vs_total_expected_repayment_by_dpd5"] = df_dpd5['repayment_amount_by_dpd5'] / df_dpd5['total_expected_repayment_by_end_rollover_date']
    # df_dpd5 = df_dpd5.groupby(['store_number'], as_index=False).agg({'repayments_by_dpd5_vs_total_expected_repayment_by_dpd5': ['min', 'mean'], 'repayments_by_dpd5_vs_principal': 'mean'})
    # df_dpd5.columns = ['_'.join(col).strip('_') for col in df_dpd5.columns.values]
    # df_red["rllvr_date_rm_ge_rm_add_back_new"] = np.where(df_red["repayments_by_erd_vs_total_expected_repayment_by_erd_mean"] >= 1.00, 0, 0)
    
    # df_dpd5["rllvr_date_rm_ge_rm_1d_new"] = np.where(df_dpd5["repayments_by_dpd5_vs_total_expected_repayment_by_dpd5_mean"] >= 1.00, 1, 0)
    
    
    # Merge data sets
    final = df_dd[['store_number', 'repayments_by_dd_vs_total_expected_repayment_by_dd_mean', 'due_date_rm_ge_rm_add_back_new', 'hurdle_rate_by_due_date_mean', 'hurdle_rate_by_end_rollover_date_mean', 'hurdle_rate_by_dpd30_mean']].merge(df_red[['store_number', 'repayments_by_erd_vs_total_expected_repayment_by_erd_mean', 'rllvr_date_rm_ge_rm_add_back_new', 'rllvr_date_rm_ge_rm_1d_new']], on=['store_number'], how='left')
    # final = final.merge(df_dpd5[['store_number', 'rllvr_date_rm_ge_rm_1d_new']], on=['store_number'], how='left')
    final['due_date_rm_ge_rm_add_back_new'] = final['due_date_rm_ge_rm_add_back_new'].fillna(1)
    final['rllvr_date_rm_ge_rm_add_back_new'] = final['rllvr_date_rm_ge_rm_add_back_new'].fillna(1)
    final['rllvr_date_rm_ge_rm_1d_new'] = final['rllvr_date_rm_ge_rm_1d_new'].fillna(1)

    # Merge data sets
    df = agg_summary.merge(final, how="left", on=["store_number"])

    # Flag never borrowed clients
    df['never_borrowed_flag_new'] = np.where(df['due_date_rm_ge_rm_add_back_new'].isna(), 1, 0)

    return df


def update_due_date_rm_ge_rm_add_back(due_date_rm_ge_rm_add_back_new, due_date_rm_ge_rm_add_back_old):
    return np.where(due_date_rm_ge_rm_add_back_new == 1, 1, due_date_rm_ge_rm_add_back_old)


def update_rllvr_date_rm_ge_rm_add_back(rllvr_date_rm_ge_rm_add_back_new, rllvr_date_rm_ge_rm_add_back_old):
    return np.where(rllvr_date_rm_ge_rm_add_back_new == 1, 1, rllvr_date_rm_ge_rm_add_back_old)


def update_due_date_100_check(due_date_rm_ge_rm_add_back_new, due_date_100_check):
    return np.where(due_date_rm_ge_rm_add_back_new == 1, 1, due_date_100_check)


def update_rllvr_date_102_check(rllvr_date_rm_ge_rm_add_back_new, rllvr_date_102_check):
    return np.where(rllvr_date_rm_ge_rm_add_back_new == 1, 1, rllvr_date_102_check)

# def update_due_date_rm_ge_rm_1d(rllvr_date_rm_ge_rm_1d_new, due_date_rm_ge_rm_1d_old):
#     return np.where(rllvr_date_rm_ge_rm_1d_new == 1, 1, due_date_rm_ge_rm_1d_old)

def update_due_date_rm_ge_rm_1d(rllvr_date_rm_ge_rm_1d_new, due_date_rm_ge_rm_1d_old):
    return np.where(rllvr_date_rm_ge_rm_1d_new == 1, 1, 0)


def never_borrowed_flag(never_borrowed_flag_new, never_borrowed_flag_old):
    return np.where(never_borrowed_flag_new == 1, 1, never_borrowed_flag_old)
                    
                    
def update_lrr_good_loans_repayment_ratio(to_lrr_update_flag, good_loans_repayment_ratio):
    return np.where(to_lrr_update_flag == 1, 1, good_loans_repayment_ratio)


def update_lrr_days_past_due(to_lrr_update_flag, days_past_due):
    return np.where(to_lrr_update_flag == 1, 0, days_past_due)


def update_lrr_num_days_since_last_disbursement(to_lrr_update_flag, num_days_since_last_disbursement):
    return np.where(to_lrr_update_flag == 1, 0, num_days_since_last_disbursement)


def update_lrr_dd_hurdle_rate_new(to_lrr_update_flag, repayments_by_dd_vs_total_expected_repayment_by_dd_mean):
    return np.where(to_lrr_update_flag == 1, 1.00, repayments_by_dd_vs_total_expected_repayment_by_dd_mean)


def update_lrr_erd_hurdle_rate_new(to_lrr_update_flag, repayments_by_erd_vs_total_expected_repayment_by_erd_mean):
    return np.where(to_lrr_update_flag == 1, 1.00, repayments_by_erd_vs_total_expected_repayment_by_erd_mean)


def update_lrr_dd_hurdle_rate(to_lrr_update_flag, repayments_by_dd_vs_principal_mean):
    return np.where(to_lrr_update_flag == 1, 1.02, repayments_by_dd_vs_principal_mean)


def update_lrr_erd_hurdle_rate(to_lrr_update_flag, repayments_by_erd_vs_principal_mean):
    return np.where(to_lrr_update_flag == 1, 1.04, repayments_by_erd_vs_principal_mean)


def update_lrr_dd_add_back_flag(to_lrr_update_flag, due_date_rm_ge_rm_add_back):
    return np.where(to_lrr_update_flag == 1, 1, due_date_rm_ge_rm_add_back)


def update_lrr_erd_add_back_flag(to_lrr_update_flag, rllvr_date_rm_ge_rm_add_back):
    return np.where(to_lrr_update_flag == 1, 1, rllvr_date_rm_ge_rm_add_back)


def update_lrr_due_date_100_check_flag(to_lrr_update_flag, due_date_100_check):
    return np.where(to_lrr_update_flag == 1, 1, due_date_100_check)


def update_lrr_rllvr_date_102_check_flag(to_lrr_update_flag, rllvr_date_102_check):
    return np.where(to_lrr_update_flag == 1, 1, rllvr_date_102_check)


def update_lrr_1d_add_back_flag(to_lrr_update_flag, due_date_rm_ge_rm_1d):
    return np.where(to_lrr_update_flag == 1, due_date_rm_ge_rm_1d, due_date_rm_ge_rm_1d)


def update_idm_recommendation(rllvr_date_102_check):
    return np.where(rllvr_date_102_check == 1, 'Approve', 'Reject')


def limit_review_request_exeptions(df, lrr_clean_data_parquet):
    # Load data frame with customer details
    lrr_details = pd.read_parquet(lrr_clean_data_parquet)

    # Merge datasets
    df = df.merge(lrr_details, how="left", on=["store_number"])
    df['lrr_update_flag'].fillna(0, inplace=True)
    
    # Update good loans repayment ratio
    df['good_loans_repayment_ratio'] = update_lrr_good_loans_repayment_ratio(df['lrr_update_flag'], df['good_loans_repayment_ratio'])
    
    # Update days past due
    df['days_past_due'] = update_lrr_days_past_due(df['lrr_update_flag'], df['days_past_due'])
    
    # Update num days since last disbursement
    df['num_days_since_last_disbursement'] = update_lrr_days_past_due(df['lrr_update_flag'], df['num_days_since_last_disbursement'])
    
    # Update due date hurdle rate
    df['repayments_by_dd_vs_total_expected_repayment_by_dd_mean'] = update_lrr_dd_hurdle_rate_new(df['lrr_update_flag'], df['repayments_by_dd_vs_total_expected_repayment_by_dd_mean'])
    
    # Update due date add back flag 
    df['repayments_by_erd_vs_total_expected_repayment_by_erd_mean'] = update_lrr_erd_hurdle_rate_new(df['lrr_update_flag'], df['repayments_by_erd_vs_total_expected_repayment_by_erd_mean'])
    
    # Update due date hurdle rate
    df['repayments_by_dd_vs_principal_mean'] = update_lrr_dd_hurdle_rate(df['lrr_update_flag'], df['repayments_by_dd_vs_principal_mean'])
    
    # Update due date add back flag 
    df['repayments_by_erd_vs_principal_mean'] = update_lrr_erd_hurdle_rate(df['lrr_update_flag'], df['repayments_by_erd_vs_principal_mean'])
    
    # Update end rollover date add back flag
    df['due_date_rm_ge_rm_add_back'] = update_lrr_dd_add_back_flag(df['lrr_update_flag'], df['due_date_rm_ge_rm_add_back'])
    
    # Update end rollover date add back flag
    df['rllvr_date_rm_ge_rm_add_back'] = update_lrr_erd_add_back_flag(df['lrr_update_flag'], df['rllvr_date_rm_ge_rm_add_back'])
    
    # Update end rollover date add back flag
    df['due_date_100_check'] = update_lrr_due_date_100_check_flag(df['lrr_update_flag'], df['due_date_100_check'])
    
    # Update end rollover date add back flag
    df['rllvr_date_102_check'] = update_lrr_rllvr_date_102_check_flag(df['lrr_update_flag'], df['rllvr_date_102_check'])
    
    # Update due date 1 day add back flag
    df['due_date_rm_ge_rm_1d'] = update_lrr_1d_add_back_flag(df['lrr_update_flag'], df['due_date_rm_ge_rm_1d'])
    
    return df


def merge_datasets(config_path):
    # Load configurations
    config = read_params(config_path)
    project_dir = config["project_dir"]
    trxn_summary_data_path = config["interim_data_config"]["trxn_summary_data_parquet"]
    idm_clean_data_path = config["processed_data_config"]["idm_clean_data_parquet"]
    bcsv_clean_data_path = config["processed_data_config"]["bcsv_clean_data_parquet"]
    waiver_clean_data_path = config["processed_data_config"]["waiver_clean_data_parquet"]
    loans_summary_data_path = config["interim_data_config"]["loans_summary_data_parquet"]
    sr_21_limits_clean_data_path = config["processed_data_config"]["sr_21_limits_clean_data_parquet"]
    sr_latest_refresh_limits_clean_data_path = config["processed_data_config"]["sr_latest_refresh_limits_clean_data_parquet"]
    lftsv_clean_data_path = config["processed_data_config"]["lftsv_clean_data_parquet"]
    lftsv_engineered_features_path = config["interim_data_config"]["engineered_features_parquet"]
    rmv_clean_data_path = config["processed_data_config"]["rmv_clean_data_parquet"]
    lrr_clean_data_path = config["processed_data_config"]["lrr_clean_data_parquet"]
    previous_summaries_path = config["interim_data_config"]["previous_summaries_parquet"]
    sr_rein_last_limits_clean_data_path = config["processed_data_config"]["sr_rein_last_limits_clean_data_parquet"]
    sr_rein_cohort_data_path = config["interim_data_config"]["sr_rein_cohort_data_parquet"]
    cols_fill_na = config["merged_data_config"]["cols_fill_na"]
    cols_fill_idm = config["merged_data_config"]["cols_fill_idm"]
    cols_fill_iprs = config["merged_data_config"]["cols_fill_na"]
    cols_fill_dpd = config["merged_data_config"]["cols_fill_dpd"]
    cols_fill_map = config["merged_data_config"]["cols_fill_map"]
    bloom_customers_no_till_data_path = config["processed_data_config"]["bloom_customers_no_till_data_parquet"]
    merged_data_path = config["processed_data_config"]["merged_data_parquet"]
    td_summary_data_path = config["interim_data_config"]["td_summary_data_parquet"]
    kyc_flag_updates_path = config["interim_data_config"]["kyc_flag_updates_excel"]
    
    # Load snapshot
    df_meta_trxn = pd.read_parquet(project_dir + trxn_summary_data_path)
    df_idm_clean = pd.read_parquet(project_dir + idm_clean_data_path)
    df_lftsv_clean = pd.read_parquet(project_dir + loans_summary_data_path)
    df_sr_21_limits_clean = pd.read_parquet(project_dir + sr_21_limits_clean_data_path)
    df_sr_latest_refresh_clean = pd.read_parquet(project_dir + sr_latest_refresh_limits_clean_data_path)
    df_rmv_clean = pd.read_parquet(project_dir + rmv_clean_data_path)
    df_lrr_clean = pd.read_parquet(project_dir + lrr_clean_data_path)
    df_previous_summaries = pd.read_parquet(project_dir + previous_summaries_path)
    td_agg = pd.read_parquet(project_dir + td_summary_data_path)
    
    # Metabase left merge BCSV
    df = merge_customer_details_data(df_meta_trxn, project_dir + bcsv_clean_data_path, project_dir + previous_summaries_path) # TODO
    
    # Left merge IDM
    df = df.merge(df_idm_clean, how="left", on=["national_id"])

    # Customers we have loans history but no till data summaries
    bloom_customers_no_till = pd.DataFrame(list(set(df_lftsv_clean["store_number"]) - set(df["store_number"])), columns=['national_id'])

    # Customers we have loans history but no reported store_number
    bloom_customers_no_store_num = df_lftsv_clean.loc[df_lftsv_clean["store_number"].isnull(), ["client_mobile_number", "store_number", "bloom_version", "loan_count", "loan_repayment_status"]]
    
    # Left merge LFTSV
    df = df.merge(df_lftsv_clean, how="left", on=["store_number", "inference_col"])

    # Left merge Bloom-21 limits
    df = df.merge(df_sr_21_limits_clean, how="left", on=["store_number"])

    # Left merge previous limit allocation
    df = df.merge(df_sr_latest_refresh_clean, how="left", on=["store_number"])

    # Left merge RMV
    df = rm_hurdle_rates_old(df, project_dir + lftsv_engineered_features_path, df_rmv_clean, project_dir + td_summary_data_path, project_dir + waiver_clean_data_path)
                  
    df = rm_hurdle_rates_new(df, project_dir + lftsv_engineered_features_path, df_rmv_clean, project_dir + td_summary_data_path, project_dir + waiver_clean_data_path)
                    
    df['due_date_rm_ge_rm_add_back'] = update_due_date_rm_ge_rm_add_back(df['due_date_rm_ge_rm_add_back_new'], df['due_date_rm_ge_rm_add_back_old'])
    
    df['rllvr_date_rm_ge_rm_add_back'] = update_rllvr_date_rm_ge_rm_add_back(df['rllvr_date_rm_ge_rm_add_back_new'], df['rllvr_date_rm_ge_rm_add_back_old'])
    
    df['due_date_100_check'] = update_rllvr_date_102_check(df['due_date_rm_ge_rm_add_back_new'], df['due_date_100_check'])
    
    df['rllvr_date_102_check'] = update_rllvr_date_102_check(df['rllvr_date_rm_ge_rm_add_back_new'], df['rllvr_date_102_check'])
    
    df['due_date_rm_ge_rm_1d'] = update_due_date_rm_ge_rm_1d(df['rllvr_date_rm_ge_rm_1d_new'], df['due_date_rm_ge_rm_1d_old'])
                    
    df['never_borrowed_flag'] = never_borrowed_flag(df['never_borrowed_flag_new'], df['never_borrowed_flag_old'])
    
    # Left merge lrr
    df = limit_review_request_exeptions(df, project_dir + lrr_clean_data_path)

    # Left merge bloom 2.0 reinstatement
    df = merge_last_limits_data(df, project_dir + sr_rein_last_limits_clean_data_path, project_dir + sr_rein_cohort_data_path)
    df[df['store_number'] == '968526'][['store_number', 'rein_7_limit']]
    
    # Delete duplicate errors created because of broken ranking feature
    df = df.sort_values(by=["disbursed_on_date"], ascending=[True]).drop_duplicates(subset="store_number", keep='last')

    # Remove duplicate rows from df
    df = df[~df.duplicated()]
    
    # Replace 'NaN' with zero and coerce data type
    for col in cols_fill_na:
        df[col].fillna(0, inplace=True)
        df[col] = df[col].astype('int')
        
    # Replace 'NaN' with zero and coerce data type
    for col in cols_fill_dpd:
        df[col].fillna(0, inplace=True)
        df[col] = df[col].astype('float')
    
    # Impute missing IDM recommendation and coerce data type
    for col in cols_fill_idm:
        df[col].fillna("Reject", inplace=True)
        df[col] = df[col].astype('str')
    
    # Impute missing IPRS flag and coerce data type
    for col in cols_fill_iprs:
        df[col].fillna('False', inplace=True)
        df[col] = df[col].astype('str')
    df['is_iprs_validated'].fillna('False', inplace=True)
    
    # Impute missing location mapped flag and coerce data type
    for col in cols_fill_map:
        df[col].fillna('False', inplace=True)
        df[col] = df[col].astype('str')
    df['is_location_mapped'].fillna('False', inplace=True)
    
    # Coerce data types
    df["loan_count"] = df["loan_count"].astype('int')
    df["total_final_21_limit"] = df["total_final_21_limit"].astype('int')
    df["previous_21_limit"] = df["previous_21_limit"].astype('int')
    df["previous_7_limit"] = df["previous_7_limit"].astype('int')
    df["previous_1_limit"] = df["previous_1_limit"].astype('int')
    df["due_date_rm_ge_rm_1d"] = df["due_date_rm_ge_rm_1d"].astype('int')
    df["due_date_rm_ge_rm_add_back"] = df["due_date_rm_ge_rm_add_back"].astype('int')
    df["rllvr_date_rm_ge_rm_add_back"] = df["rllvr_date_rm_ge_rm_add_back"].astype('int')
    df["due_date_100_check"] = df["due_date_100_check"].astype('int')
    df["rllvr_date_102_check"] = df["rllvr_date_102_check"].astype('int')
#     df["rllvr_date_rm_ge_rm_limit_increase"] = df["rllvr_date_rm_ge_rm_limit_increase"].astype('int')
    # df["dpd90_rm_ge104p"] = df["dpd90_rm_ge104p"].astype('int')
    # df["rllvr_date_rm_ge_rm_never_borrowed"] = df["rllvr_date_rm_ge_rm_never_borrowed"].astype('int')
    del df["idm_recommendation"]
    # df["idm_recommendation"] = 'Approve'
    df['idm_recommendation'] = update_idm_recommendation(df['rllvr_date_102_check'])
    df["idm_recommendation"] = df["idm_recommendation"].astype('str')
    df["is_iprs_validated"] = df["is_iprs_validated"].astype('str')
    df["is_location_mapped"] = df["is_location_mapped"].astype('str')
    
    # Save snapshot
    bloom_customers_no_till.to_parquet(project_dir + bloom_customers_no_till_data_path, index=False)
    df.to_parquet(project_dir + merged_data_path, index=False)
    
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
    df_merge = merge_datasets(parsed_args.config)
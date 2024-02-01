# Import modules
import os
import sys


# Import custom modules
sys.path.append(os.path.join(os.getcwd(), ""))

from src.models.limit_scoring_engine import *


# Functions
def limit_zeroization_defaulters(df, blacklist_clean_data_path, term_frequencies):
    # Load snapshot
    df_blacklist_clean = pd.read_parquet(blacklist_clean_data_path)

    # Get list of defaulters
    mfs_defaulters_list = list(df_blacklist_clean["store_number"].unique())

    # Zeroize limits of defaulters
    for tf in term_frequencies:
        df.loc[(df["store_number"].isin(mfs_defaulters_list)) & (df[f"final_{tf}_limit"] > 0), f"final_{tf}_limit"] = 0

    return df


# def updating_blacklist_flag(df):
#     # Group by 'national id' and iterate through each group
#     for _, group in df.groupby('national_id'):
#         # Check if any store number in the group has blacklist flag as 1
#         if (group['blacklist_flag'] == 1).any():
#             # Set blacklist flag to 1 for all store numbers in the group
#             df.loc[group.index, 'blacklist_flag'] = 1
            
#     return df


def updating_blacklist_flag(df):
    # Create a mask to identify groups where at least one row has 'blacklist_flag' equal to 1
    mask = df.groupby('national_id')['blacklist_flag'].transform('max') == 1
    
    # Update 'blacklist_flag' for the groups identified by the mask
    df.loc[mask, 'blacklist_flag'] = 1
    
    return df


def zeroize_blacklisted_merchants(final_limit, blacklist_flag):
    return np.where(blacklist_flag == 1, 0, final_limit)


def calc_blacklist_flag_old(days_past_due, bloom_version, loan_status):    
    return np.where(((bloom_version == 1) & (loan_status == 300)) | (days_past_due >= 60), 1, 0)


def calc_blacklist_flag(days_past_due, bloom_version, loan_status):    
    return np.where((days_past_due >= 90), 1, 0)


def declare_limit_factor(config, idm_recommendation, tf):
    """
    function to declare limit factors used to allocate limits based on defined conditions i.e IDM recommendation
    
    Inputs:
    1) IDM recommendation column i.e IDM Approve VS IDM Reject
    
    Output:
    assigned limit factors used to allocate limits based on defined conditions i.e IDM recommendation
    """
    # Load configurations
    limit_approve = config["limit_factor_config"][f"limit_{tf}_approve"]
    limit_reject = config["limit_factor_config"][f"limit_{tf}_reject"]
   
    # Conditions
    conditions = [idm_recommendation.str.match("Approve"),
                  idm_recommendation.str.match("Reject")]
    
    # Choices
    choices = [limit_approve,
               limit_reject]
    
    # Limit factor feature
    limit_factor = np.select(conditions, choices)
    
    return limit_factor


def calc_final_21_limit(total_final_21_limit, final_21_limit):
    f21l = np.where(total_final_21_limit == 0, 0,
           np.where(total_final_21_limit > 0, final_21_limit,
           np.nan))
    
    return f21l


def no_new_limits_assignement(total_final_limit, final_limit, rllvr_date_102_check):
    f21l = np.where((total_final_limit == 0) & (rllvr_date_102_check == 0), 0,
                     final_limit)
    
    return f21l


def calc_final_limit_from_limit_cap_old1(limit_cap, final_limit, previous_limit):
    fl = np.where(previous_limit == 0, final_limit,
         np.where(final_limit <= previous_limit, final_limit,
         np.where((final_limit > previous_limit) & (final_limit < limit_cap), final_limit,
         np.where((final_limit > previous_limit) & (final_limit >= limit_cap), limit_cap,
                  final_limit))))
    
    return fl


def calc_final_limit_from_limit_cap_old2(final_limit, previous_limit):
    fl = np.where(final_limit == 0, final_limit,
         np.where(previous_limit == 0, final_limit,
         np.where((final_limit >= (previous_limit * 0.85)) | (final_limit <= (previous_limit * 1.15)), previous_limit,
         np.where(final_limit >= (previous_limit * 1.5), previous_limit * 1.5,
         final_limit))))
    
    return fl


def calc_final_limit_from_limit_cap_old3(final_limit, previous_limit):
    fl = np.where(final_limit > previous_limit, previous_limit, final_limit)
    
    return fl


def calc_final_limit_from_limit_cap_old4(final_limit, previous_limit, to_update_flag, reinstatement_reason, previous_is_iprs_validated, is_iprs_validated):
    fl = np.where((to_update_flag == 1) & (reinstatement_reason == 'limit review request'), final_limit,
         np.where((previous_is_iprs_validated == 'False') & (is_iprs_validated == 'True'), final_limit,   
         np.where(final_limit > previous_limit, previous_limit,
                  final_limit)))
    
    return fl


def calc_final_limit_from_limit_cap_10(final_limit, previous_limit, latest_loan, to_update_flag, reinstatement_reason, previous_is_iprs_validated, is_iprs_validated, due_date_rm_ge_rm_add_back, rllvr_date_102_check, loan_count, final_limit_non_zero):
    fl = np.where((to_update_flag == 1) & (reinstatement_reason == 'limit review request'), final_limit,
         np.where((previous_is_iprs_validated == 'False') & (is_iprs_validated == 'True'), final_limit,
         np.where((previous_limit == 0) & (final_limit > 0) & (loan_count == 0), final_limit,
         np.where((previous_limit == 0) & (final_limit > 0) & (loan_count > 0) & (final_limit >= final_limit_non_zero*1.5), final_limit_non_zero*1.5,
         np.where((rllvr_date_102_check == 1) & (final_limit > 0) & (final_limit >= previous_limit*0.85) & (final_limit <= previous_limit*1.00), previous_limit,
         np.where((rllvr_date_102_check == 1) & (final_limit > 0) & (final_limit > previous_limit*1.00) & (final_limit < previous_limit*1.5), final_limit,
         np.where((rllvr_date_102_check == 1) & (final_limit > 0) & (final_limit >= previous_limit*1.5), previous_limit*1.5,
                   final_limit)))))))
                  
    return fl


def calc_final_limit_from_limit_cap(final_limit, previous_limit, latest_loan, to_update_flag, reinstatement_reason, previous_is_iprs_validated, is_iprs_validated, due_date_rm_ge_rm_add_back, rllvr_date_102_check, loan_count, final_limit_non_zero):
    fl = np.where((to_update_flag == 1) & (reinstatement_reason == 'limit review request'), final_limit,
         np.where((previous_limit == 0) & (previous_is_iprs_validated == 'False') & (is_iprs_validated == 'True'), final_limit,
         np.where((previous_limit == 0) & (final_limit > 0) & (loan_count == 0), final_limit,
         np.where((previous_limit == 0) & (final_limit > 0) & (loan_count > 0) & (final_limit_non_zero == 0), final_limit,
         np.where((previous_limit == 0) & (final_limit > 0) & (loan_count > 0) & (final_limit >= final_limit_non_zero*1.5), final_limit_non_zero*1.5,
         np.where((previous_limit > 0) & (rllvr_date_102_check == 1) & (final_limit > 0) & (final_limit >= previous_limit*0.85) & (final_limit <= previous_limit*1.00), previous_limit,
         np.where((previous_limit > 0) & (rllvr_date_102_check == 1) & (final_limit > 0) & (final_limit > previous_limit*1.00) & (final_limit < previous_limit*1.5), final_limit,
         np.where((previous_limit > 0) & (rllvr_date_102_check == 1) & (final_limit > 0) & (final_limit >= previous_limit*1.5), previous_limit*1.5,
                   final_limit))))))))
                  
    return fl


def calc_final_limit_from_limit_cap_9(final_limit, previous_limit, latest_loan, to_update_flag, reinstatement_reason, previous_is_iprs_validated, is_iprs_validated, due_date_rm_ge_rm_add_back, rllvr_date_102_check, loan_count, final_limit_non_zero):
    fl = np.where((to_update_flag == 1) & (reinstatement_reason == 'limit review request'), final_limit,
         np.where((previous_is_iprs_validated == 'False') & (is_iprs_validated == 'True'), final_limit,
         np.where((previous_limit == 0) & (final_limit > 0), final_limit,
         np.where((rllvr_date_102_check == 1) & (final_limit > 0) & (final_limit >= previous_limit*0.85) & (final_limit <= previous_limit*1.00), previous_limit,
         np.where((rllvr_date_102_check == 1) & (final_limit > 0) & (final_limit > previous_limit*1.00) & (final_limit < previous_limit*1.5), final_limit,
         np.where((rllvr_date_102_check == 1) & (final_limit > 0) & (final_limit >= previous_limit*1.5), previous_limit*1.5,
                   final_limit))))))
                  
    return fl


def calc_final_limit_from_limit_cap_old6(final_limit, previous_limit, to_update_flag, reinstatement_reason, previous_is_iprs_validated, is_iprs_validated, rllvr_date_rm_ge_rm_limit_increase):
    fl = np.where((to_update_flag == 1) & (reinstatement_reason == 'limit review request'), final_limit,
         np.where((previous_is_iprs_validated == 'False') & (is_iprs_validated == 'True'), final_limit,
         np.where((rllvr_date_rm_ge_rm_limit_increase == 1) & (final_limit > 0) & ((final_limit >= (previous_limit * 0.85)) | (final_limit <= (previous_limit * 1.15))), previous_limit,
         np.where((rllvr_date_rm_ge_rm_limit_increase == 1) & (final_limit > 0) & (final_limit >= (previous_limit * 1.5)), previous_limit * 1.5,
         np.where(final_limit > previous_limit, previous_limit,
                  final_limit)))))
    
    return fl


def calc_final_limit_from_limit_cap_old7(final_limit, previous_limit, to_update_flag, reinstatement_reason, previous_is_iprs_validated, is_iprs_validated, due_date_rm_ge_rm_add_back, rllvr_date_102_check):
    fl = np.where((to_update_flag == 1) & (reinstatement_reason == 'limit review request'), final_limit,
         np.where((previous_is_iprs_validated == 'False') & (is_iprs_validated == 'True'), final_limit,
         np.where((rllvr_date_102_check == 1) & (final_limit > 0) & ((final_limit >= (previous_limit * 0.85)) & (final_limit <= (previous_limit * 1.15))), previous_limit,
         np.where((rllvr_date_102_check == 1) & (final_limit > 0) & ((final_limit > (previous_limit * 1.15)) & (final_limit < (previous_limit * 1.5))), final_limit,
         np.where((rllvr_date_102_check == 1) & (final_limit > 0) & (final_limit >= (previous_limit * 1.5)), previous_limit * 1.5,
         np.where(((rllvr_date_102_check == 0) | (final_limit == 0)) & (final_limit > previous_limit), previous_limit,
                  final_limit))))))
                  
    return fl

def calc_final_limit_from_limit_cap_old8(final_limit, previous_limit, to_update_flag, reinstatement_reason, previous_is_iprs_validated, is_iprs_validated, due_date_rm_ge_rm_add_back, rllvr_date_102_check):
    fl = np.where((to_update_flag == 1) & (reinstatement_reason == 'limit review request'), final_limit,
         np.where((previous_is_iprs_validated == 'False') & (is_iprs_validated == 'True'), final_limit,
         np.where((rllvr_date_102_check == 1) & (final_limit > 0) & (final_limit >= previous_limit*0.85) & (final_limit <= previous_limit*1.00), previous_limit,
         np.where((rllvr_date_102_check == 1) & (final_limit > 0) & (final_limit > previous_limit*1.00) & (final_limit < previous_limit*1.5), final_limit,
         np.where((rllvr_date_102_check == 1) & (final_limit > 0) & (final_limit >= previous_limit*1.5), previous_limit*1.5,
                   final_limit)))))
                  
    return fl


def calc_final_limit_from_limit_cap_8(final_limit, previous_limit, latest_loan, to_update_flag, reinstatement_reason, previous_is_iprs_validated, is_iprs_validated, due_date_rm_ge_rm_add_back, rllvr_date_102_check):
    fl = np.where((final_limit == 0), 0,
         np.where((to_update_flag == 1) & (reinstatement_reason == 'limit review request') & (latest_loan == 0), final_limit,
         np.where((to_update_flag == 1) & (reinstatement_reason == 'limit review request') & (latest_loan > 0) & (final_limit > 0), latest_loan,
         np.where((previous_is_iprs_validated == 'False') & (is_iprs_validated == 'True') & (latest_loan == 0), final_limit,
         np.where((previous_is_iprs_validated == 'False') & (is_iprs_validated == 'True') & (latest_loan > 0) & (final_limit > 0), latest_loan,
         np.where((latest_loan == 0) & (final_limit > 0), final_limit,
         np.where((rllvr_date_102_check == 1) & (final_limit > 0) & (final_limit >= latest_loan * 0.85) & (final_limit <= latest_loan * 1.00), latest_loan,
         np.where((rllvr_date_102_check == 1) & (final_limit > 0) & (final_limit > latest_loan * 1.00) & (final_limit < latest_loan * 1.5), final_limit,
         np.where((rllvr_date_102_check == 1) & (final_limit > 0) & (final_limit >= latest_loan * 1.5), latest_loan * 1.5,
                   final_limit)))))))))
                  
    return fl


def calc_final_limit_from_limit_cap_last_loan(final_limit, previous_limit, latest_loan, loan_count, final_limit_non_zero):
    fl = np.where((loan_count == 0), final_limit,
         np.where((final_limit <= previous_limit), final_limit,
         np.where((loan_count > 0) & (previous_limit > 0) & (latest_loan*1.5 <= previous_limit), previous_limit,
         np.where((loan_count > 0) & (previous_limit > 0) & (latest_loan*1.5 > previous_limit) & (latest_loan*1.5 < final_limit), latest_loan*1.5,
         np.where((loan_count > 0) & (previous_limit == 0) & (latest_loan*1.5 <= final_limit_non_zero), final_limit,
         np.where((loan_count > 0) & (previous_limit == 0) & (latest_loan*1.5 > final_limit_non_zero) & (latest_loan*1.5 < final_limit), latest_loan*1.5,
                 final_limit))))))
                  
    return fl


def calc_final_limit_from_snapshot_cap(final_limit, snapshot):
    f1 = np.where((snapshot == 0) & (final_limit > 0), final_limit,
         np.where((final_limit >= snapshot * 1.5), snapshot * 1.5, final_limit))
    
    return f1


# def calc_final_limit_for_1_day(due_date_rm_ge_rm_1d, final_limit):
#     return np.where(due_date_rm_ge_rm_1d == 1, final_limit, 0)


def calc_final_limit_from_dpd30(days_past_due, final_limit):
    """Limit adjustment rule 1: 
    Merchants that have DPD 30 plus on loans get a 50% limit reduction across all limits 
    (1-day, 7-day and 21-day)
    subject to product minimum rules
    """
    return np.where(days_past_due >= 30, 0.5 * final_limit, final_limit)


def calc_final_limit_from_kyc_flag(is_iprs_validated, mobile_number, final_limit):   
    return np.where((is_iprs_validated == 'False') | (mobile_number == 'None'), 0, final_limit)


def calc_final_limit_from_dpd15(days_past_due, final_limit):
    """Limit adjustment rule 1: 
    Merchants that have DPD 30 plus on loans get a 50% limit reduction across all limits 
    (1-day, 7-day and 21-day)
    subject to product minimum rules
    """
    return np.where(days_past_due > 15, 0, final_limit)


def calc_final_limit_from_last_disb90(num_days_since_last_disbursement, rllvr_date_rm_ge_rm_add_back, final_limit):
    """
    Limit adjustment rule 1: 
    Merchants that have not taken any loans in the last 90 days get a limit of 0 across all products 
    (1-day, 7-day and 21-day)
    subject to product minimum rules
    """
    return np.where((num_days_since_last_disbursement > 90) & (rllvr_date_rm_ge_rm_add_back == 0), 0, final_limit)
    # return np.where(num_days_since_last_disbursement > 90, 0, final_limit)
    # return np.where(num_days_since_last_disbursement <= 90, final_limit, 0)
    
    
def calc_final_limit_from_last_disb90_new(num_days_since_last_disbursement, due_date_100_check, rllvr_date_102_check, final_limit):
    """
    Limit adjustment rule 1: 
    Merchants that have not taken any loans in the last 90 days get a limit of 0 across all products 
    (1-day, 7-day and 21-day)
    subject to product minimum rules
    """
    f3 =  np.where((num_days_since_last_disbursement <= 90), final_limit,
          np.where((num_days_since_last_disbursement > 90) & (due_date_100_check == 1) & (due_date_100_check == 1), final_limit,   
                    0))
    
    return f3


def approximate_21_day_limit(config, avg_7_day_principal_disbursed):
    # Load configurations
    seven_day_limit_factor = config["approximate_21_day_limit_config"]["seven_day_limit_factor"]
    twenty_one_day_limit_factor = config["approximate_21_day_limit_config"]["twenty_one_day_limit_factor"]
    product_cap = config["approximate_21_day_limit_config"]["product_cap"]
    zero = config["approximate_21_day_limit_config"]["zero"]
    
    # Adjusted weighted average 7 day principal in last 3 months feature
    operation = (np.ceil(((avg_7_day_principal_disbursed * twenty_one_day_limit_factor) / seven_day_limit_factor) / 100) * 100).astype(int, errors="ignore")
    
    # Conditions
    conditions = [avg_7_day_principal_disbursed.eq(zero),
                  operation.le(product_cap),
                  operation.gt(product_cap)]
    
    # Choices
    choices = [zero,
               operation,
               product_cap]
    
    # Adjusted 21 day limit feature
    limit_col = np.select(conditions, choices)
    
    return limit_col


def bloom_21d_graduation_new_limits(df, max_dpd_30_data_path):
    # Load snapshot
    max_dpd_30 = pd.read_parquet(max_dpd_30_data_path)

    # Filter loans summary df to see how many customers had a 21 day limit
    # target_customers_scope = df[(df["total_final_21_limit"] == 0) & (df["final_21_limit"] > 0) & (df["final_7_limit"] > 0) & (df["is_location_mapped"] == 'True')]
    target_customers_scope = df[(df["previous_21_limit"] == df['previous_7_limit']) & (df["final_21_limit"] > 0) & (df["final_7_limit"] > 0) & (df["is_location_mapped"] == 'True')]

    # Check to make sure customer has no history of >30 DPD
    ntarget_customers_scope = target_customers_scope[target_customers_scope["store_number"].isin(max_dpd_30["store_number"])]

    # Apply thresholds
    mtarget_customers_scope = ntarget_customers_scope[(ntarget_customers_scope["good_loans_repayment_ratio(7_day_loans)"] >= 0.9) &
                                                      (ntarget_customers_scope["21_day_limit"] >= 7500)]
    
        
    # Ensure they are above hurdle rates thresholds
    ltarget_customers_scope = mtarget_customers_scope[(mtarget_customers_scope['due_date_rm_ge_rm_add_back'] == 1) & 
                                                      (mtarget_customers_scope['rllvr_date_rm_ge_rm_add_back'] == 1)]
    
    ltarget_customers_scope['l21_day_graduation_flag_new_limits'] = 'pass'
    
    in_df_not_in_ltarget_customers_scope = df[~df['store_number'].isin(ltarget_customers_scope['store_number'].tolist())]
    
    in_df_not_in_ltarget_customers_scope['l21_day_graduation_flag_new_limits'] = 'fail'
    
    df = pd.concat([ltarget_customers_scope, in_df_not_in_ltarget_customers_scope], axis=0)

    return df


def calc_new_21_day_limits_assignment(previous_21_limit, previous_7_limit, final_21_limit, l21_day_graduation_flag_new_limits):
    """
    Limit adjustment rule 1: 
    Merchants that have not taken any loans in the last 90 days get a limit of 0 across all products 
    (1-day, 7-day and 21-day)
    subject to product minimum rules
    """
    # return np.where((total_final_21_limit == 0) & (l21_day_graduation_flag_new_limits == 'fail'), 0, final_21_limit)
    return np.where((previous_21_limit == previous_7_limit) & (l21_day_graduation_flag_new_limits == 'fail'), 0, final_21_limit)


def bloom_21d_graduation(df, max_dpd_30_data_path):
    # Load snapshot
    max_dpd_30 = pd.read_parquet(max_dpd_30_data_path)

    # Filter loans summary df to see how many customers had a 21 day limit
    target_customers_scope = df[df["final_21_limit"] > 0]

    # Check to make sure customer has no history of >30 DPD
    ntarget_customers_scope = target_customers_scope[target_customers_scope["store_number"].isin(max_dpd_30["store_number"])]

    # Apply thresholds
    mtarget_customers_scope = ntarget_customers_scope[(ntarget_customers_scope["good_loans_repayment_ratio(7_day_loans)"] >= 0.9) &
                                                      (ntarget_customers_scope["21_day_limit"] >= 7500)]
    
    mtarget_customers_scope['21_day_graduation_flag'] = 'pass'
    
    # Get adjusted final 21 limits
    final_21_day = mtarget_customers_scope[['store_number', 'final_21_limit', '21_day_graduation_flag']]
    df_adj = df.drop(columns=['final_21_limit'])
    
    # Merge data sets
    final_limits = df_adj.merge(final_21_day, on=['store_number'], how='left')
    final_limits['final_21_limit'].fillna(0, inplace=True)
    final_limits['21_day_graduation_flag'].fillna('fail', inplace=True)

    return final_limits


def calc_final_21_limits(bf_final_21_limit, af_final_21_limit, af95_final_21_limit):
    f21l = np.where((af_final_21_limit == 0) & (af95_final_21_limit > 0), af95_final_21_limit,
           np.where((af_final_21_limit >= 0) & (af95_final_21_limit == 0), af_final_21_limit,
                    np.nan))
    
    return f21l


def calc_final_7_limit_cap(final_21_limit, final_7_limit, loan_count, rein_update_flag):
    conditions = [
        (rein_update_flag == 1),
        (final_21_limit == 0) & (loan_count == 0),
        (final_21_limit == 0) & (loan_count >= 1) & (loan_count <= 2),
        (final_21_limit == 0) & (loan_count >= 3) & (loan_count <= 4),
        (final_21_limit == 0) & (loan_count >= 5) & (loan_count <= 6),
        (final_21_limit == 0) & (loan_count >= 7) & (loan_count <= 8),
        (final_21_limit == 0) & (loan_count >= 9) & (loan_count <= 10),
        (final_21_limit == 0) & (loan_count >= 11) & (loan_count <= 12),
        (final_21_limit == 0) & (loan_count > 12),
        (final_21_limit > 0)
    ]
    choices = [
        final_7_limit,
        np.minimum(final_7_limit, 30000),
        np.minimum(final_7_limit, 35000),
        np.minimum(final_7_limit, 40000),
        np.minimum(final_7_limit, 45000),
        np.minimum(final_7_limit, 50000),
        np.minimum(final_7_limit, 55000),
        np.minimum(final_7_limit, 60000),
        np.minimum(final_7_limit, 70000),
        np.minimum(final_7_limit, final_21_limit/2)
    ]
    
    f7l = np.select(conditions, choices, np.nan)
    
    return f7l


def zeroize_new_cust_less_than_six_months(final_limit, opt_in_date, ref_date, trxn_period_in_scope):
    six_months_ago = pd.Timestamp(ref_date) + dt.timedelta(days=trxn_period_in_scope)

    return np.where(opt_in_date > six_months_ago, 0, final_limit)


def restore_21d_limits_zeroized_but_rmdd_ge95(new_limits, before_21d_graduation_limits_data_path):
    # Load snapshot
    previous_limits = pd.read_parquet(before_21d_graduation_limits_data_path)

    # After 21 day loan graduation
    new_limits['final_21_limit'] = new_limits['final_21_limit'].fillna(0)
    new_limits.rename(columns={'final_21_limit': 'af_final_21_limit'}, inplace=True)

    # Before 21 day loan graduation
    previous_limits = previous_limits[['store_number', 'final_21_limit']]
    previous_limits.rename(columns={'final_21_limit': 'bf_final_21_limit'}, inplace=True)

    # Average repayment milestone of 102% by due date
    principal_repayments_by_due_date_95 = new_limits[(new_limits['due_date_rm_ge_rm_add_back'] == 1) & (new_limits['rllvr_date_rm_ge_rm_add_back'] == 1)]

    # Merge before and after limits
    all_limits = previous_limits.merge(new_limits, on=['store_number'], how='right')

    # Retrieve clients zeroized after 21 day graduation
    zeroized = all_limits[(all_limits['bf_final_21_limit'] > 0) & (all_limits['af_final_21_limit'] == 0)]
    
    # Zeroized clients who have average repayment milestone of 102% by due date
    target_customers_scope = zeroized[zeroized["store_number"].isin(principal_repayments_by_due_date_95['store_number'])]
    target_customers_scope = target_customers_scope[['store_number', 'bf_final_21_limit']]
    target_customers_scope.rename(columns={'bf_final_21_limit': 'af95_final_21_limit'}, inplace=True)

    # Advance previous limits to clients zeroized after 21 day graduation but have repayment milestone of 95% by due date across all loans
    comb_limits = all_limits.merge(target_customers_scope, on=['store_number'], how='left')
    comb_limits = comb_limits.fillna(0)

    # Adjusted final 21 limits feature
    comb_limits['final_21_limit'] = calc_final_21_limits(comb_limits['bf_final_21_limit'], comb_limits['af_final_21_limit'], comb_limits['af95_final_21_limit'])
    comb_limits['new_final_21_limit'] = comb_limits['final_21_limit']

    # Get adjusted final 21 limits
    final_21_day = comb_limits[['store_number', 'bf_final_21_limit', 'af_final_21_limit', 'af95_final_21_limit', 'final_21_limit', 'new_final_21_limit']]
    new_limits_adj = new_limits.drop(columns=['af_final_21_limit'])
    # new_limits_adj = new_limits.copy()
    
    # Merge data sets
    final_limits = new_limits_adj.merge(final_21_day, on=['store_number'], how='left')

    return final_limits


def calc_final_limit_saf_credit_score(old_src_crdt_score, final_limit):
    return np.where((old_src_crdt_score >= 0) & (old_src_crdt_score <= 477), 0, final_limit)


def calc_zeroized(old_final_limit, new_final_limit_1):
    return np.where((new_final_limit_1 == 0) & (old_final_limit > 0), 1, 0)


def updated_limit_old(zeroized, old_final_limit, new_final_limit_1, average_repayments_by_due_date, average_repayments_by_rollover_end_date, average_repayments_by_dpd90): 
    return np.where((zeroized == 1) & (average_repayments_by_due_date == 1) & (average_repayments_by_rollover_end_date == 1) & (average_repayments_by_dpd90 == 1), old_final_limit, new_final_limit_1)


def updated_limit(zeroized, old_final_limit, new_final_limit_1, average_repayments_by_due_date, average_repayments_by_rollover_end_date): 
    return np.where((zeroized == 1) & (average_repayments_by_due_date == 1) & (average_repayments_by_rollover_end_date == 1), old_final_limit, new_final_limit_1)


def restore_1d_7d_limits_rm_hurdle_rate(new_limits_raw, before_21d_graduation_limits_data_path, term_frequency):
    # Load snapshot
    previous_limits = pd.read_parquet(before_21d_graduation_limits_data_path)

    # After zeroizing bloom 1 and 7 limits for merchants with a Safaricom score between 0 and 477
    new_limits = new_limits_raw[['store_number', 'new_final_21_limit', 'updated_final_7_limit', 'updated_final_1_limit']]
    new_limits.rename(columns={'new_final_21_limit': 'new_final_21_limit_1', 'updated_final_7_limit': 'new_final_7_limit_1', 'updated_final_1_limit': 'new_final_1_limit_1'}, inplace=True)

    # Before 21 day loan graduation
    previous_limits = previous_limits[['store_number', 'final_21_limit', 'final_7_limit', 'final_1_limit']]
    previous_limits.rename(columns={'final_21_limit': 'old_final_21_limit', 'final_7_limit': 'old_final_7_limit', 'final_1_limit': 'old_final_1_limit'}, inplace=True)

    # Merge before and after limits
    combined = previous_limits.merge(new_limits, on=['store_number'], how='right')

    # 
    for tf in term_frequency:
        combined[f'zeroized_{tf}'] = calc_zeroized(combined[f'old_final_{tf}_limit'], combined[f'new_final_{tf}_limit_1'])
    
    # 
    final_combined = new_limits_raw.merge(combined, how="left", on=["store_number"])
    # final_combined = final_combined[(final_combined['due_date_rm_ge96p'] == 1) & (final_combined['rllvr_date_rm_ge102p'] == 1) & (final_combined['dpd90_rm_ge104p'] == 1)]
    
    # 
    for tf in term_frequency:
        final_combined[f'updated_{tf}_limit'] = updated_limit(final_combined[f'zeroized_{tf}'], final_combined[f'old_final_{tf}_limit'], final_combined[f'new_final_{tf}_limit_1'], final_combined['due_date_rm_ge_rm_add_back'], final_combined['rllvr_date_rm_ge_rm_add_back'])
        final_combined[f'final_{tf}_limit'] = updated_limit(final_combined[f'zeroized_{tf}'], final_combined[f'old_final_{tf}_limit'], final_combined[f'new_final_{tf}_limit_1'], final_combined['due_date_rm_ge_rm_add_back'], final_combined['rllvr_date_rm_ge_rm_add_back'])

    return final_combined


def updated_new_limit(updated_limit, new_final_limit_1, old_final_limit):
    return np.where((new_final_limit_1 > 0) & (updated_limit == 0), old_final_limit, updated_limit)


def calc_final_limit_for_1_day(due_date_rm_ge_rm_1d, final_limit):
    return np.where(due_date_rm_ge_rm_1d == 1, final_limit, 0)


def calc_ftd_limit(to_ftd_update_flag, model_630_max_global_limit, model_630_limit, final_limit, balance):
    return np.where((to_ftd_update_flag == 1) & (model_630_max_global_limit > balance), model_630_limit, final_limit)


def update_ftd_blacklist_flag(to_ftd_update_flag, model_630_max_global_limit, balance, blacklist_flag):
    return np.where((to_ftd_update_flag == 1) & (model_630_max_global_limit > balance), 0, blacklist_flag)


def calc_rein_7_limit(to_rein_update_flag, rein_7_limit, final_7_limit):
    return np.where((to_rein_update_flag == 1), rein_7_limit, final_7_limit)


def calc_rein_21_limit(to_rein_update_flag, rein_7_limit, final_21_limit):
    return np.where((to_rein_update_flag == 1), rein_7_limit, final_21_limit)


def calc_rein_1_limit(to_rein_update_flag, final_1_limit):
    return np.where((to_rein_update_flag == 1), 0, final_1_limit)


def update_rein_blacklist_flag(to_rein_update_flag, blacklist_flag):
    return np.where((to_rein_update_flag == 1), 0, blacklist_flag)


def reinstate_first_time_defaulters_limits(df, ftd_clean_data_parquet, term_frequency):
    # Load data frame with customer details
    ftd_details = pd.read_parquet(ftd_clean_data_parquet)
    
    # del df['update_flag']

    # Merge datasets
    df = df.merge(ftd_details, how="left", on=["store_number"])
    df['ftd_update_flag'].fillna(0, inplace=True)

    # Get max global limit feature
    df["model_630_max_global_limit"] = df[["model_630_21_limit", "model_630_7_limit", "model_630_1_limit"]].max(axis=1)

    # Reinstate limit
    for tf in term_frequency:
        df[f'final_{tf}_limit'] = calc_ftd_limit(df['ftd_update_flag'], df[f'model_630_max_global_limit'], df[f'model_630_{tf}_limit'], df[f'final_{tf}_limit'], df['loan_balance'])
    
    # Update blacklist flag
    df['blacklist_flag'] = update_ftd_blacklist_flag(df['ftd_update_flag'], df[f'model_630_max_global_limit'], df['loan_balance'], df['blacklist_flag'])

    return df


def reinstate_defaulters_limits(df, rein_clean_data_parquet):
    # Load data frame with customer details
    rein_details = pd.read_parquet(rein_clean_data_parquet)
    
    print(rein_details[rein_details['store_number'] == '968526'])
    
    print(rein_details.head(2))
    # del rein_details['reinstatement_reason']

    # Merge datasets
    df = df.merge(rein_details, how="left", on=["store_number"])
    df['rein_update_flag'].fillna(0, inplace=True)
    df['rein_7_limit'].fillna(0, inplace=True)

    # Reinstate limit
    df['final_7_limit'] = calc_rein_7_limit(df['rein_update_flag'], df['rein_7_limit'], df['final_7_limit'])
    
    df['final_21_limit'] = calc_rein_21_limit(df['rein_update_flag'], df['rein_7_limit'], df['final_21_limit'])
    
    df['final_1_limit'] = calc_rein_1_limit(df['rein_update_flag'], df['final_1_limit'])
    
    # Update blacklist flag
    df['blacklist_flag'] = update_rein_blacklist_flag(df['rein_update_flag'], df['blacklist_flag'])

    return df


def never_borrowed_hurdle_rates_have_borrowed(agg_summary, cluster_hr_path, clf_model_path, clf_scaling_path, nbfis):
    # Load artifacts
    with open(clf_scaling_path, "rb") as f:
        clf_scaling = pickle.load(f)
    
    # with open(clf_dim_reduction_path, "rb") as f:
    #     clf_dim_reduction = pickle.load(f)
    
    with open(clf_model_path, "rb") as f:
        clf_model = pickle.load(f)
    
    cluster_hurdle_rates = pd.read_parquet(cluster_hr_path)

    # Max global limit feature
    agg_summary["max_global_limit"] = agg_summary[["final_21_limit", "final_7_limit", "final_1_limit"]].max(axis=1)
    
    # Max global limit feature
    agg_summary["total_max_global_limit"] = agg_summary[["total_final_21_limit", "total_final_7_limit", "total_final_1_limit"]].max(axis=1)

    # Filter for repayment by due date
    cluster_hurdle_rates_dd = cluster_hurdle_rates[cluster_hurdle_rates["repayment_milestone"] == "dd"]
    cluster_hurdle_rates_dd.rename(columns={'hurdle_rate': 'hurdle_rate_dd'}, inplace=True)
    print('\n', cluster_hurdle_rates_dd)

    # Filter for repayment by rollover end date
    cluster_hurdle_rates_erd = cluster_hurdle_rates[cluster_hurdle_rates["repayment_milestone"] == "erd"]
    cluster_hurdle_rates_erd.rename(columns={'hurdle_rate': 'hurdle_rate_erd'}, inplace=True)
    print('\n', cluster_hurdle_rates_erd)
    
    # Filter for repayment by rollover end date
    cluster_hurdle_rates_dpd30 = cluster_hurdle_rates[cluster_hurdle_rates["repayment_milestone"] == "dpd30"]
    cluster_hurdle_rates_dpd30.rename(columns={'hurdle_rate': 'hurdle_rate_dpd30'}, inplace=True)
    print('\n', cluster_hurdle_rates_dpd30)    

    # Never borrowed subset
    never_borrowed_list = agg_summary[(agg_summary['never_borrowed_flag'] == 0) & (agg_summary['final_7_limit'] > 0)]
    
    # never_borrowed_list = agg_summary.copy()

    # Handle missing values
    never_borrowed_list['days_since_last_trx'].fillna(31, inplace=True)

    # Predict assigned cluster stage 1
    never_borrowed_list["repayments_by_erd_vs_principal_cluster"] = clf_model.predict(clf_scaling.transform(never_borrowed_list[nbfis]))

    # Align cluster classes stage 1
    never_borrowed_list["repayments_by_erd_vs_principal_cluster"] = never_borrowed_list["repayments_by_erd_vs_principal_cluster"]
    print('\nClusters of never borrowed limits stage 1:\n', never_borrowed_list['repayments_by_erd_vs_principal_cluster'].unique(), sep='')
    print(never_borrowed_list['repayments_by_erd_vs_principal_cluster'].value_counts())
    
    average_performance = never_borrowed_list.groupby("repayments_by_erd_vs_principal_cluster")[["hurdle_rate_by_due_date_mean", "hurdle_rate_by_end_rollover_date_mean", "hurdle_rate_by_dpd30_mean"]].median()

    print(average_performance)
    
    return never_borrowed_list


def never_borrowed_hurdle_rates_old(agg_summary, cluster_hr_path, cluster_model_1_path, cluster_model_2_path, cluster_scaling_path, cluster_dim_reduction_path, nbfis):
    # Load artifacts
    with open(cluster_scaling_path, "rb") as f:
        clustering_scaling = pickle.load(f)
    
    with open(cluster_dim_reduction_path, "rb") as f:
        clustering_dim_reduction = pickle.load(f)
    
    with open(cluster_model_1_path, "rb") as f:
        clustering_model_1 = pickle.load(f)
    
    with open(cluster_model_2_path, "rb") as f:
        clustering_model_2 = pickle.load(f)
    
    cluster_hurdle_rates = pd.read_parquet(cluster_hr_path)

    # Max global limit feature
    agg_summary["max_global_limit"] = agg_summary[["final_21_limit", "final_7_limit", "final_1_limit"]].max(axis=1)
    
    # Max global limit feature
    agg_summary["total_max_global_limit"] = agg_summary[["total_final_21_limit", "total_final_7_limit", "total_final_1_limit"]].max(axis=1)
    
    print(agg_summary['never_borrowed_flag'].value_counts())

    # Filter for repayment by due date
    cluster_hurdle_rates_dd = cluster_hurdle_rates[cluster_hurdle_rates["repayment_milestone"] == "dd"]
    cluster_hurdle_rates_dd.rename(columns={'hurdle_rate': 'hurdle_rate_dd'}, inplace=True)
    print('\n', cluster_hurdle_rates_dd)

    # Filter for repayment by rollover end date
    cluster_hurdle_rates_erd = cluster_hurdle_rates[cluster_hurdle_rates["repayment_milestone"] == "erd"]
    cluster_hurdle_rates_erd.rename(columns={'hurdle_rate': 'hurdle_rate_erd'}, inplace=True)
    print('\n', cluster_hurdle_rates_erd)
    
    # Filter for repayment by rollover end date
    cluster_hurdle_rates_dpd30 = cluster_hurdle_rates[cluster_hurdle_rates["repayment_milestone"] == "dpd30"]
    cluster_hurdle_rates_dpd30.rename(columns={'hurdle_rate': 'hurdle_rate_dpd30'}, inplace=True)
    print('\n', cluster_hurdle_rates_dpd30)    

    # Never borrowed subset
    never_borrowed_list_stage1 = agg_summary[agg_summary['never_borrowed_flag'] == 1]
    
    print(never_borrowed_list_stage1.shape)

    # Handle missing values
    never_borrowed_list_stage1['days_since_last_trx'].fillna(31, inplace=True)

    # Predict assigned cluster stage 1
    never_borrowed_list_stage1["repayments_by_erd_vs_principal_cluster_stage1"] = clustering_model_1.fit_predict(clustering_dim_reduction.transform(clustering_scaling.transform(never_borrowed_list_stage1[nbfis])))

    # Align cluster classes stage 1
    never_borrowed_list_stage1["repayments_by_erd_vs_principal_cluster_stage1"] = never_borrowed_list_stage1["repayments_by_erd_vs_principal_cluster_stage1"] + 1
    print('\nClusters of never borrowed limits stage 1:\n', never_borrowed_list_stage1['repayments_by_erd_vs_principal_cluster_stage1'].unique(), sep='')

    # Retrive data set for stage 2
    never_borrowed_list_stage2 = never_borrowed_list_stage1[never_borrowed_list_stage1["repayments_by_erd_vs_principal_cluster_stage1"] == 3]
    
    # Predict assigned cluster stage 2
    never_borrowed_list_stage2["repayments_by_erd_vs_principal_cluster_stage2"] = clustering_model_2.fit_predict(clustering_dim_reduction.transform(clustering_scaling.transform(never_borrowed_list_stage2[nbfis])))

    # Align cluster classes stage 2
    never_borrowed_list_stage2["repayments_by_erd_vs_principal_cluster_stage2"] = never_borrowed_list_stage2["repayments_by_erd_vs_principal_cluster_stage2"] + 1
    never_borrowed_list_stage2["repayments_by_erd_vs_principal_cluster_stage1"] = 3
    print('\nClusters of never borrowed limits stage 2:\n', never_borrowed_list_stage2['repayments_by_erd_vs_principal_cluster_stage2'].unique(), sep='')

    # Combine predictions from both models
    never_borrowed_list = never_borrowed_list_stage1.merge(never_borrowed_list_stage2[['store_number', 'repayments_by_erd_vs_principal_cluster_stage1', 'repayments_by_erd_vs_principal_cluster_stage2']], how='left', on=['store_number', 'repayments_by_erd_vs_principal_cluster_stage1'])
    never_borrowed_list['repayments_by_erd_vs_principal_cluster_stage2'].fillna(0, inplace=True)
    never_borrowed_list['repayments_by_erd_vs_principal_cluster'] = never_borrowed_list['repayments_by_erd_vs_principal_cluster_stage1'].astype('int').astype('str') + '.' + never_borrowed_list['repayments_by_erd_vs_principal_cluster_stage2'].astype('int').astype('str')
    never_borrowed_list['repayments_by_erd_vs_principal_cluster'] = never_borrowed_list['repayments_by_erd_vs_principal_cluster'].astype('float')
    print('\nClusters of never borrowed limits:\n', never_borrowed_list.loc[never_borrowed_list['final_7_limit'] > 0,'repayments_by_erd_vs_principal_cluster'].unique(), sep='')

    # Retrieve cluster mean
    never_borrowed_list_cluster_hurdle_rates = never_borrowed_list.merge(cluster_hurdle_rates_erd, how="left", left_on=["repayments_by_erd_vs_principal_cluster"], right_on=["cluster"])
    never_borrowed_list_cluster_hurdle_rates = never_borrowed_list_cluster_hurdle_rates.merge(cluster_hurdle_rates_dd, how="left", left_on=["repayments_by_erd_vs_principal_cluster"], right_on=["cluster"])
    never_borrowed_list_cluster_hurdle_rates = never_borrowed_list_cluster_hurdle_rates.merge(cluster_hurdle_rates_dpd30, how="left", left_on=["repayments_by_erd_vs_principal_cluster"], right_on=["cluster"])
    print('\nMax of never borrowed limits:\n', never_borrowed_list_cluster_hurdle_rates['hurdle_rate_erd'].max(), sep='')

    # Validate merge
    display(never_borrowed_list_cluster_hurdle_rates.groupby(['repayments_by_erd_vs_principal_cluster'])['hurdle_rate_erd'].mean())

    # Set hurdle rate flag
    never_borrowed_list_cluster_hurdle_rates['rllvr_date_rm_ge_rm_never_borrowed'] = np.where(never_borrowed_list_cluster_hurdle_rates["hurdle_rate_erd"] >= 1.02, 1, 0)

    # Merge data sets
    df = agg_summary.drop(columns=['max_global_limit']).merge(never_borrowed_list_cluster_hurdle_rates[['store_number', 'hurdle_rate_dd', 'hurdle_rate_erd', 'hurdle_rate_dpd30', 'rllvr_date_rm_ge_rm_never_borrowed']], how="left", on=["store_number"])

    # Handle missing values
    df['rllvr_date_rm_ge_rm_never_borrowed'].fillna(1, inplace=True)

    # Feature repayment by DD
    df['consolidated_repayments_by_dd'] = np.where(df['hurdle_rate_dd'].isna(), df['hurdle_rate_by_due_date_mean'], df['hurdle_rate_dd'])

    # Feature repayment by ERD
    df['consolidated_repayments_by_erd'] = np.where(df['hurdle_rate_erd'].isna(), df['hurdle_rate_by_end_rollover_date_mean'], df['hurdle_rate_erd'])

    # Feature repayment by DPD30
    df['consolidated_repayments_by_dpd30'] = np.where(df['hurdle_rate_dpd30'].isna(), df['hurdle_rate_by_dpd30_mean'], df['hurdle_rate_dpd30'])

    return df


def never_borrowed_hurdle_rates(config, agg_summary):
    project_dir = config["project_dir"]
    clf_scaling_path = config["clf_model_config"]["clf_scaling_pipeline_path"]
    # clf_dim_reduction = config["clf_model_config"]["clf_dim_reduction_pipeline_path"]
    clf_model_path = config["clf_model_config"]["clf_model_path"]
    cluster_hr_path = config["cluster_model_config"]["cluster_hurdle_rates_data_parquet"]
    nbfis = config["clf_model_config"]["never_borrowed_features_in_scope"]
    reg_clusters_in_scope = config["reg_model_config"]["reg_clusters_in_scope"]

    # Load artifacts
    with open(project_dir + clf_scaling_path, "rb") as f:
        clf_scaling = pickle.load(f)
    
    # with open(project_dir + clf_dim_reduction_path, "rb") as f:
    #     clf_dim_reduction = pickle.load(f)
    
    with open(project_dir + clf_model_path, "rb") as f:
        clf_model = pickle.load(f)
    
    cluster_hurdle_rates = pd.read_parquet(project_dir + cluster_hr_path)

    # Max global limit feature
    # agg_summary["max_global_limit"] = agg_summary[["final_21_limit", "final_7_limit", "final_1_limit"]].max(axis=1)
    
    # Filter for repayment by due date
    cluster_hurdle_rates_dd = cluster_hurdle_rates[cluster_hurdle_rates["repayment_milestone"] == "dd"]
    cluster_hurdle_rates_dd.rename(columns={'hurdle_rate': 'hurdle_rate_dd'}, inplace=True)
    print('\n', cluster_hurdle_rates_dd)

    # Filter for repayment by rollover end date
    cluster_hurdle_rates_erd = cluster_hurdle_rates[cluster_hurdle_rates["repayment_milestone"] == "erd"]
    cluster_hurdle_rates_erd.rename(columns={'hurdle_rate': 'hurdle_rate_erd'}, inplace=True)
    print('\n', cluster_hurdle_rates_erd)
    
    # Filter for repayment by rollover end date
    cluster_hurdle_rates_dpd30 = cluster_hurdle_rates[cluster_hurdle_rates["repayment_milestone"] == "dpd30"]
    cluster_hurdle_rates_dpd30.rename(columns={'hurdle_rate': 'hurdle_rate_dpd30'}, inplace=True)
    print('\n', cluster_hurdle_rates_dpd30)    

    # Never borrowed subset
    never_borrowed_list = agg_summary[agg_summary['never_borrowed_flag'] == 1]

    # Handle missing values
    never_borrowed_list['days_since_last_trx'].fillna(31, inplace=True)

    # Predict assigned cluster stage 1
    never_borrowed_list["repayments_by_erd_vs_principal_cluster"] = clf_model.predict(clf_scaling.transform(never_borrowed_list[nbfis]))
    print('\nClusters of never borrowed limits stage 1:\n', never_borrowed_list['repayments_by_erd_vs_principal_cluster'].unique(), sep='')
    print(never_borrowed_list['repayments_by_erd_vs_principal_cluster'].value_counts())

    # Retrieve cluster mean
    never_borrowed_list_cluster_hurdle_rates = never_borrowed_list.merge(cluster_hurdle_rates_erd[['target', 'hurdle_rate_erd']], how="left", left_on=["repayments_by_erd_vs_principal_cluster"], right_on=["target"])
    never_borrowed_list_cluster_hurdle_rates = never_borrowed_list_cluster_hurdle_rates.drop(columns=['target']).merge(cluster_hurdle_rates_dd[['target', 'hurdle_rate_dd']], how="left", left_on=["repayments_by_erd_vs_principal_cluster"], right_on=["target"])
    never_borrowed_list_cluster_hurdle_rates = never_borrowed_list_cluster_hurdle_rates.drop(columns=['target']).merge(cluster_hurdle_rates_dpd30[['target', 'hurdle_rate_dpd30']], how="left", left_on=["repayments_by_erd_vs_principal_cluster"], right_on=["target"])
    print('\nMax of never borrowed limits:\n', never_borrowed_list_cluster_hurdle_rates['hurdle_rate_erd'].max(), sep='')
    display(never_borrowed_list_cluster_hurdle_rates.groupby(['repayments_by_erd_vs_principal_cluster'])['hurdle_rate_erd'].mean())

    # Individual hurdle rates prediction
    for sub_cluster in reg_clusters_in_scope:
        never_borrowed_list_cluster_hurdle_rates = never_borrowed_regression_hurdle_rates(config, never_borrowed_list_cluster_hurdle_rates, sub_cluster)
    
    # Set hurdle rate flag
    never_borrowed_list_cluster_hurdle_rates['rllvr_date_rm_ge_rm_never_borrowed'] = np.where(never_borrowed_list_cluster_hurdle_rates["hurdle_rate_erd"] >= 1.00, 1, 0)
    
    # Define the conditions
    condition1 = (never_borrowed_list_cluster_hurdle_rates["repayments_by_erd_vs_principal_cluster"].isin([0, 1, 2])) & (never_borrowed_list_cluster_hurdle_rates["hurdle_rate_dpd30"] >= 1.03)
    condition2 = (never_borrowed_list_cluster_hurdle_rates["repayments_by_erd_vs_principal_cluster"].isin([4, 5, 8])) & (never_borrowed_list_cluster_hurdle_rates["hurdle_rate_dpd30"] >= 1.00)

    # Apply the conditions and assign values accordingly
    never_borrowed_list_cluster_hurdle_rates['dpd30_rm_ge_rm_never_borrowed'] = np.where(condition1 | condition2, 1, 0)
    # never_borrowed_list_cluster_hurdle_rates['dpd30_rm_ge_rm_never_borrowed'] = np.where(never_borrowed_list_cluster_hurdle_rates["hurdle_rate_dpd30"] >= 1.03, 1, 0)

    # Merge data sets
    df = agg_summary.merge(never_borrowed_list_cluster_hurdle_rates[['store_number', 'hurdle_rate_dd', 'hurdle_rate_erd', 'hurdle_rate_dpd30', 'rllvr_date_rm_ge_rm_never_borrowed', 'dpd30_rm_ge_rm_never_borrowed', 'repayments_by_erd_vs_principal_cluster']], how="left", on=["store_number"])

    # Handle missing values
    df['rllvr_date_rm_ge_rm_never_borrowed'].fillna(1, inplace=True)
    df['dpd30_rm_ge_rm_never_borrowed'].fillna(1, inplace=True)

    # Feature repayment by DD
    df['consolidated_repayments_by_dd'] = np.where(df['hurdle_rate_dd'].isna(), df['hurdle_rate_by_due_date_mean'], df['hurdle_rate_dd'])

    # Feature repayment by ERD
    df['consolidated_repayments_by_erd'] = np.where(df['hurdle_rate_erd'].isna(), df['hurdle_rate_by_end_rollover_date_mean'], df['hurdle_rate_erd'])

    # Feature repayment by DPD30
    df['consolidated_repayments_by_dpd30'] = np.where(df['hurdle_rate_dpd30'].isna(), df['hurdle_rate_by_dpd30_mean'], df['hurdle_rate_dpd30'])

    return df


def never_borrowed_regression_hurdle_rates(config, never_borrowed_list_cluster_hurdle_rates, sub_cluster):
    project_dir = config["project_dir"]
    reg_clusters_in_scope = config["reg_model_config"]["reg_clusters_in_scope"]
    nbfis = config["reg_model_config"]["never_borrowed_features_in_scope"]
    reg_scaling_path = config["reg_model_config"][f"reg_scaling_pipeline_cluster_{sub_cluster}_path"]
    reg_dim_reduction_path = config["reg_model_config"][f"reg_dim_reduction_cluster_{sub_cluster}_pipeline_path"]
    reg_model_erd_path = config["reg_model_config"][f"reg_model_erd_cluster_{sub_cluster}_path"]
    reg_model_dpd30_path = config["reg_model_config"][f"reg_model_dpd30_cluster_{sub_cluster}_path"]

    # Load artifacts
    with open(project_dir + reg_scaling_path, "rb") as f:
        reg_scaling = pickle.load(f)
    
    with open(project_dir + reg_dim_reduction_path, "rb") as f:
        reg_dim_reduction = pickle.load(f)
    
    # with open(project_dir + reg_model_erd_path, "rb") as f:
    #     reg_model_erd = pickle.load(f)
    
    with open(project_dir + reg_model_dpd30_path, "rb") as f:
        reg_model_dpd30 = pickle.load(f)
    
    # Filter sub-clusters in-scope of individual hr prediction
    never_borrowed_sub_clusters = never_borrowed_list_cluster_hurdle_rates[never_borrowed_list_cluster_hurdle_rates['target'] == sub_cluster]

    # Predict expected hurdle rates
    # never_borrowed_sub_clusters["repayments_by_erd_vs_principal_hr_dd"] = reg_model.predict(reg_scaling.transform(never_borrowed_sub_clusters[nbfis]))
    # never_borrowed_sub_clusters["repayments_by_erd_vs_principal_hr_erd"] = reg_model_erd.predict(reg_scaling.transform(never_borrowed_sub_clusters[nbfis]))
    never_borrowed_sub_clusters["repayments_by_erd_vs_principal_hr_dpd30"] = reg_model_dpd30.predict(reg_dim_reduction.transform(reg_scaling.transform(never_borrowed_sub_clusters[nbfis])))

    print('\nSub-clusters in-scope:\n', never_borrowed_sub_clusters['repayments_by_erd_vs_principal_cluster'].unique(), sep='')
    print('\nMin of never borrowed limits (sub-clusters):\n', never_borrowed_sub_clusters['repayments_by_erd_vs_principal_hr_dpd30'].min(), sep='')
    print('\nMax of never borrowed limits (sub-clusters):\n', never_borrowed_sub_clusters['repayments_by_erd_vs_principal_hr_dpd30'].max(), sep='')
    display(never_borrowed_sub_clusters.groupby(['repayments_by_erd_vs_principal_cluster'])['repayments_by_erd_vs_principal_hr_dpd30'].mean())
    display(never_borrowed_sub_clusters.groupby(['repayments_by_erd_vs_principal_cluster'])['repayments_by_erd_vs_principal_hr_dpd30'].median())
    # display(never_borrowed_sub_clusters.loc[(never_borrowed_sub_clusters['repayments_by_erd_vs_principal_hr_dpd30'] > 1.03), ['store_number', 'repayments_by_erd_vs_principal_hr_dpd30', 'final_21_limit', 'final_7_limit']])

    # Merge data sets
    # df = never_borrowed_list_cluster_hurdle_rates.merge(never_borrowed_sub_clusters[['store_number', 'repayments_by_erd_vs_principal_hr_dd']], how="left", on=["store_number"])
    # df = never_borrowed_list_cluster_hurdle_rates.merge(never_borrowed_sub_clusters[['store_number', 'repayments_by_erd_vs_principal_hr_erd']], how="left", on=["store_number"])
    try:
        df = never_borrowed_list_cluster_hurdle_rates.drop(columns=['repayments_by_erd_vs_principal_hr_dpd30']).merge(never_borrowed_sub_clusters[['store_number', 'repayments_by_erd_vs_principal_hr_dpd30']], how="left", on=["store_number"])
    except:
        df = never_borrowed_list_cluster_hurdle_rates.merge(never_borrowed_sub_clusters[['store_number', 'repayments_by_erd_vs_principal_hr_dpd30']], how="left", on=["store_number"])

    # Update hurdle rates    
    # df['hurdle_rate_dd'] = np.where(~(df["repayments_by_erd_vs_principal_hr_dd"].isna()), df["repayments_by_erd_vs_principal_hr_dd"], df['hurdle_rate_dd'])
    # df['hurdle_rate_erd'] = np.where(~(df["repayments_by_erd_vs_principal_hr_erd"].isna()), df["repayments_by_erd_vs_principal_hr_erd"], df['hurdle_rate_erd'])
    df['hurdle_rate_dpd30'] = np.where(~(df["repayments_by_erd_vs_principal_hr_dpd30"].isna()), df["repayments_by_erd_vs_principal_hr_dpd30"], df['hurdle_rate_dpd30'])

    return df


def calc_final_limit_never_borrowed_below_rm_hurdle_rate_old(final_limit, rllvr_date_rm_ge_rm_never_borrowed):
    return np.where(rllvr_date_rm_ge_rm_never_borrowed == 1, final_limit, 0)


def calc_final_limit_never_borrowed_below_rm_hurdle_rate(final_limit, dpd30_rm_ge_rm_never_borrowed):
    return np.where(dpd30_rm_ge_rm_never_borrowed == 1, final_limit, 0)


def get_multiple_ids_with_limits(df_scoring_stabilisation):
    risk = df_scoring_stabilisation.loc[:, ['store_number', 'national_id', 'final_21_limit', 'final_7_limit', 'final_1_limit']]
    risk["max_global_limit"] = risk[["final_21_limit", "final_7_limit", "final_1_limit"]].max(axis=1)
    risk = risk.loc[risk['max_global_limit'] > 0, :]
    # display(risk.head(2))
    # print(risk.max_global_limit.min())
    
    risk_grp_ni = risk.groupby(['national_id'])['store_number'].count()
    risk_grp_ni_dup = risk_grp_ni[risk_grp_ni > 1]
    # display(risk_grp_ni_dup.head(2))
    
    risk_multiple_stores = risk[risk['national_id'].isin(risk_grp_ni_dup.index)].sort_values(by=['national_id'])
    # display(risk_multiple_stores.head(2))
    
    return risk_multiple_stores


def load_active_loans(clean_loans_path):
    loans = pd.read_parquet(clean_loans_path)
    active_loans = loans.loc[(loans['loan_status'] == 300) & (loans['safaricom_loan_balance'] != 0), ['store_number', 'loan_mifos_id', 'bloom_version', 'loan_status', 'principal_disbursed', 'safaricom_loan_balance']]
    # active_loans['loan_id_product_concat'] = (active_loans["loan_mifos_id"].astype("str") + "-" + active_loans["bloom_version"].astype("str")).astype("str")
    # display(active_loans.head(2))
    # print(active_loans.loan_status.unique(), active_loans.safaricom_loan_balance.min())
    
    return active_loans


def zeroize_with_limits_no_active_loan(df_scoring_stabilisation, clean_loans_path):
    risk_multiple_stores = get_multiple_ids_with_limits(df_scoring_stabilisation)
    active_loans = load_active_loans(clean_loans_path)
    
    risk_multiple_stores_current_loans = risk_multiple_stores.merge(active_loans, how='left', on=['store_number'])
    # display(risk_multiple_stores_current_loans.head(2))
    
    risk_multiple_stores_active_loans = risk_multiple_stores_current_loans[risk_multiple_stores_current_loans['principal_disbursed'].notna()]
    # display(risk_multiple_stores_active_loans.head(2))
    
    ## ===== Zeroize multiple stores per national ID and no active loan 
    risk_multiple_stores_not_active_loans = risk_multiple_stores_current_loans[~(risk_multiple_stores_current_loans['national_id'].isin(risk_multiple_stores_active_loans['national_id']))]\
                                        .sort_values(['national_id', 'max_global_limit', 'final_21_limit', 'final_7_limit', 'final_1_limit'], ascending=[True, False, False, False, False])
    # display(risk_multiple_stores_not_active_loans.head(2))
    # print(risk_multiple_stores_not_active_loans.principal_disbursed.unique())
    
    risk_multiple_stores_not_active_loans_best = risk_multiple_stores_not_active_loans.drop_duplicates(subset=['national_id'], keep='first')
    # display(risk_multiple_stores_not_active_loans_best.head(2))
    
    risk_multiple_stores_not_active_loans_zeroize_batch_1 = risk_multiple_stores_not_active_loans[~(risk_multiple_stores_not_active_loans['store_number'].isin(risk_multiple_stores_not_active_loans_best['store_number']))]
    # display(risk_multiple_stores_not_active_loans_zeroize_batch_1.head(2))

    df_scoring_stabilisation.loc[df_scoring_stabilisation['store_number'].isin(risk_multiple_stores_not_active_loans_zeroize_batch_1['store_number']), \
                                 ['final_21_limit', 'final_7_limit', 'final_1_limit']] = 0
    df_scoring_stabilisation.loc[df_scoring_stabilisation['store_number'].isin(risk_multiple_stores_not_active_loans_zeroize_batch_1['store_number']), \
                                 ['multiple_limits']] = "True"
    print(df_scoring_stabilisation.loc[df_scoring_stabilisation['store_number'].isin(risk_multiple_stores_not_active_loans_zeroize_batch_1['store_number']), \
                                 ['final_21_limit', 'final_7_limit', 'final_1_limit']].sum())
    
    ## ===== Zeroize multiple stores per national ID and an active loan
    risk_multiple_stores_active_loans_history = risk_multiple_stores_current_loans[(risk_multiple_stores_current_loans['national_id'].isin(risk_multiple_stores_active_loans['national_id']))]
    # display(risk_multiple_stores_active_loans_history.head(2))

    risk_multiple_stores_not_active_loans_zeroize_batch_2_3 = risk_multiple_stores_active_loans_history[(risk_multiple_stores_active_loans_history['principal_disbursed'].isna())]
    # display(risk_multiple_stores_not_active_loans_zeroize_batch_2_3.head(2))

    df_scoring_stabilisation.loc[df_scoring_stabilisation['store_number'].isin(risk_multiple_stores_not_active_loans_zeroize_batch_2_3['store_number']), \
                                 ['final_21_limit', 'final_7_limit', 'final_1_limit']] = 0
    df_scoring_stabilisation.loc[df_scoring_stabilisation['store_number'].isin(risk_multiple_stores_not_active_loans_zeroize_batch_2_3['store_number']), \
                                 ['multiple_limits']] = "True"
    print(df_scoring_stabilisation.loc[df_scoring_stabilisation['store_number'].isin(risk_multiple_stores_not_active_loans_zeroize_batch_2_3['store_number']), \
                                 ['final_21_limit', 'final_7_limit', 'final_1_limit']].sum())
    
    return df_scoring_stabilisation


def tu_data_limits_scoring_old(df, tu_data_no_limits_path):
    
    # Load TU data and summaries
    tu_data = pd.read_parquet(tu_data_no_limits_path)
    
    # Drop national ID column
    del tu_data['national_id']
    
    tu_data['store_number'] = tu_data['store_number'].astype('str')
    
    # Filter to get those who do not qualify for limits 
    no_limits = df[(df['final_21_limit'] == 0) & (df['final_7_limit'] == 0) & (df['final_1_limit'] == 0)]
    
    # Filter to get those who are iprs_validated
    no_limits = no_limits[(no_limits['is_iprs_validated'] == 'True')]
    
    # Merge to get merchants for whom we have their TU summaries
    tu_data_no_limits = pd.merge(tu_data, no_limits, on='store_number', how='inner')
    
    # Set adjusted loan count to 0
    tu_data_no_limits['adjusted_loan_count'] = 0
    
    # Calculate their limit factor
    tu_data_no_limits['new_limit_factor_7'] = tu_data_no_limits.apply(lambda x: calc_limit_factor_7(x), axis=1)
    
    tu_data_no_limits['risk_rules_factor'] = 1
    
    # Calculate their ultimate factor
    tu_data_no_limits['ultimate_factor_7'] = tu_data_no_limits['risk_rules_factor'] * tu_data_no_limits['idm_factor_7'] * tu_data_no_limits['new_limit_factor_7']
    
    print(tu_data_no_limits[tu_data_no_limits['store_number'] == '908681'][['store_number', 'final_7_limit', 'approx_30_days_trx_val', 'ultimate_factor_7']])
    
    # Calculate their limit
    tu_data_no_limits['final_7_limit'] = tu_data_no_limits['approx_30_days_trx_val'] * tu_data_no_limits['ultimate_factor_7']
    
    print(tu_data_no_limits[tu_data_no_limits['store_number'] == '908681'][['store_number', 'final_7_limit', 'approx_30_days_trx_val', 'ultimate_factor_7']])
    
    # Get merchants who have met the set thresholds
    tu_data_no_limits_above_threshold = tu_data_no_limits[
        (tu_data_no_limits['credit_history_other_sectors'] >= 50) &
        (tu_data_no_limits['total_mobile_accounts_to_date_other_sectors'] >= 30) &
        (tu_data_no_limits['total_open_mobile_accounts_to_date_other_sectors'] <= 3) &
        (tu_data_no_limits['total_mobile_lenders_to_date_other_sectors'] >= 2) &
        (tu_data_no_limits['credit_active']) &
        (tu_data_no_limits['risk_indicator'] == 1)
    ]
    
    # Drop unnecessary columns
    columns_to_drop = [
        'credit_history_other_sectors', 
        'total_mobile_accounts_to_date_other_sectors',
        'total_open_mobile_accounts_to_date_other_sectors',
        'total_mobile_lenders_to_date_other_sectors',
        'credit_active',
        'risk_indicator'
    ]
    tu_data_no_limits_above_threshold = tu_data_no_limits_above_threshold.drop(columns_to_drop, axis=1)
    
    # Modify final_7_limit by capping at 30,000
    tu_data_no_limits_above_threshold['final_7_limit'] = tu_data_no_limits_above_threshold['final_7_limit'].apply(lambda x: 30000 if x >= 30000 else x)
    
    # Find merchants in df but not in tu_data_no_limits_above_threshold
    in_df_not_in_tu = df[~df['store_number'].isin(tu_data_no_limits_above_threshold['store_number'].tolist())]

    cols_order =list(in_df_not_in_tu.columns)
    
    tu_data_no_limits_above_threshold = tu_data_no_limits_above_threshold[cols_order]
    
    tu_data_no_limits_above_threshold['tu_data_scoring'] = 'yes'
    
    in_df_not_in_tu['tu_data_scoring'] = 'no'
    
    # Concatenate the DataFrames
    df = pd.concat([in_df_not_in_tu, tu_data_no_limits_above_threshold])
        
    return df

def tu_data_limits_scoring(df, tu_data_no_limits_path):
    
    # Load TU data and summaries
    tu_data = pd.read_parquet(tu_data_no_limits_path)
    
    # Drop national ID column
    del tu_data['national_id']
    
    tu_data['store_number'] = tu_data['store_number'].astype('str')
    
    # Filter to get those who do not qualify for limits 
    no_limits = df[(df['final_21_limit'] == 0) & (df['final_7_limit'] == 0) & (df['final_1_limit'] == 0)]
    
    # Filter to get merchant in clusters 4,5,8 
    # no_limits_clusters_in_scope = no_limits[no_limits["repayments_by_erd_vs_principal_cluster"].isin([4, 5, 8])]
    
    no_limits_clusters_in_scope = no_limits[no_limits["repayments_by_erd_vs_principal_cluster"].isin([5])]
    
    print(no_limits_clusters_in_scope[(no_limits_clusters_in_scope['hurdle_rate_dpd30'] < 1.00)]['repayments_by_erd_vs_principal_cluster'].value_counts())
    
    # Filter to get those who are iprs_validated
    no_limits_clusters_in_scope = no_limits_clusters_in_scope[(no_limits_clusters_in_scope['is_iprs_validated'] == 'True')]
    
    # Merge to get merchants for whom we have their TU summaries
    tu_data_no_limits = pd.merge(tu_data, no_limits_clusters_in_scope, on='store_number', how='inner')
    
    print(tu_data_no_limits['repayments_by_erd_vs_principal_cluster'].value_counts())
    
    print(tu_data_no_limits[(tu_data_no_limits['hurdle_rate_dpd30'] < 1.00)]['repayments_by_erd_vs_principal_cluster'].value_counts())
    
    # Set adjusted loan count to 0
    tu_data_no_limits['adjusted_loan_count'] = 0
    
    # Calculate their limit factor
    tu_data_no_limits['new_limit_factor_7'] = tu_data_no_limits.apply(lambda x: calc_limit_factor_7(x), axis=1)
    
    # tu_data_no_limits['risk_rules_factor'] = 1
    
    # Calculate their ultimate factor
    tu_data_no_limits['ultimate_factor_7'] = tu_data_no_limits['risk_rules_factor'] * tu_data_no_limits['idm_factor_7'] * tu_data_no_limits['new_limit_factor_7']
    
    # Calculate their limit
    tu_data_no_limits['final_7_limit'] = tu_data_no_limits['approx_30_days_trx_val'] * tu_data_no_limits['ultimate_factor_7']
    
    # Get merchants who have met the set thresholds
    tu_data_no_limits_above_threshold = tu_data_no_limits[
        (tu_data_no_limits['credit_history_other_sectors'] >= 50) &
        (tu_data_no_limits['total_mobile_accounts_to_date_other_sectors'] >= 30) &
        (tu_data_no_limits['total_open_mobile_accounts_to_date_other_sectors'] <= 3) &
        (tu_data_no_limits['total_mobile_lenders_to_date_other_sectors'] >= 2) &
        (tu_data_no_limits['credit_active']) &
        (tu_data_no_limits['risk_indicator'] == 1)
    ]
    
    tu_data_no_limits_above_threshold = tu_data_no_limits_above_threshold[
        (tu_data_no_limits_above_threshold['hurdle_rate_dpd30'] < 1.00)
    ]
    
    # Drop unnecessary columns
    columns_to_drop = [
        'credit_history_other_sectors', 
        'total_mobile_accounts_to_date_other_sectors',
        'total_open_mobile_accounts_to_date_other_sectors',
        'total_mobile_lenders_to_date_other_sectors',
        'credit_active',
        'risk_indicator'
    ]
    tu_data_no_limits_above_threshold = tu_data_no_limits_above_threshold.drop(columns_to_drop, axis=1)
    
    # Modify final_7_limit by capping at 30,000
    tu_data_no_limits_above_threshold['final_7_limit'] = tu_data_no_limits_above_threshold['final_7_limit'].apply(lambda x: 30000 if x >= 30000 else x)
    
    # Find merchants in df but not in tu_data_no_limits_above_threshold
    in_df_not_in_tu = df[~df['store_number'].isin(tu_data_no_limits_above_threshold['store_number'].tolist())]

    cols_order =list(in_df_not_in_tu.columns)
    
    tu_data_no_limits_above_threshold = tu_data_no_limits_above_threshold[cols_order]
    
    tu_data_no_limits_above_threshold['tu_data_scoring'] = 'yes'
    
    in_df_not_in_tu['tu_data_scoring'] = 'no'
    
    # Concatenate the DataFrames
    df = pd.concat([in_df_not_in_tu, tu_data_no_limits_above_threshold])
        
    return df



def tu_data_limits_scoring_old(df, tu_data_no_limits_path):
    
    # Load TU data and summaries
    tu_data = pd.read_parquet(tu_data_no_limits_path)
    
    # Drop national ID column
    del tu_data['national_id']
    
    tu_data['store_number'] = tu_data['store_number'].astype('str')
    
    # Filter to get those who do not qualify for limits 
    never_borrowed_merchants = df[(df['never_borrowed_flag'] == 1)]
    
    # Merge to get merchants for whom we have their TU summaries
    tu_data_never_borrowed_merchants = pd.merge(tu_data, never_borrowed_merchants, on='store_number', how='inner')
    
    # Get merchants who have met the set thresholds
    tu_data_never_borrowed_merchants_above_threshold = tu_data_never_borrowed_merchants[
        (tu_data_never_borrowed_merchants['credit_history_other_sectors'] >= 50) &
        (tu_data_never_borrowed_merchants['total_mobile_accounts_to_date_other_sectors'] >= 30) &
        (tu_data_never_borrowed_merchants['total_open_mobile_accounts_to_date_other_sectors'] <= 3) &
        (tu_data_never_borrowed_merchants['total_mobile_lenders_to_date_other_sectors'] >= 2) &
        (tu_data_never_borrowed_merchants['credit_active']) &
        (tu_data_never_borrowed_merchants['risk_indicator'] == 1)
    ]
    
    # Drop unnecessary columns
    columns_to_drop = [
        'credit_history_other_sectors', 
        'total_mobile_accounts_to_date_other_sectors',
        'total_open_mobile_accounts_to_date_other_sectors',
        'total_mobile_lenders_to_date_other_sectors',
        'credit_active',
        'risk_indicator'
    ]
    
    tu_data_never_borrowed_merchants_above_threshold = tu_data_never_borrowed_merchants_above_threshold.drop(columns_to_drop, axis=1)

    # Find merchants in df but not in tu_data_no_limits_above_threshold
    in_df_not_in_tu = df[~df['store_number'].isin(tu_data_never_borrowed_merchants_above_threshold['store_number'].tolist())]

    cols_order =list(in_df_not_in_tu.columns)
    
    tu_data_never_borrowed_merchants_above_threshold = tu_data_never_borrowed_merchants_above_threshold[cols_order]
    
    tu_data_never_borrowed_merchants_above_threshold['tu_data_scoring'] = 'yes'
    
    in_df_not_in_tu['tu_data_scoring'] = 'no'
    
    # Concatenate the DataFrames
    df = pd.concat([in_df_not_in_tu, tu_data_never_borrowed_merchants_above_threshold])
        
    return df


def calc_final_limit_never_borrowed_below_tu_thresholds(final_limit, tu_data_scoring, never_borrowed_flag):
    return np.where((tu_data_scoring == 'no') & (never_borrowed_flag == 1), 0, final_limit)


# def calc_update_final_21_limit(final_21_limit, final_7_limit, blacklist_flag, ftd_update_flag, rein_update_flag):
#     f21 = np.where((blacklist_flag == 1), final_21_limit,
#           np.where((ftd_update_flag == 1), final_21_limit,
#           np.where((rein_update_flag == 1), final_21_limit,
#           np.where((final_21_limit > 0), final_21_limit,
#           np.where((final_21_limit == 0) & (final_7_limit > 0), final_7_limit,
#                    final_21_limit)))))
    
#     return f21


def calc_update_final_21_limit(final_21_limit, final_7_limit, blacklist_flag, ftd_update_flag, rein_update_flag):
    f21 = np.where((blacklist_flag == 1), final_21_limit,
          np.where((final_21_limit > 0), final_21_limit,
          np.where((final_21_limit == 0) & (final_7_limit > 0), final_7_limit,
                   final_21_limit)))
    
    return f21


def add_model_version_and_create_date(df, model_index, model_start_date, refresh_date, created_at):
    # Tag model version
    df["model_version"] = label_model(model_index, model_start_date, refresh_date)

    # Add created at column
    df["created_at"] = created_at

    # Convert column to timestamp
    df["created_at"] = df["created_at"].apply(pd.to_datetime, errors="coerce")

    return df


def label_model(model_index, model_start_date, refresh_date):
    """
    function to label model version and track model changes i.e.\
    model index/rank e.g 2022_001,
    model_start_date e.g 2022,2,24 reported as year-month-day,
    model_latest_date e.g today() reported as year-month-day,
    this is full is combined to i,e 2022_001[2022-2-24, 2022-3-24]
    
    Inputs:
    Model start date
    Current latest refresh date for the model
    Model index/rank
    
    Outputs:
    model version that dynamically tracks the dates of refresh for a particulay model 
    
    """    
    model_version  = model_index + "[" + model_start_date.strftime("%Y-%m-%d") + "," + " "+ refresh_date +"]"
    
    return model_version


def get_mlflow_run(config):
    # Load configurations
    project_dir = config["project_dir"]
    mlflow_credentials = config["db_credentials"]
    mlflow_config = config["mlflow_config"]
    prefix = mlflow_config["prefix"]
    mlflow_config = config["mlflow_config"]
    remote_server_uri = mlflow_config["remote_server_uri"]
    experiment_name = mlflow_config["experiment_name"]
    run_name = mlflow_config["run_name"]

    # Set up MLflow login credentials
    mlflow_connection(mlflow_credentials, prefix, project_dir)

    # MLflow parameters
    mlflow.set_tracking_uri(remote_server_uri)
    mlflow.set_experiment(experiment_name)
    experiment = mlflow.get_experiment_by_name(experiment_name)
    mr_uri = mlflow.get_registry_uri()
    tracking_uri = mlflow.get_tracking_uri()

    # MLflow logs
    print("Experiment_id: {}".format(experiment.experiment_id))
    print("Artifact Location: {}".format(experiment.artifact_location))
    print("Tags: {}".format(experiment.tags))
    print("Lifecycle_stage: {}".format(experiment.lifecycle_stage))    
    print("Current model registry uri: {}".format(mr_uri))
    print("Current tracking uri: {}".format(tracking_uri))
    # print("URIs assertion: ()".format(assert mr_uri == tracking_uri))

    return mlflow, run_name


def log_model_metrics(config, df, term_frequency, bcsv_clean_data_parquet, metabase_amount_clean_data_path, config_path):
    # Mlflow instance
    mlflow, run_name = get_mlflow_run(config)

    # Load data frame with customer details
    customer_details = pd.read_parquet(bcsv_clean_data_parquet)
    df_amount = pd.read_parquet(metabase_amount_clean_data_path)

    # Final limits logs
    mv = df['model_version'].unique()
    df_idm_rec_val_cnts = df['idm_recommendation'].value_counts()
    df_store_cnt = df["store_number"].nunique()
    vmla = df["max_global_limit"].sum()
    nmla = df.loc[(df['max_global_limit'] > 0), "store_number"].nunique()
    atv3m = df["approx_30_days_trx_val"].sum()
    # nl = df.loc[(df["previous_max_global_limit"] == 0) & (df["max_global_limit"] > 0) & (df["loan_count"] == 0), "store_number"].nunique()
    nl = df.loc[(df['total_max_global_limit'] == 0) & (df["max_global_limit"] > 0), "store_number"].nunique()
    nlv = df.loc[(df['total_max_global_limit'] == 0) & (df["max_global_limit"] > 0), "max_global_limit"].sum()
    # nlv = df.loc[(df["previous_max_global_limit"] == 0) & (df["max_global_limit"] > 0) & (df["loan_count"] == 0), "max_global_limit"].sum()
    # lic = df.loc[(df['max_limit_diff'] > 0) & (df["loan_count"] > 0), "store_number"].nunique()
    # liv = df.loc[(df['max_limit_diff'] > 0) & (df["loan_count"] > 0), 'max_global_limit'].sum()
    lic = df.loc[(df['total_max_global_limit'] > 0) & (df["max_limit_diff"] > 0), "store_number"].nunique()
    liv = df.loc[(df['total_max_global_limit'] > 0) & (df["max_limit_diff"] > 0), "max_limit_diff"].sum()
    # licc = df.loc[df['max_limit_diff'] < 0, "store_number"].nunique()
    # livc = df.loc[df['max_limit_diff'] < 0, "max_global_limit"].sum()
    licc = df.loc[(df['total_max_global_limit'] > 0) & (df["max_limit_diff"] < 0), "store_number"].nunique()
    livc = df.loc[(df['total_max_global_limit'] > 0) & (df["max_limit_diff"] < 0), "max_limit_diff"].sum()
    aglrr = round(df.loc[df['max_global_limit'] > 0, "good_loans_repayment_ratio"].mean(), 4)
    attc = round(df.loc[df['max_global_limit'] > 0, "page_active_days"].mean(), 4)
    cidma = df.loc[df['idm_recommendation'] == 'Approve', 'store_number'].nunique()
    cidmr = df.loc[df['idm_recommendation'] == 'Reject', 'store_number'].nunique()
    ckyci = df.loc[df['is_iprs_validated'] == 'False', 'store_number'].nunique()
    tnbc = customer_details['store_number'].nunique()
    anbc = df_amount['store_number'].nunique()
    ia_dd = round(df.loc[df['max_global_limit'] > 0, 'consolidated_repayments_by_dd'].mean(), 4)
    ia_red = round(df.loc[df['max_global_limit'] > 0, 'consolidated_repayments_by_erd'].mean(), 4)
    ia_dpd30 = round(df.loc[df['max_global_limit'] > 0, 'consolidated_repayments_by_dpd30'].mean(), 4)

    
    print(f"\nModel version: {mv}")
    print("\nIDM recommendation distribution:")
    print(df_idm_rec_val_cnts)
    print(f"\nNum of customers who've been scored: {df_store_cnt}")
    print(f"Value of maximum limits assigned: {vmla}")
    print(f"Number of stores with a maximum limit assigned: {nmla}")
    print(f"Average value of transactions for last 3 months: {atv3m}")
    print("\n")
    print(f"Number of stores with a new limit (on max limits): {nl}")
    print(f"Value of new limits: {nlv}")
    print("\n")
    print(f"Number of stores with limit increase (on max limits): {lic}")
    print(f"Value of limit increases: {liv}")
    print("\n")
    print(f"Number of stores with limit decrease (on max limits): {licc}")
    print(f"Value of limit decreases: {livc}")
    print("\n")
    print(f"Average of good loan repayment ratio: {aglrr}")
    print(f"Average of till transaction consistency: {attc}")
    print("\n")
    print(f"Count of IDM approve: {cidma}")
    print(f"Count of IDM decline: {cidmr}")
    print("\n")
    print(f"KYC incomplete: {ckyci}")
    print("\n")
    print(f"Total number of bloom clients: {tnbc}")
    print(f"Clients with active transactions last three months: {anbc}")
    print("\n")
    print(f"Expected repayment by due date: {ia_dd}")
    print(f"Expected repayment by rollover end date: {ia_red}")
    print(f"Expected repayment by dpd30: {ia_dpd30}")
    print("\n")
    
    # MLflow experiment run
    with mlflow.start_run(run_name=run_name) as mlops_run:
        # tracking_url_type_store = urlparse(mlflow.get_artifact_uri()).scheme
        mlflow.log_metric(f"Num of customers who have been scored", df_store_cnt)
        mlflow.log_metric(f"Value of maximum limits assigned", vmla)
        mlflow.log_metric(f"Number of stores with a maximum limit assigned", nmla)
        mlflow.log_metric(f"Average value of transactions for last 3 months", atv3m)
        mlflow.log_metric(f"Number of stores with a new limit on max limits", nl)
        mlflow.log_metric(f"Value of new limits", nlv)
        mlflow.log_metric(f"Number of stores with limit increase on max limits", lic)
        mlflow.log_metric(f"Value of limit increases", liv)
        mlflow.log_metric(f"Number of stores with limit decrease on max limits", licc)
        mlflow.log_metric(f"Value of limit decreases", livc)
        mlflow.log_metric(f"Average of good loan repayment ratio", aglrr)
        mlflow.log_metric(f"Average of till transaction consistency", attc)
        mlflow.log_metric(f"Count of IDM approve", cidma)
        mlflow.log_metric(f"Count of IDM decline", cidmr)
        mlflow.log_metric(f"KYC incomplete", ckyci)
        mlflow.log_metric(f"Total number of bloom clients", tnbc)
        mlflow.log_metric(f"Clients with active transactions last three months", anbc)
        mlflow.log_metric(f"Expected repayment by due date", ia_dd)
        mlflow.log_metric(f"Expected repayment by rollover end date", ia_red)
        mlflow.log_metric(f"Expected repayment by dpd30", ia_dpd30)
        mlflow.set_tag(f'Model version', mv)
        mlflow.log_artifact(config_path)

        for tf in term_frequency:
            gla = df[f"final_{tf}_limit"].sum()
            gsa = df.loc[df[f"final_{tf}_limit"] > 0, "store_number"].nunique()
            elablf = df.loc[df["blacklist_flag"] == 0, f"final_{tf}_limit"].sum()
            esa = df.loc[(df[f"final_{tf}_limit"] > 0 ) & (df["blacklist_flag"] == 0), "store_number"].nunique()
            
            print(f"Gross limit allocation for {tf} day: {gla}")
            print(f"Gross num of store nums allocated {tf} day limit: {gsa}")
            print(f"Effective limit allocation for {tf} day with blacklist check: {elablf}")
            print(f"Effective num of store nums allocated {tf} day limit: {esa}\n")

            mlflow.log_metric(f'Gross limit allocation for {tf} day', gla)
            mlflow.log_metric(f'Gross num of store nums allocated {tf} day limit', gsa)
            mlflow.log_metric(f'Effective limit allocation for {tf} day with blacklist check', elablf)
            mlflow.log_metric(f'Effective num of store nums allocated {tf} day limit', esa)
        

def limit_stabilisation(config_path, refresh_date):
    # Load configurations
    config = read_params(config_path)
    project_dir = config["project_dir"]
    snaphot_period = config["snaphot_period_config"]
    trxn_data_period = config["trxn_data_period_config"]
    scored_limits_data_path = config["interim_data_config"]["scored_limits_data_parquet"]
    blacklist_clean_data_path = config["processed_data_config"]["blacklist_clean_data_parquet"]
    bcsv_clean_data_path = config["processed_data_config"]["bcsv_clean_data_parquet"]
    excluded_first_time_data_path = config["processed_data_config"]["excluded_first_time_data_parquet"]
    before_21d_graduation_limits_data_path = config["interim_data_config"]["before_21d_graduation_limits_data_parquet"]
    after_21d_graduation_limits_data_path = config["interim_data_config"]["after_21d_graduation_limits_data_parquet"]
    metabase_amount_clean_data_path = config["processed_data_config"][f"metabase_amount_clean_data_parquet"]
    term_frequencies = config["crb_limit_factor_config"]["term_frequencies"]
    cluster_scaling_pipeline_path = config["cluster_model_config"]["cluster_scaling_pipeline_path"]
    cluster_dim_reduction_pipeline_path = config["cluster_model_config"]["cluster_dim_reduction_pipeline_path"]
    cluster_model_1_path = config["cluster_model_config"]["cluster_model_1_path"]
    cluster_model_2_path = config["cluster_model_config"]["cluster_model_2_path"]
    clf_scaling_pipeline_path = config["clf_model_config"]["clf_scaling_pipeline_path"]
    # clf_dim_reduction_pipeline_path = config["clf_model_config"]["clf_dim_reduction_pipeline_path"]
    clf_model_path = config["clf_model_config"]["clf_model_path"]
    cluster_hurdle_rates_data_path = config["cluster_model_config"]["cluster_hurdle_rates_data_parquet"]
    never_borrowed_features_in_scope = config["cluster_model_config"]["never_borrowed_features_in_scope"]
    reg_clusters_in_scope = config["reg_model_config"]["reg_clusters_in_scope"]
    model_index = config["model_label_config"]["model_index"]
    model_start_date_y = config["model_label_config"]["model_start_date_y"]
    model_start_date_m = config["model_label_config"]["model_start_date_m"]
    model_start_date_d = config["model_label_config"]["model_start_date_d"]
    max_dpd_30_data_path = config["interim_data_config"]["max_dpd_30_data_parquet"]
    tu_data_no_limits_path = config["interim_data_config"]["tu_data_no_limits_parquet"]
    scored_limits_risk_review_data_path_parquet = config["processed_data_config"]["scored_limits_risk_review_data_parquet"]
    scored_limits_risk_review_data_path_excel = config["processed_data_config"]["scored_limits_risk_review_data_excel"]
    scored_limits_risk_review_data_path_excel_parquet = config["processed_data_config"]["scored_limits_risk_review_data_excel_parquet"]
    after_rmdd_ge95_data_path = config["interim_data_config"]["after_rmdd_ge95_data_parquet"]
    after_rmdd_ge96_data_path = config["interim_data_config"]["after_rmdd_ge96_data_parquet"]
    ftd_clean_data_path = config["processed_data_config"]["ftd_clean_data_parquet"]
    rein_clean_data_path = config["processed_data_config"]["rein_clean_data_parquet"]
    lftsv_clean_data_path = config["processed_data_config"]["lftsv_clean_data_parquet"]
    risk_features_in_scope = config["upload_data_config"]["risk_features_in_scope"]
    
    # Parameters
    print(model_start_date_y, model_start_date_m, model_start_date_d)
    model_start_date = dt.datetime(int(model_start_date_y), int(model_start_date_m), int(model_start_date_d))

    # Load snapshot
    df = pd.read_parquet(project_dir + scored_limits_data_path)
    print('\nFirst assignment:\n', df[['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('First assignment sample:\n', df.loc[df['store_number'] == '7649581', ['store_number', 'final_21_limit', 'final_7_limit', 'final_1_limit']], sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')
   
    # Zeroize limits of defaulters
    df = limit_zeroization_defaulters(df, project_dir + blacklist_clean_data_path, term_frequencies)
    print('\nZeroize defaulters:\n', df[['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('Zeroize defaulters sample:\n', df.loc[df['store_number'] == '7649581', ['store_number', 'final_21_limit', 'final_7_limit', 'final_1_limit']], sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')
    
    # Exempt totally new customers who have not taken any loan but have failed our rules
    excluded_first_time_df = df[(df["loan_count"] == 0) & (df["final_7_limit"] == 0) & (df["final_1_limit"] == 0)]
    df = df[~((df["loan_count"] == 0) & (df["final_7_limit"] == 0) & (df["final_1_limit"] == 0))]
    print('\nExempt new customers:\n', df[['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('Exempt new customers sample:\n', df.loc[df['store_number'] == '7649581', ['store_number', 'final_21_limit', 'final_7_limit', 'final_1_limit']], sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')
    
    # Blacklist flag feature
    df['blacklist_flag'] = calc_blacklist_flag(df["days_past_due"], df["bloom_version"], df["loan_status"])
    print('\nBlacklist flag accuracy check:', df.loc[(df['days_past_due'] >= 90), 'blacklist_flag'].unique(), sep=' ')
    print('---------------------------------------------------------------------------------------------------------------------------------------')

    # Limit factor feature
    for tf in term_frequencies:
        df[f"limit_factor_{tf}"] = declare_limit_factor(config, df["idm_recommendation"], tf)

    # # Adjusted final 21 limit feature
    # df['final_21_limit'] = calc_final_21_limit(df['total_final_21_limit'], df['final_21_limit'])
    # print('\nAdjust final 21:\n', df[['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    # print('Adjust final 21 sample:\n', df.loc[df['store_number'] == '7629083', ['store_number', 'total_final_21_limit', 'final_21_limit', 'final_7_limit', 'final_1_limit']], sep='')
    # print('Accuracy check:\n', df.loc[df['total_final_21_limit'] == 0, ['total_final_21_limit', 'final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    # print('---------------------------------------------------------------------------------------------------------------------------------------')
    
    # # No new limits assignment
    # for tf in term_frequencies:
    #     df[f'final_{tf}_limit'] = no_new_limits_assignement(df[f'total_final_{tf}_limit'], df[f'final_{tf}_limit'], df['rllvr_date_rm_ge_rm_add_back'])
    # print('\nNo new limits assignement:\n', df[['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    # print('No new limits assignment check\n', df.loc[df['store_number'] == '719557', ['store_number', 'final_21_limit', 'final_7_limit', 'final_1_limit']], sep='')
    # print('---------------------------------------------------------------------------------------------------------------------------------------')
    
    # # Limit cap feature
    # for tf in term_frequencies:
    #     df[f'limit_{tf}_cap'] = df[f'previous_{tf}_limit'] * limit_cap_config

    # # Adjusted final limit feature
    # for tf in term_frequencies:
    #     df[f'final_{tf}_limit'] = calc_final_limit_from_limit_cap(df[f'limit_{tf}_cap'], df[f'final_{tf}_limit'], df[f'previous_{tf}_limit'])
    
    # # Adjusted final limit feature
    # for tf in term_frequencies:
    #     df[f'final_{tf}_limit'] = calc_final_limit_from_limit_cap(df[f'final_{tf}_limit'], df[f'previous_{tf}_limit'], df['lrr_update_flag'], df['reinstatement_reason'], df['previous_is_iprs_validated'], df['is_iprs_validated'], df['due_date_rm_ge_rm_add_back'], df['rllvr_date_102_check'])
    #             # df[f'final_{tf}_limit'] = calc_final_limit_from_limit_cap(df[f'final_{tf}_limit'], df[f'previous_{tf}_limit'], df['update_flag'], df['reinstatement_reason'], df['previous_is_iprs_validated'], df['is_iprs_validated'], df['rllvr_date_rm_ge_rm_limit_increase'])
    # print('\nLimit cap:\n', df[['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    # print('Limit cap sample:\n', df.loc[df['store_number'] == '7629083', ['store_number', 'final_21_limit', 'final_7_limit', 'final_1_limit']], sep='')
    # print('Limit cap 21:', df[df['previous_21_limit'] > df['final_21_limit']].shape, sep=' ')
    # print('Limit cap 7:', df[df['previous_7_limit'] > df['final_7_limit']].shape, sep=' ')
    # print('Limit cap 1:', df[df['previous_1_limit'] > df['final_1_limit']].shape, sep=' ')
    # print(type(df['previous_is_iprs_validated']))
    # print(type(df['is_iprs_validated']))
    # # print(df[(df['previous_is_iprs_validated'] == 'False') & (df['is_iprs_validated'] == True)]).shape
    # print(df[(df['previous_is_iprs_validated'] == 'False') & (df['is_iprs_validated'] == 'True')].shape)
    # # print(df[(df['previous_is_iprs_validated'] == False) & (df['is_iprs_validated'] == True)]).shape
    # # print(df[(df['previous_is_iprs_validated'] == False) & (df['is_iprs_validated'] == 'True')]).shape
    # print('---------------------------------------------------------------------------------------------------------------------------------------')
    
    # Adjusted final limit feature
    for tf in term_frequencies:
        df[f'final_{tf}_limit'] = calc_final_limit_from_limit_cap(df[f'final_{tf}_limit'], df[f'previous_{tf}_limit'], df[f'latest_{tf}_loan'], df['lrr_update_flag'], df['reinstatement_reason'], df['previous_is_iprs_validated'], df['is_iprs_validated'], df['due_date_rm_ge_rm_add_back'], df['rllvr_date_102_check'], df['loan_count'], df[f'final_{tf}_limit_non_zero'])
    print('\nLimit cap:\n', df[['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('Limit cap sample:\n', df.loc[df['store_number'] == '7649581', ['store_number', 'final_21_limit', 'final_7_limit', 'final_1_limit']], sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')
    
    # Adjusted final limit feature
    for tf in term_frequencies:
        df[f'final_{tf}_limit'] = calc_final_limit_from_limit_cap_last_loan(df[f'final_{tf}_limit'], df[f'previous_{tf}_limit'], df[f'latest_{tf}_loan'], df['loan_count'], df[f'final_{tf}_limit_non_zero'])
    print('\nLimit cap loan increase:\n', df[['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('Limit cap sample:\n', df.loc[df['store_number'] == '7649581', ['store_number', 'final_21_limit', 'final_7_limit', 'final_1_limit']], sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')
    

    # Limt increase of more than fifty percent from last three months
    for tf in term_frequencies:
        df[f'final_{tf}_limit'] = calc_final_limit_from_snapshot_cap(df[f'final_{tf}_limit'], df[f'previous_3m_{tf}_limit'])
        # df[f'final_{tf}_limit'] = calc_final_limit_from_snapshot_cap(df[f'final_{tf}_limit'], df[f'snapshot_3m_{tf}_loan'])
    print('\Snapshot cap:\n', df[['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('Snapshot cap sample:\n', df.loc[df['store_number'] == '7649581', ['store_number', 'final_21_limit', 'final_7_limit', 'final_1_limit']], sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')

    # # No new limits assignment
    # for tf in term_frequencies:
    #     df[f'final_{tf}_limit'] = no_new_limits_assignement(df[f'total_final_{tf}_limit'], df[f'final_{tf}_limit'], df['rllvr_date_102_check'])
    # print('\nNo new limits assignement:\n', df[['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    # print('No new limits assignment check\n', df.loc[df['store_number'] == '7629083', ['store_number', 'final_21_limit', 'final_7_limit', 'final_1_limit']], sep='')
    # print('---------------------------------------------------------------------------------------------------------------------------------------')
    
    # Adjusted final limit feature
    print('\nBefore limits for dpd30:\n', df.loc[df['days_past_due'] > 30, ['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    for tf in term_frequencies:
        df[f'final_{tf}_limit'] = calc_final_limit_from_dpd30(df["days_past_due"], df[f"final_{tf}_limit"])
    print('DPD30 check:\n', df[['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('DPD30 check sample:\n', df.loc[df['store_number'] == '7649581', ['store_number', 'final_21_limit', 'final_7_limit', 'final_1_limit']], sep='')
    print('After limits for dpd30:\n', df.loc[df['days_past_due'] > 30, ['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')
    
    # Adjusted final limit feature
    for tf in term_frequencies:
        df[f"final_{tf}_limit"] = (np.ceil(df[f"final_{tf}_limit"] / 100) * 100).astype(int)
    print('\nCeiling:\n', df[['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('Ceiling sample:\n', df.loc[df['store_number'] == '7649581', ['store_number', 'final_21_limit', 'final_7_limit', 'final_1_limit']], sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')
    
    # Adjusted final limit feature
    for tf in term_frequencies:
        df[f'final_{tf}_limit'] = calc_new_final_limit_by_product_cap(df[f'final_{tf}_limit'], tf)
    print('\nProduct cap:\n', df[['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('Product cap sample:\n', df.loc[df['store_number'] == '7649581', ['store_number', 'final_21_limit', 'final_7_limit', 'final_1_limit']], sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')
    
    # df['mobile_number'] = df['mobile_number'].astype(str)
    
    # Adjusted final limits based on IPRS flags
    print('\nIPRS flag distribution:\n', df["is_iprs_validated"].value_counts())
    for tf in term_frequencies:
        df[f'final_{tf}_limit'] = calc_final_limit_from_kyc_flag(df["is_iprs_validated"], df['mobile_number'], df[f"final_{tf}_limit"])
    print('\nIPRS validated:\n', df[['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('IPRS validated sample:\n', df.loc[df['store_number'] == '7649581', ['store_number', 'final_21_limit', 'final_7_limit', 'final_1_limit', 'is_iprs_validated', 'mobile_number']], sep='')
    print('Accuracy check for iprs:\n', df.loc[df['is_iprs_validated'] == 'False', ['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('Accuracy check for mobile number:\n', df.loc[df['mobile_number'] == 'None', ['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')
    
    # Adjusted final limit feature
    for tf in term_frequencies:
        df[f'final_{tf}_limit'] = calc_final_limit_from_dpd15(df["days_past_due"], df[f"final_{tf}_limit"])
    print('\nDPD15:\n', df[['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('DPD15 sample:\n', df.loc[df['store_number'] == '7649581', ['store_number', 'final_21_limit', 'final_7_limit', 'final_1_limit']], sep='')
    print('Accuracy check for dpd15:\n', df.loc[df['days_past_due'] > 15, ['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')
    
    # Adjusted final limit feature
    for tf in term_frequencies:
        df[f'final_{tf}_limit'] = calc_final_limit_from_last_disb90(df["num_days_since_last_disbursement"], df['rllvr_date_rm_ge_rm_add_back'], df[f"final_{tf}_limit"])
    print('\n90+ Days Since Disbursement:\n', df[['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('90+ Days Since Disbursement sample:\n', df.loc[df['store_number'] == '7649581', ['store_number', 'num_days_since_last_disbursement', 'final_21_limit', 'final_7_limit', 'final_1_limit']], sep='')
    print('Accuracy check for last loan 90:\n', df.loc[(df['num_days_since_last_disbursement'] > 90) & (df['rllvr_date_rm_ge_rm_add_back'] == 0), ['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')

    # 21 day limit feature
    df["21_day_limit"] = approximate_21_day_limit(config, df["avg_7_day_principal_disbursed"])
    
    # Adjusted final 21 limit feature after bloom 21 day graduation
    df = bloom_21d_graduation_new_limits(df, project_dir + max_dpd_30_data_path)
    print('\n21 day graduation:\n', df[['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('21 day graduation new limits sample:\n', df.loc[df['store_number'] == '7649581', ['store_number', 'final_21_limit', 'final_7_limit', 'final_1_limit']], sep='')
    print('Dataframe shape:\n', df.shape)
    print('Dataframe store_number count:\n', df['store_number'].nunique())
    print('---------------------------------------------------------------------------------------------------------------------------------------')
    
    # Adjusted final 21 limit feature after bloom 21 day graduation
    df['final_21_limit'] = calc_new_21_day_limits_assignment(df['previous_21_limit'], df['previous_7_limit'], df['final_21_limit'], df['l21_day_graduation_flag_new_limits'])
    print('\n21 day graduation:\n', df[['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('21 day graduation new limits sample:\n', df.loc[df['store_number'] == '7649581', ['store_number', 'final_21_limit', 'final_7_limit', 'final_1_limit']], sep='')
    print('Accuracy check for allocated 21 day limits before value:\n', df.loc[(df['total_final_21_limit'] > 0), ['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('Accuracy check for allocated 21 day limits before count:\n', df.loc[(df['total_final_21_limit'] > 0), ['store_number']].nunique(), sep='')
    print('Accuracy check for 21 day graduation pass value:\n', df.loc[(df['total_final_21_limit'] == 0) & (df['l21_day_graduation_flag_new_limits'] == 'pass'), ['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('Accuracy check for 21 day graduation pass count:\n', df.loc[(df['total_final_21_limit'] == 0) & (df['l21_day_graduation_flag_new_limits'] == 'pass'), ['store_number']].nunique(), sep='')
    print('Accuracy check for 21 day graduation fail value:\n', df.loc[(df['total_final_21_limit'] == 0) & (df['l21_day_graduation_flag_new_limits'] == 'fail'), ['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('Accuracy check for 21 day graduation fail value:\n', df.loc[(df['total_final_21_limit'] == 0) & (df['l21_day_graduation_flag_new_limits'] == 'fail'), ['store_number']].nunique(), sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')
    
    # Export snapshot
    df.to_parquet(project_dir + before_21d_graduation_limits_data_path, index=False)
    
    # Adjusted final 21 limit feature after bloom 21 day graduation
    df = bloom_21d_graduation(df, project_dir + max_dpd_30_data_path)
    print('\n21 day graduation:\n', df[['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('21 day graduation sample:\n', df.loc[df['store_number'] == '7649581', ['store_number', 'final_21_limit', 'final_7_limit', 'final_1_limit']], sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')
    
    # Export snapshot
    df.to_parquet(project_dir + after_21d_graduation_limits_data_path, index=False)

    # Adjusted final 21 limit for zeroized after 21d graduation but historical repayments is greater than 95
    df = restore_21d_limits_zeroized_but_rmdd_ge95(df, project_dir + before_21d_graduation_limits_data_path)
    print('\nRM95dd:\n', df[['bf_final_21_limit', 'af_final_21_limit', 'af95_final_21_limit', 'final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('RM95dd sample:\n', df.loc[df['store_number'] == '7649581', ['store_number', 'final_21_limit', 'final_7_limit', 'final_1_limit', 'due_date_rm_ge_rm_add_back', 'repayments_by_dd_vs_principal_mean', 'rllvr_date_rm_ge_rm_add_back', 'repayments_by_erd_vs_principal_mean']], sep='')
    # print('Accuracy check for 21 day graduation pass value:\n', df.loc[(df['total_final_21_limit'] == 0) & (df['l21_day_graduation_flag_new_limits'] == 'pass'), ['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    # print('Accuracy check for 21 day graduation pass count:\n', df.loc[(df['total_final_21_limit'] == 0) & (df['l21_day_graduation_flag_new_limits'] == 'pass'), ['store_number']].nunique(), sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')

    # Export snapshot
    df.to_parquet(project_dir + after_rmdd_ge95_data_path, index=False)

    # Adjusted final 7 and 1 day limit for merchants with a Safaricom score between 0 and 477
    for tf in term_frequencies[1:]:
        df[f'updated_final_{tf}_limit'] = calc_final_limit_saf_credit_score(df['src_crdt_score'], df[f'final_{tf}_limit'])
        df[f'final_{tf}_limit'] = calc_final_limit_saf_credit_score(df['src_crdt_score'], df[f'final_{tf}_limit'])
    print('\nSafaricom score:\n', df[['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('Safaricom score sample:\n', df.loc[df['store_number'] == '7649581', ['store_number', 'final_21_limit', 'final_7_limit', 'final_1_limit']], sep='')
    print('Accuracy check for Saf Score:\n', df.loc[(df['src_crdt_score'] >= 0) & (df['src_crdt_score'] <= 477), ['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')
    
    # Add back 7 day and 1 day limits for merchants with repayment milestone above threshold
    df = restore_1d_7d_limits_rm_hurdle_rate(df, project_dir + before_21d_graduation_limits_data_path, term_frequencies[1:])
    print('\nRepayment milestone hurdle rate:\n', df[['final_21_limit', 'final_7_limit', 'final_1_limit', 'updated_7_limit', 'updated_1_limit', 'new_final_7_limit_1', 'new_final_1_limit_1']].sum(), sep='')
    print('Repayment milestone hurdle rate sample:\n', df.loc[df['store_number'] == '7649581', ['store_number', 'final_21_limit', 'final_7_limit', 'final_1_limit']], sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')

    # Export snapshot
    df.to_parquet(project_dir + after_rmdd_ge96_data_path, index=False)
    
    # Adjusted new 7 and 1 day limit for merchants with only 21 day limits
    print('\nNo 7 day limits:\n', df.loc[(df['final_21_limit'] > 0) & (df['final_7_limit'] == 0), ['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('No 1 day limits:\n', df.loc[(df['final_21_limit'] > 0) & (df['final_1_limit'] == 0), ['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    for tf in term_frequencies[1:]:
        df[f'updated_new_{tf}_limit'] = updated_new_limit(df[f'updated_{tf}_limit'], df['new_final_21_limit_1'], df[f'old_final_{tf}_limit'])
        df[f'final_{tf}_limit'] = updated_new_limit(df[f'updated_{tf}_limit'], df['new_final_21_limit_1'], df[f'old_final_{tf}_limit'])
    print('Add back 1d and 7d:\n', df[['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('Add back 1d and 7d sample:\n', df.loc[df['store_number'] == '7649581', ['store_number', 'final_21_limit', 'final_7_limit', 'final_1_limit']], sep='')
    print('No 7 day limits:\n', df.loc[(df['final_21_limit'] > 0) & (df['final_7_limit'] == 0), ['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('No 1 day limits:\n', df.loc[(df['final_21_limit'] > 0) & (df['final_1_limit'] == 0), ['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('Has 21 no 7 and 1 day limits:\n', df.loc[(df['final_21_limit'] > 0) & (df['final_7_limit'] == 0) & (df['final_1_limit'] == 0), ['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')
    
    # Adjusted final 1 limit feature
    for tf in term_frequencies[2:]:
        df[f'final_{tf}_limit'] = calc_final_limit_for_1_day(df["due_date_rm_ge_rm_1d"], df[f"final_{tf}_limit"])
        df[f'zeroize_final_{tf}_limit'] = calc_final_limit_for_1_day(df["due_date_rm_ge_rm_1d"], df[f"final_{tf}_limit"])
    print('\nZeroizing 1 day limits:\n', df[['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('Accuracy check for zeroizing 1 day limits:\n', df.loc[df['due_date_rm_ge_rm_1d'] != 1, ['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')

    # Add back limits of first time defaulters during election time
    df = reinstate_first_time_defaulters_limits(df, project_dir + ftd_clean_data_path, term_frequencies) # TODO
    print('\nFirst time defaults reinstatement:\n', df[['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('First time defaults reinstatement sample:\n', df.loc[df['store_number'] == '7649581', ['store_number', 'final_21_limit', 'final_7_limit', 'final_1_limit', "model_630_21_limit", "model_630_7_limit", "model_630_1_limit", "model_630_max_global_limit", "ftd_update_flag", "blacklist_flag"]], sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')
    
    # Add back limits of first time defaulters during election time
    df = reinstate_defaulters_limits(df, project_dir + rein_clean_data_path) # TODO
    print('\nDefaults reinstatement:\n', df[['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('Defaults reinstatement sample:\n', df.loc[df['store_number'] == '7649581', ['store_number', 'final_21_limit', 'final_7_limit', 'final_1_limit', "rein_update_flag", "blacklist_flag"]], sep='')
    print('Sum of Defaults reinstatement:\n', df.loc[(df['rein_update_flag'] == 1), ['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('Count of Defaults reinstatement:\n', df.loc[(df['rein_update_flag'] == 1), ['final_21_limit', 'final_7_limit', 'final_1_limit']].count(), sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')

    # Zeroize limits for never borrowed clients below threshold
    # df = never_borrowed_hurdle_rates(df, project_dir + cluster_hurdle_rates_data_path, project_dir + cluster_model_1_path, project_dir + cluster_model_2_path, project_dir + cluster_scaling_pipeline_path, project_dir + cluster_dim_reduction_pipeline_path, never_borrowed_features_in_scope)
    df = never_borrowed_hurdle_rates(config, df)
    print('\nZeroize never borrowed add flags:\n', df[['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('Sum of never borrowed limits before:\n', df.loc[(df['never_borrowed_flag'] == 1), ['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')

    for tf in term_frequencies:
        # df[f'final_{tf}_limit'] = calc_final_limit_never_borrowed_below_rm_hurdle_rate(df[f'final_{tf}_limit'], df['rllvr_date_rm_ge_rm_never_borrowed']) # ERD
        df[f'final_{tf}_limit'] = calc_final_limit_never_borrowed_below_rm_hurdle_rate(df[f'final_{tf}_limit'], df['dpd30_rm_ge_rm_never_borrowed']) # DPD30
    print('\nZeroize never borrowed:\n', df[['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('Zeroize never borrowed sample:\n', df.loc[df['store_number'] == '7649581', ['store_number', 'final_21_limit', 'final_7_limit', 'final_1_limit']], sep='')
    print('Sum of never borrowed limits after:\n', df.loc[(df['never_borrowed_flag'] == 1), ['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')

    df_reg_clusters_in_scope = df[(df['repayments_by_erd_vs_principal_cluster'].isin(reg_clusters_in_scope)) & (df['dpd30_rm_ge_rm_never_borrowed'] == 1) & (df['final_7_limit'] > 0)]
    print('\nRegression clusters in scope distribution:\n', df_reg_clusters_in_scope.groupby(['repayments_by_erd_vs_principal_cluster']).agg({'store_number': 'count', 'final_21_limit': 'sum', 'final_7_limit': 'sum', 'final_1_limit': 'sum'}), sep='')
    print('Regression clusters in scope overall:\n', df_reg_clusters_in_scope[['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')
    
    # Adjusted final 7 limit cap
    df = tu_data_limits_scoring(df, project_dir + tu_data_no_limits_path)
    print('\nTU data limits scoring:\n', df[['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('TU data limits scoring:\n', df.loc[df['store_number'] == '7649581', ['store_number', 'approx_30_days_trx_val', 'ultimate_factor_7', 'final_21_limit', 'final_7_limit', 'final_1_limit']], sep='')
    print(df[df['tu_data_scoring'] == 'yes'].shape)
    print('---------------------------------------------------------------------------------------------------------------------------------------')
    
    # Adjusted final limit feature
    for tf in term_frequencies:
        df[f'final_{tf}_limit'] = calc_final_limit_from_limit_cap(df[f'final_{tf}_limit'], df[f'previous_{tf}_limit'], df[f'latest_{tf}_loan'], df['lrr_update_flag'], df['reinstatement_reason'], df['previous_is_iprs_validated'], df['is_iprs_validated'], df['due_date_rm_ge_rm_add_back'], df['rllvr_date_102_check'], df['loan_count'], df[f'final_{tf}_limit_non_zero'])
    print('\nLimit cap:\n', df[['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('Limit cap sample:\n', df.loc[df['store_number'] == '7649581', ['store_number', 'final_21_limit', 'final_7_limit', 'final_1_limit']], sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')
    
    # Adjusted final limit feature
    for tf in term_frequencies:
        df[f'final_{tf}_limit'] = calc_final_limit_from_limit_cap_last_loan(df[f'final_{tf}_limit'], df[f'previous_{tf}_limit'], df[f'latest_{tf}_loan'], df['loan_count'], df[f'final_{tf}_limit_non_zero'])
    print('\nLimit cap loan increase:\n', df[['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('Limit cap sample:\n', df.loc[df['store_number'] == '7649581', ['store_number', 'final_21_limit', 'final_7_limit', 'final_1_limit']], sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')
    
    # Limt increase of more than fifty percent from last three months
    for tf in term_frequencies:
        df[f'final_{tf}_limit'] = calc_final_limit_from_snapshot_cap(df[f'final_{tf}_limit'], df[f'previous_3m_{tf}_limit'])
        # df[f'final_{tf}_limit'] = calc_final_limit_from_snapshot_cap(df[f'final_{tf}_limit'], df[f'snapshot_3m_{tf}_loan'])
    print('\Snapshot cap:\n', df[['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('Snapshot cap sample:\n', df.loc[df['store_number'] == '7649581', ['store_number', 'final_21_limit', 'final_7_limit', 'final_1_limit']], sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')

    # Zeroize multiple stores per national ID and no active loan/an active loan
    df['multiple_limits'] = "False"
    df = zeroize_with_limits_no_active_loan(df, project_dir + lftsv_clean_data_path)
    print('\nZeroize multiple stores per national ID and no active loan/an active loan:\n', df[['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), '\n', sep='')
    print('Zeroize multiple stores per national ID and no active loan sample/an active loan:\n', df.loc[df['store_number'] == '7649581', ['store_number', 'final_21_limit', 'final_7_limit', 'final_1_limit']], sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')

    # Zeroizing all 1-day limits
    df['final_1_limit'] = 0
    print('\nZeroize all 1-day limits:\n', df[['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), '\n', sep='')
    print('Zeroize all 1-day limits:\n', df.loc[df['store_number'] == '7649581', ['store_number', 'final_21_limit', 'final_7_limit', 'final_1_limit']], sep='')
    print('Accuracy check:\n', df['final_1_limit'].sum(), sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')
    
    # Adjusted final 7 limit cap
    df['final_7_limit'] = calc_final_7_limit_cap(df['final_21_limit'], df['final_7_limit'], df['adjusted_loan_count'], df['rein_update_flag'])
    print('\nAdjust final 7 limit cap:\n', df[['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('Adjust final 7 limit cap sample:\n', df.loc[df['store_number'] == '7649581', ['store_number', 'total_final_21_limit', 'final_21_limit', 'final_7_limit', 'final_1_limit']], sep='')
    print('Accuracy check:\n', df['final_7_limit'].max(), sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')

    # Zeroize new customers who opted in less than six months ago
    for tf in term_frequencies:
        df[f'final_{tf}_limit'] = zeroize_new_cust_less_than_six_months(df[f'final_{tf}_limit'], df['opt_in_date'], refresh_date, trxn_data_period)
    print('\nZeroize new customers who opted in less than six months ago:\n', df[['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('Product cap sample:\n', df.loc[df['store_number'] == '7649581', ['store_number', 'final_21_limit', 'final_7_limit', 'final_1_limit']], sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')
     
    print(df['blacklist_flag'].value_counts())
    print('Zeroize defaulters sample - before:\n', df.loc[df['national_id'] == '28075830', ['store_number', 'blacklist_flag', 'final_21_limit', 'final_7_limit', 'final_1_limit']], sep='')

    
    # Update blacklist flag
    df = updating_blacklist_flag(df)
    print('\nUpdate Blacklist Flag:\n', df[['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('Zeroize defaulters sample - after:\n', df.loc[df['national_id'] == '28075830', ['store_number', 'blacklist_flag', 'final_21_limit', 'final_7_limit', 'final_1_limit']], sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')
    
    print(df['blacklist_flag'].value_counts())
    
    # Zeroize limits for blacklisted merchants
    for tf in term_frequencies:
        df[f'final_{tf}_limit'] = zeroize_blacklisted_merchants(df[f'final_{tf}_limit'], df['blacklist_flag'])
    print('\nZeroize limits for blacklisted merchants:\n', df[['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('Product cap sample:\n', df.loc[df['store_number'] == '7649581', ['store_number', 'final_21_limit', 'final_7_limit', 'final_1_limit']], sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')
    
    # Adjusted final 21 limit
    df['final_21_limit'] = calc_update_final_21_limit(df['final_21_limit'], df['final_7_limit'], df['blacklist_flag'], df['ftd_update_flag'], df['rein_update_flag'])
    print('\nAdjust final 21 limit:\n', df[['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('Adjust final 21 limit sample:\n', df.loc[df['store_number'] == '7649581', ['store_number', 'total_final_21_limit', 'final_21_limit', 'final_7_limit', 'final_1_limit']], sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')
    
    # Adjusted final limit feature
    for tf in term_frequencies:
        df[f'final_{tf}_limit'] = calc_new_final_limit_by_product_cap(df[f'final_{tf}_limit'], tf)
    print('\nProduct cap:\n', df[['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('Product cap sample:\n', df.loc[df['store_number'] == '7649581', ['store_number', 'final_21_limit', 'final_7_limit', 'final_1_limit']], sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')

    # Final limit feature impute missing values
    for tf in term_frequencies:
        df[f'final_{tf}_limit'].fillna(0, inplace=True)
    print('\nImpute missing values:\n', df[['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('Impute missing values sample:\n', df.loc[df['store_number'] == '7649581', ['store_number', 'final_21_limit', 'final_7_limit', 'final_1_limit']], sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')
    
    # Adjusted final limit feature
    for tf in term_frequencies:
        df[f"final_{tf}_limit"] = (np.ceil(df[f"final_{tf}_limit"] / 100) * 100).astype(int)
    print('\nCeiling:\n', df[['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('Ceiling sample:\n', df.loc[df['store_number'] == '7649581', ['store_number', 'final_21_limit', 'final_7_limit', 'final_1_limit']], sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')
    
    # Drop duplicates based on store_number/national id
    df.drop_duplicates(subset="store_number", keep="first", inplace=True)
    print('\nDrop duplicates:\n', df[['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), '\n', sep='')
    print('Drop duplicates sample:\n', df.loc[df['store_number'] == '7649581', ['store_number', 'final_21_limit', 'final_7_limit', 'final_1_limit']], sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')

    # Max global limit feature
    df["max_global_limit"] = df[["final_21_limit", "final_7_limit", "final_1_limit"]].max(axis=1)
    
    print(df['tu_data_scoring'].value_counts())

    # Validate final limits after logic run for regression sub-clusters
    df_reg_clusters_in_scope = df[(df['repayments_by_erd_vs_principal_cluster'].isin(reg_clusters_in_scope)) & (df['dpd30_rm_ge_rm_never_borrowed'] == 1) & (df['max_global_limit'] > 0) & (df['tu_data_scoring'] == 'no')]
    print('\nRegression clusters in scope distribution:\n', df_reg_clusters_in_scope.groupby(['repayments_by_erd_vs_principal_cluster']).agg({'store_number': 'count', 'final_21_limit': 'sum', 'final_7_limit': 'sum', 'final_1_limit': 'sum'}), sep='')
    print('Regression clusters in scope overall:\n', df_reg_clusters_in_scope[['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')
    
    # Validate final limits after logic run for regression sub-clusters
    df_tu_clusters_in_scope = df[(df['repayments_by_erd_vs_principal_cluster'].isin(reg_clusters_in_scope)) & (df['dpd30_rm_ge_rm_never_borrowed'] == 0) & (df['max_global_limit'] > 0) & (df['tu_data_scoring'] == 'yes')]
    print('TU data clusters in scope distribution:\n', df_tu_clusters_in_scope.groupby(['repayments_by_erd_vs_principal_cluster']).agg({'store_number': 'count', 'final_21_limit': 'sum', 'final_7_limit': 'sum', 'final_1_limit': 'sum'}), sep='')
    print('TU data clusters in scope overall:\n', df_tu_clusters_in_scope[['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')

    # Total max global limit feature
    df["total_max_global_limit"] = df[["total_final_21_limit", "total_final_7_limit", "total_final_1_limit"]].max(axis=1)

    # Previous max global limit fetaure
    df["previous_max_global_limit"] = df[["previous_21_limit", "previous_7_limit", "previous_1_limit"]].max(axis=1)

    # Max limit diff feature
    df["max_limit_diff"] = df["max_global_limit"] - df["previous_max_global_limit"]

    # Add model version and created at fetaures
    df = add_model_version_and_create_date(df, model_index, model_start_date, refresh_date, created_at)

    # Logs
    log_model_metrics(config, df, term_frequencies, project_dir + bcsv_clean_data_path, project_dir + metabase_amount_clean_data_path, config_path)
    print('---------------------------------------------------------------------------------------------------------------------------------------')

    # Export snapshot
    # df[risk_features_in_scope].to_excel(project_dir + scored_limits_risk_review_data_path_excel.format(refresh_date.replace("-", "")), index=False)
    df[risk_features_in_scope].to_parquet(project_dir + scored_limits_risk_review_data_path_excel_parquet.format(refresh_date.replace("-", "")), index=False)
    excluded_first_time_df.to_parquet(project_dir + excluded_first_time_data_path, index=False)
    df.to_parquet(project_dir + scored_limits_risk_review_data_path_parquet, index=False)

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
    df_stabilisation_scoring = limit_stabilisation(parsed_args.config, refresh_date)
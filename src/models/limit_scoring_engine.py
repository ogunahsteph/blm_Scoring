# Import modules
import os
import sys


# Import custom modules
sys.path.append(os.path.join(os.getcwd(), ""))

from src.features.merge_datasets import *


# Functions
def calc_limit_factor(idm_recommendation, crb_approve_limit_factor, crb_reject_limit_factor):
    return np.where(idm_recommendation == 'Approve', crb_approve_limit_factor, crb_reject_limit_factor)


def calc_trading_consistency_bands(count):
    tcb = np.where((count >= 0) & (count < 0.30), 'Band 1',
          np.where((count >= 0.30) & (count < 0.50), 'Band 2',
          np.where((count >= 0.50) & (count < 0.60), 'Band 3',
          np.where((count >= 0.60) & (count < 0.70), 'Band 4',
          np.where((count >= 0.70) & (count < 0.80), 'Band 5',
          np.where((count >= 0.80) & (count < 0.90), 'Band 6',
          np.where((count >= 0.90) & (count <= 1.00), 'Band 7',
          np.nan)))))))
    
    return tcb


def calc_loan_count_bands(count):
    lcb = np.where((count == 0), 'Band 1',
          np.where((count == 1) | (count == 2), 'Band 2',
          np.where((count == 3) | (count == 4), 'Band 3',
          np.where((count == 5) | (count == 6), 'Band 4',
          np.where((count == 7) | (count == 8), 'Band 5',
          np.where((count == 9) | (count == 10), 'Band 6',
          np.where((count == 11) | (count == 12), 'Band 7',
          np.where(count > 12, 'Band 8',
          np.nan))))))))
    
    return lcb


# defining a function to use the bands for both trading consistency and loan count to get limit factors
def calc_limit_factor_21(df):
    trading_consistency = df['trading_consistency_bands']
    loan_count = df['loan_count_bands']
  
    if (trading_consistency == 'Band 1' and loan_count == 'Band 1') or (trading_consistency == 'Band 1' and loan_count == 'Band 2') or (trading_consistency == 'Band 1' and loan_count == 'Band 3')  or (trading_consistency == 'Band 2' and loan_count == 'Band 1') or (trading_consistency == 'Band 2' and loan_count == 'Band 2') or (trading_consistency == 'Band 3' and loan_count == 'Band 1') or (trading_consistency == 'Band 2' and loan_count == 'Band 3') or (trading_consistency == 'Band 4' and loan_count == 'Band 1') or (trading_consistency == 'Band 1' and loan_count == 'Band 4') or (trading_consistency == 'Band 1' and loan_count == 'Band 5') or (trading_consistency == 'Band 2' and loan_count == 'Band 4') or (trading_consistency == 'Band 3' and loan_count == 'Band 2') or (trading_consistency == 'Band 3' and loan_count == 'Band 3') or (trading_consistency == 'Band 1' and loan_count == 'Band 6') or (trading_consistency == 'Band 2' and loan_count == 'Band 5') or (trading_consistency == 'Band 3' and loan_count == 'Band 4') or (trading_consistency == 'Band 4' and loan_count == 'Band 2') or (trading_consistency == 'Band 4' and loan_count == 'Band 3') or (trading_consistency == 'Band 1' and loan_count == 'Band 7') or (trading_consistency == 'Band 2' and loan_count == 'Band 6') or (trading_consistency == 'Band 3' and loan_count == 'Band 5') or (trading_consistency == 'Band 4' and loan_count == 'Band 4') or (trading_consistency == 'Band 1' and loan_count == 'Band 8') or (trading_consistency == 'Band 2' and loan_count == 'Band 7') or (trading_consistency == 'Band 3' and loan_count == 'Band 6') or (trading_consistency == 'Band 4' and loan_count == 'Band 5') or (trading_consistency == 'Band 2' and loan_count == 'Band 8') or (trading_consistency == 'Band 3' and loan_count == 'Band 7') or (trading_consistency == 'Band 4' and loan_count == 'Band 6') or (trading_consistency == 'Band 3' and loan_count == 'Band 8') or (trading_consistency == 'Band 4' and loan_count == 'Band 7') or (trading_consistency == 'Band 4' and loan_count == 'Band 8'):
        return 0.00
    elif (trading_consistency == 'Band 5' and loan_count == 'Band 1'):
        return 0.10
    elif (trading_consistency == 'Band 5' and loan_count == 'Band 2') or (trading_consistency == 'Band 5' and loan_count == 'Band 3') or (trading_consistency == 'Band 6' and loan_count == 'Band 1'):
        return 0.15
    elif (trading_consistency == 'Band 5' and loan_count == 'Band 4') or (trading_consistency == 'Band 6' and loan_count == 'Band 2') or (trading_consistency == 'Band 7' and loan_count == 'Band 1'):
        return 0.20
    elif (trading_consistency == 'Band 5' and loan_count == 'Band 5') or (trading_consistency == 'Band 6' and loan_count == 'Band 3') or (trading_consistency == 'Band 7' and loan_count == 'Band 2'):
        return 0.25
    elif (trading_consistency == 'Band 5' and loan_count == 'Band 6') or (trading_consistency == 'Band 6' and loan_count == 'Band 4') or (trading_consistency == 'Band 7' and loan_count == 'Band 3'):
        return 0.30
    elif (trading_consistency == 'Band 5' and loan_count == 'Band 7') or (trading_consistency == 'Band 6' and loan_count == 'Band 5') or (trading_consistency == 'Band 7' and loan_count == 'Band 4'):
        return 0.35
    elif (trading_consistency == 'Band 5' and loan_count == 'Band 8') or (trading_consistency == 'Band 6' and loan_count == 'Band 6') or (trading_consistency == 'Band 7' and loan_count == 'Band 5'):
        return 0.40
    elif (trading_consistency == 'Band 6' and loan_count == 'Band 7') or (trading_consistency == 'Band 7' and loan_count == 'Band 6'):
        return 0.45
    elif (trading_consistency == 'Band 6' and loan_count == 'Band 8') or (trading_consistency == 'Band 7' and loan_count == 'Band 7'):
        return 0.50
    elif trading_consistency == 'Band 7' and loan_count == 'Band 8':
        return 0.55


def calc_limit_factor_7(df):
    trading_consistency = df['trading_consistency_bands']
    loan_count = df['loan_count_bands']
  
    if (trading_consistency == 'Band 1' and loan_count == 'Band 1') or (trading_consistency == 'Band 1' and loan_count == 'Band 2') or (trading_consistency == 'Band 1' and loan_count == 'Band 3')  or (trading_consistency == 'Band 2' and loan_count == 'Band 1') or (trading_consistency == 'Band 2' and loan_count == 'Band 2') or (trading_consistency == 'Band 3' and loan_count == 'Band 1') or (trading_consistency == 'Band 2' and loan_count == 'Band 3') or (trading_consistency == 'Band 4' and loan_count == 'Band 1') or (trading_consistency == 'Band 1' and loan_count == 'Band 4') or (trading_consistency == 'Band 1' and loan_count == 'Band 5') or (trading_consistency == 'Band 2' and loan_count == 'Band 4') or (trading_consistency == 'Band 3' and loan_count == 'Band 2') or (trading_consistency == 'Band 3' and loan_count == 'Band 3') or (trading_consistency == 'Band 1' and loan_count == 'Band 6') or (trading_consistency == 'Band 2' and loan_count == 'Band 5') or (trading_consistency == 'Band 3' and loan_count == 'Band 4') or (trading_consistency == 'Band 4' and loan_count == 'Band 2') or (trading_consistency == 'Band 4' and loan_count == 'Band 3') or (trading_consistency == 'Band 1' and loan_count == 'Band 7') or (trading_consistency == 'Band 2' and loan_count == 'Band 6') or (trading_consistency == 'Band 3' and loan_count == 'Band 5') or (trading_consistency == 'Band 4' and loan_count == 'Band 4') or (trading_consistency == 'Band 1' and loan_count == 'Band 8') or (trading_consistency == 'Band 2' and loan_count == 'Band 7') or (trading_consistency == 'Band 3' and loan_count == 'Band 6') or (trading_consistency == 'Band 4' and loan_count == 'Band 5') or (trading_consistency == 'Band 2' and loan_count == 'Band 8') or (trading_consistency == 'Band 3' and loan_count == 'Band 7') or (trading_consistency == 'Band 4' and loan_count == 'Band 6') or (trading_consistency == 'Band 3' and loan_count == 'Band 8') or (trading_consistency == 'Band 4' and loan_count == 'Band 7') or (trading_consistency == 'Band 4' and loan_count == 'Band 8'):
        return 0.00
    elif (trading_consistency == 'Band 5' and loan_count == 'Band 1'):
        return 0.10
    elif (trading_consistency == 'Band 5' and loan_count == 'Band 2') or (trading_consistency == 'Band 6' and loan_count == 'Band 1') or (trading_consistency == 'Band 7' and loan_count == 'Band 1'):
        return 0.125
    elif (trading_consistency == 'Band 5' and loan_count == 'Band 3') or (trading_consistency == 'Band 6' and loan_count == 'Band 2') or (trading_consistency == 'Band 6' and loan_count == 'Band 3') or (trading_consistency == 'Band 7' and loan_count == 'Band 2'):
        return 0.15
    elif (trading_consistency == 'Band 5' and loan_count == 'Band 4') or (trading_consistency == 'Band 5' and loan_count == 'Band 5') or (trading_consistency == 'Band 6' and loan_count == 'Band 4') or (trading_consistency == 'Band 7' and loan_count == 'Band 3'):
        return 0.175
    elif (trading_consistency == 'Band 5' and loan_count == 'Band 6') or (trading_consistency == 'Band 6' and loan_count == 'Band 5') or (trading_consistency == 'Band 7' and loan_count == 'Band 4'):
        return 0.20
    elif (trading_consistency == 'Band 5' and loan_count == 'Band 7') or (trading_consistency == 'Band 6' and loan_count == 'Band 6') or (trading_consistency == 'Band 7' and loan_count == 'Band 5'):
        return 0.225
    elif (trading_consistency == 'Band 5' and loan_count == 'Band 8') or (trading_consistency == 'Band 6' and loan_count == 'Band 7') or (trading_consistency == 'Band 7' and loan_count == 'Band 6'):
        return 0.25
    elif (trading_consistency == 'Band 6' and loan_count == 'Band 8') or (trading_consistency == 'Band 7' and loan_count == 'Band 7'):
        return 0.275
    elif trading_consistency == 'Band 7' and loan_count == 'Band 8':
        return 0.30


def calc_limit_factor_1(df):
    trading_consistency = df['trading_consistency_bands']
    loan_count = df['loan_count_bands']
  
    if (trading_consistency == 'Band 1' and loan_count == 'Band 1') or (trading_consistency == 'Band 1' and loan_count == 'Band 2') or (trading_consistency == 'Band 1' and loan_count == 'Band 3')  or (trading_consistency == 'Band 2' and loan_count == 'Band 1') or (trading_consistency == 'Band 2' and loan_count == 'Band 2') or (trading_consistency == 'Band 3' and loan_count == 'Band 1') or (trading_consistency == 'Band 2' and loan_count == 'Band 3') or (trading_consistency == 'Band 4' and loan_count == 'Band 1') or (trading_consistency == 'Band 1' and loan_count == 'Band 4') or (trading_consistency == 'Band 1' and loan_count == 'Band 5') or (trading_consistency == 'Band 2' and loan_count == 'Band 4') or (trading_consistency == 'Band 3' and loan_count == 'Band 2') or (trading_consistency == 'Band 3' and loan_count == 'Band 3') or (trading_consistency == 'Band 1' and loan_count == 'Band 6') or (trading_consistency == 'Band 2' and loan_count == 'Band 5') or (trading_consistency == 'Band 3' and loan_count == 'Band 4') or (trading_consistency == 'Band 4' and loan_count == 'Band 2') or (trading_consistency == 'Band 4' and loan_count == 'Band 3') or (trading_consistency == 'Band 1' and loan_count == 'Band 7') or (trading_consistency == 'Band 2' and loan_count == 'Band 6') or (trading_consistency == 'Band 3' and loan_count == 'Band 5') or (trading_consistency == 'Band 4' and loan_count == 'Band 4') or (trading_consistency == 'Band 1' and loan_count == 'Band 8') or (trading_consistency == 'Band 2' and loan_count == 'Band 7') or (trading_consistency == 'Band 3' and loan_count == 'Band 6') or (trading_consistency == 'Band 4' and loan_count == 'Band 5') or (trading_consistency == 'Band 2' and loan_count == 'Band 8') or (trading_consistency == 'Band 3' and loan_count == 'Band 7') or (trading_consistency == 'Band 4' and loan_count == 'Band 6') or (trading_consistency == 'Band 3' and loan_count == 'Band 8') or (trading_consistency == 'Band 4' and loan_count == 'Band 7') or (trading_consistency == 'Band 4' and loan_count == 'Band 8'):
        return 0.00
    elif (trading_consistency == 'Band 5' and loan_count == 'Band 1'):
        return 0.10
    elif (trading_consistency == 'Band 5' and loan_count == 'Band 2') or (trading_consistency == 'Band 6' and loan_count == 'Band 1') or (trading_consistency == 'Band 7' and loan_count == 'Band 1'):
        return 0.125
    elif (trading_consistency == 'Band 5' and loan_count == 'Band 3') or (trading_consistency == 'Band 6' and loan_count == 'Band 2') or (trading_consistency == 'Band 6' and loan_count == 'Band 3') or (trading_consistency == 'Band 7' and loan_count == 'Band 2'):
        return 0.15
    elif (trading_consistency == 'Band 5' and loan_count == 'Band 4') or (trading_consistency == 'Band 5' and loan_count == 'Band 5') or (trading_consistency == 'Band 6' and loan_count == 'Band 4') or (trading_consistency == 'Band 7' and loan_count == 'Band 3'):
        return 0.175
    elif (trading_consistency == 'Band 5' and loan_count == 'Band 6') or (trading_consistency == 'Band 6' and loan_count == 'Band 5') or (trading_consistency == 'Band 7' and loan_count == 'Band 4'):
        return 0.20
    elif (trading_consistency == 'Band 5' and loan_count == 'Band 7') or (trading_consistency == 'Band 6' and loan_count == 'Band 6') or (trading_consistency == 'Band 7' and loan_count == 'Band 5'):
        return 0.225
    elif (trading_consistency == 'Band 5' and loan_count == 'Band 8') or (trading_consistency == 'Band 6' and loan_count == 'Band 7') or (trading_consistency == 'Band 7' and loan_count == 'Band 6'):
        return 0.25
    elif (trading_consistency == 'Band 6' and loan_count == 'Band 8') or (trading_consistency == 'Band 7' and loan_count == 'Band 7'):
        return 0.275
    elif trading_consistency == 'Band 7' and loan_count == 'Band 8':
        return 0.30


def cal_days_past_due(days_past_due, loan_count):  
    return np.where(loan_count == 0, 0, days_past_due)


# def cal_weight_dpd(weight_dpd, loan_count):
#     return np.where(loan_count == 0, 1, weight_dpd)


def cal_weight_dpd(days_past_due):
    return np.where(days_past_due <= 0, 1, 0)


def cal_good_loans_repayment_ratio(good_loans_repayment_ratio, loan_count):   
    return np.where(loan_count == 0, 1, good_loans_repayment_ratio)


# def cal_good_loans_repayment_ratio_exceptions(good_loans_repayment_ratio):   
#     return np.where(good_loans_repayment_ratio > 0, 1, good_loans_repayment_ratio)


def cal_weight_good_loans_repayment_ratio(good_loans_repayment_ratio):
    return np.where(good_loans_repayment_ratio >= 1, good_loans_repayment_ratio, 0)


def calc_weight_consistency_old(page_active_days):
    cwc = np.where(page_active_days >= 0.7, 1.0,
          np.where((page_active_days <= 0.69) & (page_active_days >= 0.63), 0.9,
          np.where((page_active_days <= 0.62) & (page_active_days >= 0.56), 0.8,
          np.where((page_active_days <= 0.55) & (page_active_days >= 0.49), 0.7,
          0))))
    
    return cwc


def calc_weight_consistency(page_active_days):
    return np.where(page_active_days >= 0.85, 1.0, 0)


def cal_weight_recency(days_since_last_trx):
    return np.where(days_since_last_trx <= 3, 1, 0)


def limits_decrease_zeroization(config, loan_count, limit_col, latest_loan_dpd, term_frequency, repayment_ratio, inference_col):
    """
    function to adjust allocated limits in line with past repayment behavior
    
    Inputs:
    1) num of loans taken,
    2) previously allocated limit,
    3) term frequency for a loan,
    4) days past due for the most recent loan
    5) good loans repayment ratio
    6) inference variable i.e declaring whether borrower qualifies for limit stabilization OR not
    
    Output:
    adjusted limit based on rollover/default patterns of most recent loan
    """

    # Load configurations
    dpd_allowance = config["limits_decrease_zeroization_config"]["dpd_allowance"]
    repayment_ratio_threshold = config["limits_decrease_zeroization_config"]["repayment_ratio_threshold"]
    zeroize = config["limits_decrease_zeroization_config"]["zeroize"]
    
    # Conditions
    conditions = [loan_count.isna(),
                  loan_count.eq(0),
                  inference_col.str.match("relax_rules"),
                  inference_col.str.match("No_rules_relaxed") & latest_loan_dpd.ge(term_frequency+30),
                  inference_col.str.match("No_rules_relaxed") & repayment_ratio.lt(repayment_ratio_threshold) & latest_loan_dpd.gt(term_frequency + dpd_allowance),
                  inference_col.str.match("No_rules_relaxed") & repayment_ratio.lt(repayment_ratio_threshold) & latest_loan_dpd.le(term_frequency + dpd_allowance),
                  inference_col.str.match("No_rules_relaxed") & repayment_ratio.ge(repayment_ratio_threshold) & latest_loan_dpd.ge(term_frequency + 30),
                  inference_col.str.match("No_rules_relaxed") & repayment_ratio.ge(repayment_ratio_threshold) & latest_loan_dpd.le(term_frequency + dpd_allowance),
                  inference_col.str.match("No_rules_relaxed") & repayment_ratio.ge(repayment_ratio_threshold) & latest_loan_dpd.ge(term_frequency + 20),
                  inference_col.str.match("No_rules_relaxed") & repayment_ratio.ge(repayment_ratio_threshold) & latest_loan_dpd.ge(term_frequency + 29)]  
    
    # Choices
    choices = [limit_col,
               limit_col,
               limit_col,
               zeroize,
               zeroize,
               limit_col,
               zeroize,
               limit_col,
               limit_col * 0.7,
               limit_col * 0.3]
    
    # Adjusted product limit allocation feature
    limit_column = np.select(conditions, choices)
    
    return limit_column


def limit_zeroization_till_summary(config, transaction_boolean_col, consistency_col, limit_col, inference_col):
    """
    function to adjust limits based on till summaries i.e till activity and recency
    
    Inputs:
    1) recency of transactions boolean check,
    2) till consistency calculated probability,
    3) previously allocated limit,
    4) inference variable i.e declaring whether borrower qualifies for limit stabilization OR not
    
    Output:
    adjusted limits in line with till summaries i.e limits zeroized for any customer who does not meet set threshold
    """
    # Load configurations
    transaction_boolean_accepted = config["limit_zeroization_till_summary_config"]["transaction_boolean_accepted"]
    transaction_boolean_rejected = config["limit_zeroization_till_summary_config"]["transaction_boolean_rejected"]
    consistency_threshold = config["limit_zeroization_till_summary_config"]["consistency_threshold"]
    zeroize = config["limit_zeroization_till_summary_config"]["zeroize"]
    
    # Conditions
    conditions = [inference_col.str.match("relax_rules"),
                  inference_col.str.match("No_rules_relaxed") & transaction_boolean_col.str.contains(transaction_boolean_accepted) & consistency_col.ge(consistency_threshold),
                  inference_col.str.match("No_rules_relaxed") & transaction_boolean_col.str.contains(transaction_boolean_accepted) & consistency_col.lt(consistency_threshold),
                  inference_col.str.match("No_rules_relaxed") & transaction_boolean_col.str.contains(transaction_boolean_rejected) & consistency_col.ge(consistency_threshold),
                  inference_col.str.match("No_rules_relaxed") & transaction_boolean_col.str.contains(transaction_boolean_rejected) & consistency_col.lt(consistency_threshold)]
    
    # Choices
    choices = [limit_col,
               limit_col,
               zeroize,
               zeroize,
               zeroize]
    
    # Adjusted product limit allocation feature
    limit_column = np.select(conditions, choices)
    
    return limit_column


def adjust_limits_to_loan_bands_21(config, loan_count, limit_col):
    """
    function to limit loan limits based on loan bands
    
    Inputs:
    1) loan count of a borrower, 
    2) allocated limit
    
    Output:
    adjusted limit based on loan band caps
    """
    # Load configurations
    no_loans = config["adjust_limits_to_loan_bands_21_config"]["no_loans"]
    
    # Conditions
    conditions = [loan_count.isna(),
                  loan_count.isna(),
                  loan_count.eq(0),
                  loan_count.eq(0),
                  loan_count.gt(0)]
    
    # Choices
    choices = [no_loans,
               no_loans,
               no_loans,
               no_loans,
               limit_col]
    
    # Adjusted 21 day product limit allocation feature
    limit_column = np.select(conditions, choices)
    
    return limit_column


def calc_ultimate_factor_1(ultimate_factor_1):
    alt1 = np.where(ultimate_factor_1 >=0.15, 0.15, ultimate_factor_1)
    
    return alt1


def calc_new_final_limit_by_product_cap(adjusted_limit, tf):
    nfl = np.where((adjusted_limit < 1000) & (tf in [21, 7]), 0,
          np.where((adjusted_limit < 200) & (tf in [1]), 0,
          np.where(adjusted_limit > 200000, 200000,
          adjusted_limit)))
    
    return nfl


def limit_scoring(config_path):
   # Load configurations
    config = read_params(config_path)
    project_dir = config["project_dir"]
    merged_data_path = config["processed_data_config"]["merged_data_parquet"]
    loan_count_threshold = config["adjusted_loan_count_config"]["loan_count_threshold"]
    num_days_since_last_disbursement_threshold = config["adjusted_loan_count_config"]["num_days_since_last_disbursement_threshold"]
    term_frequencies = config["crb_limit_factor_config"]["term_frequencies"]
    scored_limits_data_path = config["interim_data_config"]["scored_limits_data_parquet"]

    # Load snapshot
    df = pd.read_parquet(project_dir + merged_data_path)

    # Adjusted loan count feature
    df["adjusted_loan_count"] = df["loan_count"]
    df.loc[(df["idm_recommendation"] == "Reject") & (df["loan_count"].astype('int') < loan_count_threshold), "adjusted_loan_count"] = 0
    df.loc[df["num_days_since_last_disbursement"] > num_days_since_last_disbursement_threshold, "adjusted_loan_count"] = 0
    
    # Limit factor feature
    for tf in term_frequencies:
        # Load configurations
        crb_approve_limit_factor = config["crb_limit_factor_config"][f"crb_approve_limit_factor_{tf}"]
        crb_reject_limit_factor = config["crb_limit_factor_config"][f"crb_reject_limit_factor_{tf}"]
        
        # Get limit factor
        df[f'limit_factor_{tf}'] = calc_limit_factor(df["idm_recommendation"], crb_approve_limit_factor, crb_reject_limit_factor)
    
    # IDM limit factor feature
    for tf in term_frequencies:
        # Load configurations
        crb_approve_limit_factor = config["crb_limit_factor_config"][f"crb_approve_limit_factor_{tf}"]
        
        # Set IDM limit factor
        df[f'idm_factor_{tf}'] = df[f'limit_factor_{tf}'] / crb_approve_limit_factor
    
    # Trading consistency bands feature
    df['trading_consistency_bands'] = calc_trading_consistency_bands(df['page_active_days'])

    # Loan count bands feature
    df['loan_count_bands'] = calc_loan_count_bands(df['adjusted_loan_count'])

    # New limit factor feature
    df['new_limit_factor_21'] = df.apply(lambda x: calc_limit_factor_21(x), axis = 1)
    df['new_limit_factor_7'] = df.apply(lambda x: calc_limit_factor_7(x), axis = 1)
    df['new_limit_factor_1'] = df.apply(lambda x: calc_limit_factor_1(x), axis = 1)

    # Adjusted days past due feature
    df['days_past_due'] = cal_days_past_due(df['days_past_due'], df['loan_count'])

    # Adjusted weight DPD feature
    df['weight_dpd'] = cal_weight_dpd(df['days_past_due'])

    # Adjusted good loans repayment ratio feature
    df['good_loans_repayment_ratio'] = cal_good_loans_repayment_ratio(df['good_loans_repayment_ratio'], df['loan_count'])

    # Weight good loans repayment ration feature
    df['weight_good_loans_repayment_ratio'] = cal_weight_good_loans_repayment_ratio(df['good_loans_repayment_ratio'])

    # Weight consistency feature
    df['weight_consistency'] = calc_weight_consistency(df['page_active_days'])

    # Weight recency feature
    df['weight_recency'] = cal_weight_recency(df['days_since_last_trx'])

    # Define risk rules factor feature
    df['risk_rules_factor'] = (df['weight_dpd'] + df['weight_good_loans_repayment_ratio'] + df['weight_consistency'] + df['weight_recency']) / 4

    # Ultimate factor feature
    for tf in term_frequencies:
        df[f'ultimate_factor_{tf}'] = df['risk_rules_factor'] * df[f'idm_factor_{tf}'] * df[f'new_limit_factor_{tf}']
                    
    # Adjusted final 21 limit feature
    df['ultimate_factor_1'] = calc_ultimate_factor_1(df['ultimate_factor_1'])
    print('\nMaximum ultimate factor 1:\n', df['ultimate_factor_1'].max())
    print('---------------------------------------------------------------------------------------------------------------------------------------')
    
    # Bloom limit feature
    for tf in term_frequencies:
        df[f'limit_{tf}_day'] = df['approx_30_days_trx_val'] * df[f'ultimate_factor_{tf}']
    print('\nInitial limits assignment:\n', df[['limit_21_day', 'limit_7_day', 'limit_1_day']].sum(), sep='')
    print('Initial limits assignment sample:\n', df.loc[df['store_number'] == '7954829', ['store_number', 'limit_21_day', 'limit_7_day', 'limit_1_day']], sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')
    
    # Adjusted limit feature
    for tf in term_frequencies:
        df[f"adjusted_{tf}_limit"] = limits_decrease_zeroization(config, df["adjusted_loan_count"], df[f"limit_{tf}_day"], df["days_past_due"], df["term_frequency"], df["good_loans_repayment_ratio"], df["inference_col"])
    print('\nAfter limits decrease zeroization:\n', df[['adjusted_21_limit', 'adjusted_7_limit', 'adjusted_1_limit']].sum(), sep='')
    print('After limits decrease zeroization sample:\n', df.loc[df['store_number'] == '7954829', ['store_number', 'adjusted_21_limit', 'adjusted_7_limit', 'adjusted_1_limit']], sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')
    
    # Adjusted limit feature
    for tf in term_frequencies:
        df[f"adjusted_{tf}_limit"] = limit_zeroization_till_summary(config, df["transacted_last_5_days"], df["page_active_days"], df[f"adjusted_{tf}_limit"], df["inference_col"])
    print('\nAfter limits zeroization till summary:\n', df[['adjusted_21_limit', 'adjusted_7_limit', 'adjusted_1_limit']].sum(), sep='')
    print('After limits zeroization till summary sample:\n', df.loc[df['store_number'] == '7954829', ['store_number', 'adjusted_21_limit', 'adjusted_7_limit', 'adjusted_1_limit']], sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')
    
    # Adjusted 21 day limit feature
    df["adjusted_21_limit"] = adjust_limits_to_loan_bands_21(config, df["adjusted_loan_count"], df["adjusted_21_limit"])
    print('\nAfter adjusting 21 day limits to loan bands:\n', df[['adjusted_21_limit', 'adjusted_7_limit', 'adjusted_1_limit']].sum(), sep='')
    print('After adjusting 21 day limits to loan bands sample:\n', df.loc[df['store_number'] == '7954829', ['store_number', 'adjusted_21_limit', 'adjusted_7_limit', 'adjusted_1_limit']], sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')

    # Final limit feature
    for tf in term_frequencies:
        df[f'final_{tf}_limit'] = calc_new_final_limit_by_product_cap(df[f'adjusted_{tf}_limit'], tf)
    print('\nProduct cap:\n', df[['adjusted_21_limit', 'adjusted_7_limit', 'adjusted_1_limit']].sum(), sep='')
    print('Product cap sample:\n', df.loc[df['store_number'] == '7954829', ['store_number', 'adjusted_21_limit', 'adjusted_7_limit', 'adjusted_1_limit']], sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')
    
    # Final limit feature impute missing values
    for tf in term_frequencies:
        df[f'final_{tf}_limit'].fillna(0, inplace=True)
    print('\nImputing missing values:\n', df[['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('Imputing missing values sample:\n', df.loc[df['store_number'] == '7954829', ['store_number', 'final_21_limit', 'final_7_limit', 'final_1_limit']], sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')
    
    # Adjusted final limit feature
    for tf in term_frequencies:
        df[f"final_{tf}_limit"] = (np.ceil(df[f"final_{tf}_limit"] / 100) * 100).astype(int)
    print('\nCeiling:\n', df[['final_21_limit', 'final_7_limit', 'final_1_limit']].sum(), sep='')
    print('Ceiling sample:\n', df.loc[df['store_number'] == '7954829', ['store_number', 'final_21_limit', 'final_7_limit', 'final_1_limit']], sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')

    # Drop duplicates based on store_number/national id
    df.drop_duplicates(subset="store_number", keep="first", inplace=True)
    
    # Export snapshot
    df.to_parquet(project_dir + scored_limits_data_path, index=False)

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
    df_stabilisation_scoring = limit_scoring(parsed_args.config)
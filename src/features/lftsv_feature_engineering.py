# Import modules
import os
import sys


# Import custom modules
sys.path.append(os.path.join(os.getcwd(), ""))

from src.features.td_aggregate_features import *


# Functions
def saf_loan_balance(total_outstanding, safaricom_loan_balance):
    return np.where(safaricom_loan_balance >= 0, safaricom_loan_balance, total_outstanding)


def cal_loan_count_flag(loan_count):
        return np.where(loan_count == 1, 'New Client', 'Repeat Client')


def calc_days_past_due(loan_status, due_date_fixed, closed_on_date, max_transaction_date, extract_end_date):
    """
    Function to calculate days past due for each loan record. 
    The function uses pandas.Series vectorized arguments to ensure fast iterations/loops.
    Inputs are the arguments passed in the conditions list.
    Outputs are the results that are reported based on the choices list.
    Each output/choice assigned corresponds to the input/conditions level assigned above i.e.
    the first condition corresponds to the first choice etc, as such the rows of conditions & choices need to match.
    If the output reported is a string '0', that's an error/edgecase whose conditions, choices were not well declared.
    
    Inputs: 
    1) The current status of a loan as captured on corebanking(Mifos),
    2) The loans due date, 
    3) the last repayment date on record for the loan.
    
    
    Outputs:
    A calculation of a loan's number of days past due, that is converted from datetime to integer.
    
    """   
    # The main input is the loan status id that is used to slice the df
    conditions = [loan_status.eq(300),
                  loan_status.eq(600),
                  loan_status.eq(601),
                  loan_status.eq(700)]
    
    # The other date inputs are used for calculation based on the condition of loan status of a loan at any given point
    choices = [(extract_end_date - due_date_fixed).dt.days,
               (max_transaction_date - due_date_fixed).dt.days,
               (closed_on_date - due_date_fixed).dt.days,
               (max_transaction_date - due_date_fixed).dt.days]
    
    # Days past due feature
    days_past_due = np.select(conditions, choices)
    
    return days_past_due


def cal_end_rollover_date_fixed(term_frequency, due_date_fixed):
    erdf = np.where(term_frequency == 1, due_date_fixed + np.timedelta64(1,'D'),
           np.where(term_frequency == 7, due_date_fixed + np.timedelta64(3,'D'),
                    due_date_fixed + np.timedelta64(5,'D')))
    
    return erdf


def calc_days_past_end_rollover(loan_status, end_rollover_date_fixed, closed_on_date, max_transaction_date, extract_end_date):
    """
    Function to calculate days past due for each loan record. 
    The function uses pandas.Series vectorized arguments to ensure fast iterations/loops.
    Inputs are the arguments passed in the conditions list.
    Outputs are the results that are reported based on the choices list.
    Each output/choice assigned corresponds to the input/conditions level assigned above i.e.
    the first condition corresponds to the first choice etc, as such the rows of conditions & choices need to match.
    If the output reported is a string '0', that's an error/edgecase whose conditions, choices were not well declared.
    
    Inputs: 
    1) The current status of a loan as captured on corebanking(Mifos),
    2) The loans due date, 
    3) the last repayment date on record for the loan.
    
    
    Outputs:
    A calculation of a loan's number of days past due, that is converted from datetime to integer.
    
    """   
    # The main input is the loan status id that is used to slice the df
    conditions = [loan_status.eq(300),
                  loan_status.eq(600),
                  loan_status.eq(601),
                  loan_status.eq(700)]
    
    # The other date inputs are used for calculation based on the condition of loan status of a loan at any given point
    choices = [(extract_end_date - end_rollover_date_fixed).dt.days,
               (max_transaction_date - end_rollover_date_fixed).dt.days,
               (closed_on_date - end_rollover_date_fixed).dt.days,
               (max_transaction_date - end_rollover_date_fixed).dt.days]
    
    # Days past end rollover feature
    days_past_end_rollover = np.select(conditions, choices)
    
    return days_past_end_rollover


def set_loan_status_labels(loan_status, days_past_due, term_frequency, bloom_version):
    """
    Function to set the loan repayment status of a loan.
    Inputs are the arguments passed in the conditions list.
    Outputs are the results that are reported based on the choices list.
    Each output/choice assigned corresponds to the input/conditions level assigned above i.e.
    the first condition corresponds to the first choice etc, as such the rows of conditions & choices need to match.
    If the output reported is a string '0', that's an error/edgecase whose conditions, choices were not well declared.
    
    Inputs: 
    1) The current status of a loan as captured on corebanking(Mifos),
    2) Number of days past due for each loan, 
    3) Term frequency for each loan,
    4) The version of Bloom tied to the loan record
    
    Outputs: 
    A string label noting the current loan repayment status of each loan record
    """
    # Thresholds
    thirty_day_product_rollover = 7 #Bloom 1.0
    twenty_one_day_product_rollover = 5 #Bloom 2.0 only
    seven_day_product_rollover_bloom1 = 7 #Bloom 2.0 adjusted to 5 days from 7 days in Bloom 1.0
    seven_day_product_rollover_bloom2 = 5 
    three_day_product_rollover = 2
    one_day_product_rollover = 1 #Bloom 2.0, may be adjusted to 5 days as well
    
    # Conditions
    conditions = [
        #written off loans
        loan_status.eq(601),
        
        #currently active OR loans closed in tenure
        loan_status.eq(300) & days_past_due.le(0),
        loan_status.eq(600) & days_past_due.lt(0),
        loan_status.eq(700) & days_past_due.lt(0),
        loan_status.eq(600) & days_past_due.eq(0),
        loan_status.eq(700) & days_past_due.eq(0),
    
        #active loans that are presently in rollover
        loan_status.eq(300) & term_frequency.eq(1) & days_past_due.le(one_day_product_rollover),
        loan_status.eq(300) & term_frequency.eq(3) & days_past_due.le(three_day_product_rollover),
        loan_status.eq(300) & term_frequency.eq(7) & bloom_version.eq(1) & days_past_due.le(seven_day_product_rollover_bloom1),
        loan_status.eq(300) & term_frequency.eq(7) & bloom_version.eq(2) & days_past_due.le(seven_day_product_rollover_bloom2),
        loan_status.eq(300) & term_frequency.eq(21) & days_past_due.le(twenty_one_day_product_rollover),
        loan_status.eq(300) & term_frequency.eq(30) & days_past_due.le(thirty_day_product_rollover),
    
        #loans that were cleared/closed after they got to rollover and cleared with exact balance due
        loan_status.eq(600) & term_frequency.eq(1) & days_past_due.le(one_day_product_rollover),
        loan_status.eq(600) & term_frequency.eq(3) & days_past_due.le(three_day_product_rollover),
        loan_status.eq(600) & term_frequency.eq(7) & bloom_version.eq(1) & days_past_due.le(seven_day_product_rollover_bloom1),
        loan_status.eq(600) & term_frequency.eq(7) & bloom_version.eq(2) & days_past_due.le(seven_day_product_rollover_bloom2),
        loan_status.eq(600) & term_frequency.eq(21) & days_past_due.le(twenty_one_day_product_rollover),
        loan_status.eq(600) & term_frequency.eq(30) & days_past_due.le(thirty_day_product_rollover),
    
        #loans that were cleared/closed after they got to rollover and were overpaid
        loan_status.eq(700) & term_frequency.eq(1) & days_past_due.le(one_day_product_rollover),
        loan_status.eq(700) & term_frequency.eq(3) & days_past_due.le(three_day_product_rollover),
        loan_status.eq(700) & term_frequency.eq(7) & bloom_version.eq(1) & days_past_due.le(seven_day_product_rollover_bloom1),
        loan_status.eq(700) & term_frequency.eq(7) & bloom_version.eq(2) & days_past_due.le(seven_day_product_rollover_bloom2),
        loan_status.eq(700) & term_frequency.eq(21) & days_past_due.le(twenty_one_day_product_rollover),
        loan_status.eq(700) & term_frequency.eq(30) & days_past_due.le(thirty_day_product_rollover),

        #active loans that are presently in default
        loan_status.eq(300) & term_frequency.eq(1) & days_past_due.gt(one_day_product_rollover),
        loan_status.eq(300) & term_frequency.eq(3) & days_past_due.gt(three_day_product_rollover),
        loan_status.eq(300) & term_frequency.eq(7) & bloom_version.eq(1) & days_past_due.gt(seven_day_product_rollover_bloom1),
        loan_status.eq(300) & term_frequency.eq(7) & bloom_version.eq(2) & days_past_due.gt(seven_day_product_rollover_bloom2),
        loan_status.eq(300) & term_frequency.eq(21) & days_past_due.gt(twenty_one_day_product_rollover),
        loan_status.eq(300) & term_frequency.eq(30) & days_past_due.gt(thirty_day_product_rollover),


        #loans were cleared/closed when they had got to default status and cleared with exact balance due
        loan_status.eq(600) & term_frequency.eq(1) & days_past_due.gt(one_day_product_rollover),
        loan_status.eq(600) & term_frequency.eq(3) & days_past_due.gt(three_day_product_rollover),
        loan_status.eq(600) & term_frequency.eq(7) & bloom_version.eq(1) & days_past_due.gt(seven_day_product_rollover_bloom1),
        loan_status.eq(600) & term_frequency.eq(7) & bloom_version.eq(2) & days_past_due.gt(seven_day_product_rollover_bloom2),
        loan_status.eq(600) & term_frequency.eq(21) & days_past_due.gt(twenty_one_day_product_rollover),
        loan_status.eq(600) & term_frequency.eq(30) & days_past_due.gt(thirty_day_product_rollover),

        #loans that were cleared/closed after they got to default and were overpaid
        loan_status.eq(700) & term_frequency.eq(1) & days_past_due.gt(one_day_product_rollover),
        loan_status.eq(700) & term_frequency.eq(3) & days_past_due.gt(three_day_product_rollover),
        loan_status.eq(700) & term_frequency.eq(7) & bloom_version.eq(1) & days_past_due.gt(seven_day_product_rollover_bloom1),
        loan_status.eq(700) & term_frequency.eq(7) & bloom_version.eq(2) & days_past_due.gt(seven_day_product_rollover_bloom2),
        loan_status.eq(700) & term_frequency.eq(21) & days_past_due.gt(twenty_one_day_product_rollover),
        loan_status.eq(700) & term_frequency.eq(30) & days_past_due.gt(thirty_day_product_rollover),
    ]

    # Choices
    choices = [
        "written-off_default",
        
        "current_active",
        "closed_early_repayment",
        "closed_early_repayment_overpaid",
        "closed_on_time",
        "closed_on_time_overpaid",    
        
        "active_rollover",
        "active_rollover",
        "active_rollover",
        "active_rollover",
        "active_rollover",
        "active_rollover",
        
        "closed_rollover",
        "closed_rollover",
        "closed_rollover",
        "closed_rollover",
        "closed_rollover",
        "closed_rollover",    
        
        "closed_rollover_overpaid",
        "closed_rollover_overpaid",
        "closed_rollover_overpaid",
        "closed_rollover_overpaid",
        "closed_rollover_overpaid",
        "closed_rollover_overpaid",
        
        "active_default",
        "active_default",
        "active_default",
        "active_default",
        "active_default",
        "active_default",   
        
        "closed_default",
        "closed_default",
        "closed_default",
        "closed_default",
        "closed_default",
        "closed_default",        
        
        "closed_default_overpaid",
        "closed_default_overpaid",
        "closed_default_overpaid",
        "closed_default_overpaid",
        "closed_default_overpaid",
        "closed_default_overpaid",

    ]
    
    # Loan labels feature
    loan_labels = np.select(conditions, choices)
    
    return loan_labels


def lftsv_feature_engineering(config_path, extract_end_date):
    # Load configurations
    config = read_params(config_path)
    project_dir = config["project_dir"]
    lftsv_clean_data = config["processed_data_config"]["lftsv_clean_data_parquet"]
    engineered_features_path = config["interim_data_config"]["engineered_features_parquet"]
    td_summary_data_path = config["interim_data_config"]["td_summary_data_parquet"]

    # Load snapshot
    df = pd.read_parquet(project_dir + lftsv_clean_data)
    td_agg = pd.read_parquet(project_dir + td_summary_data_path)
    
    # Loan id + bloom version feature
    df["loan_id_product_concat"] = (df["loan_mifos_id"].astype("str") + "-" + df["bloom_version"].astype("str")).astype("str")

    # Drop all duplicated rows
    df = df.loc[~df["loan_id_product_concat"].duplicated()]

    # Sort dataframe based on specific columns
    df.sort_values(["client_mobile_number", "disbursed_on_date"], ascending=[True, False], inplace=True)

    # Loan count feature
    df["loan_count"] = df.groupby("store_number")["store_number"].transform('size')

    # Loan rank feature
    df["loan_rank"] = df.groupby("store_number")["disbursed_on_date"].rank(ascending=True)

    # Repayment vs principal feature
    df['total_repayment_vs_principal_amount'] = df['total_repayment'] / df['principal_disbursed']
    
#     df['expected_dpd5'] = df['end_rollover_date_fixed'] + np.timedelta64(5,'D')

    # Any bloom 2 one day feature
    df = df.merge(df[df['bloom_version'] == 2].groupby(['store_number']).agg({'term_frequency': lambda x: np.all(np.unique(x) == 1)}).rename(columns={'term_frequency': 'any_bloom2_1day'}), on='store_number', how='left')
    df['any_bloom2_1day'].fillna(False, inplace=True)
    print('\nAny bloom 2 one day feature sample:\n', df.loc[df['loan_id_product_concat'] == '308912-2.0', ['loan_id_product_concat', 'any_bloom2_1day']], sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')
    
    # Update Safaricom loan balance
    # df['safaricom_loan_balance'] = saf_loan_balance(df['total_outstanding'], df['safaricom_loan_balance'])

    # New vs repeat client feature
    # df['loan_count_flag'] = cal_loan_count_flag(df['loan_count'])
    
    # Left merge with transaction dimension dataset
    df = df.merge(td_agg, how="left", on=["loan_id_product_concat"])
    
    # Days past due feature
    df['days_past_due'] = calc_days_past_due(df['loan_status'], df['due_date_fixed'], df['closed_on_date'], df['max_transaction_date'], extract_end_date)
    print('\nDays past due feature sample:\n', df.loc[df['loan_id_product_concat'] == '308912-2.0', ['loan_id_product_concat', 'loan_status', 'due_date_fixed', 'closed_on_date', 'max_transaction_date', 'days_past_due']], sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')

    # Days past end rollover feature
    df['days_past_end_rollover'] = calc_days_past_end_rollover(df['loan_status'], df['end_rollover_date_fixed'], df['closed_on_date'], df['max_transaction_date'], extract_end_date)
    print('\nDays past end rollover feature sample:\n', df.loc[df['loan_id_product_concat'] == '308912-2.0', ['loan_id_product_concat', 'loan_status', 'end_rollover_date_fixed', 'closed_on_date', 'max_transaction_date', 'days_past_end_rollover']], sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')
    
    # Loan repayment feature
    df['loan_repayment_status'] = set_loan_status_labels(df['loan_status'], df['days_past_due'], df['term_frequency'], df['bloom_version'])
    print('\nLoan repayment feature sample:\n', df.loc[df['loan_id_product_concat'] == '308912-2.0', ['loan_id_product_concat', 'loan_status', 'loan_repayment_status']], sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')

    # Days to maturity feature
    df['days_diff_maturity_max_trans'] = (df['max_transaction_date'] - df['due_date_fixed']).dt.days
    print('\nDays to maturity feature sample:\n', df.loc[df['loan_id_product_concat'] == '308912-2.0', ['loan_id_product_concat', 'max_transaction_date', 'due_date_fixed', 'days_diff_maturity_max_trans']], sep='')
    print('---------------------------------------------------------------------------------------------------------------------------------------')

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
    df_lftsv_features = lftsv_feature_engineering(parsed_args.config, extract_end_date)
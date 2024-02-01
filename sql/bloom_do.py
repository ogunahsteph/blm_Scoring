# Functions
def td_sql(extract_end_date):
    """
    Define SQL script
    input: None
    output: SQL script string
    """
    
    return f"""
            SELECT
                td.mifos_loan_id
                ,td.is_reversed
                ,td.transaction_type_enum
                ,td.transaction_date
                ,td.bloom_version
                --,td.amount
                --,td.outstanding_loan_balance_derived 
            FROM UBUNTU.BLOOMLIVE.transactions_dimension td
            WHERE td.is_reversed is false
                AND td.transaction_type_enum = 2
                --AND td.payment_detail_id = 'MpesaRepayment'
                --AND td.bloom_version = 2
                AND td.transaction_date < '{extract_end_date}' 
                AND (td.receipt_number NOT LIKE 'waiv%%' OR td.receipt_number IS NULL)
            """


def lftsv_sql():
    """
    Define SQL script
    input: None
    output: SQL script string
    """
    
    return f"""
            SELECT
                lftsv.client_mifos_id
                ,lftsv.client_mobile_number
                ,lftsv.loan_status
                ,lftsv.loan_mifos_id
                ,lftsv.term_frequency
                ,lftsv.principal_disbursed
                ,lftsv.principal_repaid
                --,lftsv.principal_outstanding
                ,lftsv.interest_charged
                ,lftsv.interest_repaid
                --,lftsv.interest_outstanding
                ,lftsv.fee_charges_charged
                ,lftsv.fee_charges_repaid
                --,lftsv.fee_charges_outstanding
                ,lftsv.penalty_charges_charged
                ,lftsv.penalty_charges_repaid
                --,lftsv.penalty_charges_outstanding
                ,lftsv.total_expected_repayment
                ,lftsv.total_repayment
                ,lftsv.total_outstanding
                ,lftsv.safaricom_loan_balance
                ,lftsv.disbursed_on_date
                ,lftsv.expected_matured_on_date
                ,lftsv.closed_on_date
                ,lftsv.store_number
                ,lftsv.bloom_version
                ,lftsv.src_crdt_score
                ,lftsv.expected_matured_on_date AS due_date_fixed
                ,lftsv.end_rollvr_dt AS end_rollover_date_fixed
                --,lftsv.dpd_30 AS expected_dpd30
                --,lftsv.dpd_d60 AS expected_dpd60
                ,lftsv.dpd_d90 AS expected_dpd90
            --FROM UBUNTU.BLOOMLIVE.loans_fact_table_summary_view lftsv
            FROM UBUNTU.BLOOMLIVE.loans_fact_table_materialized_summary_view lftsv
            WHERE lftsv.loan_status IN (200, 300, 600, 601, 602, 700)
            --WHERE lftsv.loan_status NOT IN (0, 100, 400, 500)
            """


def metabase_sql():
    """
    Define SQL script
    input: None
    output: SQL script string
    """
    
    return f"""
            SELECT
                tamv.store_number
                ,tamv.phone
                ,tamv.transaction_id
                ,tamv.transaction_time
            --FROM METABASE_BCK.ASANTE.payments tamv
            --FROM UBUNTU.BLOOMLIVE.till_activity_dimension tamv
            FROM UBUNTU.BLOOMLIVE.till_activity_materialized_view tamv
            WHERE tamv."type" = 'c2b'
                AND tamv.transaction_time::DATE BETWEEN NOW()::DATE - 31 AND NOW()::DATE - 1
                --AND tamv.transaction_time::DATE BETWEEN NOW()::DATE - 91 AND NOW()::DATE - 1
                --AND tamv.created_at::DATE BETWEEN NOW()::DATE - 91 AND NOW()::DATE - 1
                --AND tamv.created_at::DATE BETWEEN NOW()::DATE - INTERVAL '91 DAYS' AND NOW()::DATE - INTERVAL '1 DAY'
            """


def metabase_trxn_sql():
    """
    Define SQL script
    input: None
    output: SQL script string
    """
    
    return f"""
            SELECT
                tamv.store_number
                ,MIN(tamv.transaction_time::DATE) AS most_recent_trx_date_past_30_days
                ,MAX(tamv.transaction_time::DATE) AS last_trx_date
                ,COUNT(DISTINCT tamv.transaction_time::DATE) AS actual_trx_days
            --FROM METABASE_BCK.ASANTE.payments tamv
            FROM UBUNTU.BLOOMLIVE.till_activity_dimension tamv
            --FROM UBUNTU.BLOOMLIVE.till_activity_materialized_view tamv
            WHERE tamv."type" = 'c2b'
                AND tamv.transaction_time::DATE BETWEEN NOW()::DATE - 30 AND NOW()::DATE - 1
            GROUP BY tamv.store_number
            """


def metabase_amount_sql():
    """
    Define SQL script
    input: None
    output: SQL script string
    """
    
    return f"""
            SELECT
                tamv.store_number
                ,SUM(tamv.amount) / 3 AS approx_30_days_trx_val
            FROM UBUNTU.BLOOMLIVE.till_activity_dimension tamv
            --FROM UBUNTU.BLOOMLIVE.till_activity_materialized_view tamv
            WHERE tamv."type" = 'c2b'
                AND tamv.transaction_time::DATE BETWEEN NOW()::DATE - 90 AND NOW()::DATE - 1
            GROUP BY tamv.store_number
            """


def bcsv_sql():
    """
    Define SQL script
    input: None
    output: SQL script string
    """
    
    return f"""
            SELECT
                bcsv.store_number
                ,bcsv.national_id
                ,bcsv.is_iprs_validated
                ,bcsv.mobile_number
            FROM UBUNTU.BLOOMLIVE.client_summary_view bcsv
            --WHERE bcsv.is_iprs_validated IS true
            """


def idm_sql():
    """
    Define SQL script
    input: None
    output: SQL script string
    """
    
    return f"""
            SELECT
                irv.idnumber AS national_id
                ,irv.recommendeddecision AS idm_recommendation
                ,irv.creditlimit30days AS idm_limit
            FROM UBUNTU.BLOOMLIVE.idm_refresh_view irv
            """


def blacklist_sql():
    """
    Define SQL script
    input: None
    output: SQL script string
    """
    
    return f"""
            SELECT
                dd.account_number AS store_number
                ,dd.customer_id_number AS national_id
            FROM UBUNTU.BLOOMLIVE.defaulters_dimension dd
            WHERE dd.file_date = (SELECT 
                                        MAX(dd.file_date) 
                                    FROM UBUNTU.BLOOMLIVE.defaulters_dimension dd)
            """


def sr_21_limits_sql():
    """
    Define SQL script
    input: None
    output: SQL script string
    """
    
    return f"""
            SELECT
                sr.store_number
                ,SUM(sr.final_21_limit) AS total_final_21_limit
                ,SUM(sr.final_1_limit) AS total_final_1_limit
                ,SUM(sr.final_7_limit) AS total_final_7_limit
            FROM UBUNTU.BLOOMLIVE.scoring_results sr
            GROUP BY sr.store_number
            """


def sr_latest_refresh_limits_sql():
    """
    Define SQL script
    input: None
    output: SQL script string
    """
    
    return f"""
            SELECT 
                CASE 
                    WHEN sr_prev.store_number IS NULL THEN sr_630.store_number 
                    ELSE sr_prev.store_number
                END AS store_number
                ,sr_prev.previous_21_limit
                ,sr_prev.previous_7_limit
                ,sr_prev.previous_1_limit
                ,sr_630.model_630_21_limit
                ,sr_630.model_630_7_limit
                ,sr_630.model_630_1_limit
            FROM (SELECT
                        sr_prev.store_number
                        ,sr_prev.final_21_limit AS previous_21_limit
                        ,sr_prev.final_7_limit AS previous_7_limit
                        ,sr_prev.final_1_limit AS previous_1_limit
                    FROM UBUNTU.BLOOMLIVE.scoring_results sr_prev
                    WHERE sr_prev.model_version = (SELECT
                                                        MAX(sr.model_version) 
                                                    FROM UBUNTU.BLOOMLIVE.scoring_results sr)
                    /*WHERE sr_prev.model_version = '2022-007[2022-10-21, 2022-10-28]'*/) sr_prev
            FULL OUTER JOIN (SELECT
                                    sr_630.store_number 
                                    ,sr_630.final_21_limit AS model_630_21_limit
                                    ,sr_630.final_7_limit AS model_630_7_limit
                                    ,sr_630.final_1_limit AS model_630_1_limit
                                FROM UBUNTU.BLOOMLIVE.scoring_results AS sr_630
                                WHERE sr_630.model_version = '2022-004[2022-05-14, 2022-06-30]') sr_630	
                ON sr_prev.store_number = sr_630.store_number
            """


def sr_rein_last_limits_sql():
    """
    Define SQL script
    input: None
    output: SQL script string
    """
    
    return f"""
            SELECT 
                sr_rein.store_number
                ,sr_rein.final_7_limit AS rein_7_limit
                ,lftmsv.loan_mifos_id
            FROM UBUNTU.BLOOMLIVE.scoring_results sr_rein
            INNER JOIN UBUNTU.BLOOMLIVE.loans_fact_table_materialized_summary_view lftmsv
                ON sr_rein.store_number = lftmsv.store_number::text 
                AND sr_rein.model_version = lftmsv.model_version 
                AND lftmsv.bloom_version = 2
            """


def rmv_sql():
    """
    Define SQL script
    input: None
    output: SQL script string
    """
    
    return f"""
            SELECT
                --rmv.loan_surrogate_id	
                rmv.loan_mifos_id	
                --,rmv.bloom_version
                --,rmv.principal_disbursed
                ,rmv.repayment_amount_by_due_date	
                ,rmv.repayment_amount_by_rllvr_date	
                --,rmv.repayment_amount_by_dpd30	
                --,rmv.repayment_amount_by_dpd60	
                ,rmv.repayment_amount_by_dpd90
                --,rmv.repayment_amount_by_due_date / rmv.principal_disbursed AS repayments_vs_principal
            FROM UBUNTU.BLOOMLIVE.repayments_milestones_view rmv 
            WHERE rmv.bloom_version = 2
            """


# def rmv_sql():
#     """
#     Define SQL script
#     input: None
#     output: SQL script string
#     """
    
#     return f"""
#             select table1.loan_mifos_id, table1.repayment_amount_by_due_date, table1.repayment_amount_by_rllvr_date, table2.repayment_amount_by_dpd5, table1.repayment_amount_by_dpd90 
#             from
#             (
#             select rmv2.loan_mifos_id, rmv2.repayment_amount_by_due_date, rmv2.repayment_amount_by_rllvr_date, rmv2.repayment_amount_by_dpd90 
#             from bloomlive.repayments_milestones_view rmv2
#             where rmv2.bloom_version = 2
#             ) table1
#             left join 
#             (
#             select distinct rmv.loan_mifos_id , sum(td.amount) as repayment_amount_by_dpd5
#             from bloomlive.repayments_milestones_view rmv
#             left join bloomlive.transactions_dimension td on rmv.loan_mifos_id = CAST(td.mifos_loan_id as int) and rmv.bloom_version = td.bloom_version 
#             where td.transaction_type_enum = 2 and td.is_reversed is false
#             and rmv.bloom_version = 2 and td.bloom_version = 2
#             and td.transaction_date  <= rmv.end_rollvr_dt + interval '5' day
#             group by rmv.loan_mifos_id
#             ) table2
#             on table2.loan_mifos_id = table1.loan_mifos_id
#             """


def ftd_sql():
    """
    Define SQL script
    input: None
    output: SQL script string
    """
    
    return f"""
            SELECT
                lrd.store_number 
                ,lrd.update_flag AS ftd_update_flag
                ,lrd.reinstatement_reason 
            FROM UBUNTU.BLOOMLIVE.limit_reinstatement_dimension lrd 
            WHERE lrd.reinstatement_reason = 'first time defaulters during election risk mitigation measures'
                AND lrd.update_flag = 1
            """


def ftd_lftsv_sql():
    """
    Define SQL script
    input: None
    output: SQL script string
    """
    
    return f"""
            SELECT
                lftsv.client_mifos_id
                ,lftsv.client_mobile_number
                ,lftsv.loan_status
                ,lftsv.loan_mifos_id
                ,lftsv.term_frequency
                ,lftsv.principal_disbursed
                ,lftsv.principal_repaid
                --,lftsv.principal_outstanding
                ,lftsv.interest_charged
                ,lftsv.interest_repaid
                --,lftsv.interest_outstanding
                ,lftsv.fee_charges_charged
                ,lftsv.fee_charges_repaid
                --,lftsv.fee_charges_outstanding
                ,lftsv.penalty_charges_charged
                ,lftsv.penalty_charges_repaid
                --,lftsv.penalty_charges_outstanding
                ,lftsv.total_expected_repayment
                ,lftsv.total_repayment
                ,lftsv.total_outstanding
                ,lftsv.safaricom_loan_balance
                ,lftsv.safaricom_loan_balance_date 
                ,lftsv.disbursed_on_date
                ,lftsv.expected_matured_on_date
                ,lftsv.closed_on_date
                ,lftsv.store_number
                ,lftsv.bloom_version
                ,lftsv.src_crdt_score
                ,lftsv.expected_matured_on_date AS due_date_fixed
                ,lftsv.end_rollvr_dt AS end_rollover_date_fixed
                --,lftsv.dpd_30 AS expected_dpd30
                --,lftsv.dpd_d60 AS expected_dpd60
                ,lftsv.dpd_d90 AS expected_dpd90
            FROM UBUNTU.BLOOMLIVE.loans_fact_table_materialized_summary_view lftsv
            WHERE lftsv.loan_mifos_id IN (
                    168842, 171762, 172647, 173578, 178361, 156467, 173055, 173132,
                    175911, 175951, 177310, 178196, 178318, 179599, 179564, 180342,
                    151040, 152329, 151774, 155671, 156274, 160931, 161611, 179756,
                    179565, 180530, 181009, 180594, 181553, 181546, 181661, 181963,
                    181879, 182974, 182084, 183370, 183185, 182932, 184282, 183243,
                    184403, 184389)
                AND lftsv.bloom_version = 2
            """


def ftd_lftsv_behaviour_sql():
    """
    Define SQL script
    input: None
    output: SQL script string
    """
    
    return f"""
            SELECT
                lftsv_future.client_mifos_id
                ,lftsv_future.client_mobile_number
                ,lftsv_future.loan_status
                ,lftsv_future.loan_mifos_id
                ,lftsv_future.term_frequency
                ,lftsv_future.principal_disbursed
                ,lftsv_snapshot.disbursed_on_date AS disbursed_on_date_snapshot
	            ,(lftsv_future.disbursed_on_date - lftsv_snapshot.disbursed_on_date) AS date_diff
                ,lftsv_future.principal_repaid
                --,lftsv_future.principal_outstanding
                ,lftsv_future.interest_charged
                ,lftsv_future.interest_repaid
                --,lftsv_future.interest_outstanding
                ,lftsv_future.fee_charges_charged
                ,lftsv_future.fee_charges_repaid
                --,lftsv_future.fee_charges_outstanding
                ,lftsv_future.penalty_charges_charged
                ,lftsv_future.penalty_charges_repaid
                --,lftsv_future.penalty_charges_outstanding
                ,lftsv_future.total_expected_repayment
                ,lftsv_future.total_repayment
                ,lftsv_future.total_outstanding
                ,lftsv_future.safaricom_loan_balance
                ,lftsv_future.safaricom_loan_balance_date 
                ,lftsv_future.disbursed_on_date
                ,lftsv_future.expected_matured_on_date
                ,lftsv_future.closed_on_date
                ,lftsv_future.store_number
                ,lftsv_future.bloom_version
                ,lftsv_future.src_crdt_score
                ,lftsv_future.expected_matured_on_date AS due_date_fixed
                ,lftsv_future.end_rollvr_dt AS end_rollover_date_fixed
                --,lftsv_future.dpd_30 AS expected_dpd30
                --,lftsv_future.dpd_d60 AS expected_dpd60
                ,lftsv_future.dpd_d90 AS expected_dpd90
            FROM UBUNTU.BLOOMLIVE.loans_fact_table_materialized_summary_view lftsv_future
            RIGHT JOIN (
                        SELECT
                            lftsv.store_number
                            ,lftsv.loan_mifos_id
                            ,lftsv.disbursed_on_date
                        FROM UBUNTU.BLOOMLIVE.loans_fact_table_materialized_summary_view lftsv
                        WHERE lftsv.loan_mifos_id IN (
                                168842, 171762, 172647, 173578, 178361, 156467, 173055, 173132,
                                175911, 175951, 177310, 178196, 178318, 179599, 179564, 180342,
                                151040, 152329, 151774, 155671, 156274, 160931, 161611, 179756,
                                179565, 180530, 181009, 180594, 181553, 181546, 181661, 181963,
                                181879, 182974, 182084, 183370, 183185, 182932, 184282, 183243,
                                184403, 184389)
                            AND lftsv.bloom_version = 2
                            AND lftsv.store_number IN ('661914', '7392614', '7237719', '565405', '7275089')
                        ) lftsv_snapshot
                ON lftsv_future.store_number = lftsv_snapshot.store_number
                    AND lftsv_future.disbursed_on_date > lftsv_snapshot.disbursed_on_date
            ORDER BY 
                lftsv_future.store_number
                ,lftsv_future.disbursed_on_date
            """


def lrr_sql():
    """
    Define SQL script
    input: None
    output: SQL script string
    """
    
    return f"""
            SELECT
                lrd.store_number 
                ,lrd.update_flag AS lrr_update_flag
                ,lrd.reinstatement_reason 
            FROM UBUNTU.BLOOMLIVE.limit_reinstatement_dimension lrd 
            WHERE lrd.reinstatement_reason = 'limit review request'
                AND lrd.update_flag = 1
            """


def rein_sql():
    """
    Define SQL script
    input: None
    output: SQL script string
    """
    
    return f"""
            SELECT
                lrd.store_number 
                ,lrd.update_flag AS rein_update_flag
                ,lrd.reinstatement_reason 
            FROM UBUNTU.BLOOMLIVE.limit_reinstatement_dimension lrd 
            WHERE lrd.reinstatement_reason = 'Limit reinstatement traction >=25'
                AND lrd.update_flag = 1
            """


def validate_push():
    """
    Define SQL script
    input: None
    output: SQL script string
    """

    return """
            SELECT
                MAX({0}.model_version) AS latest_model_version
                ,COUNT({0}.store_number) AS num_of_customers_who_have_been_scored
                ,SUM({0}.final_21_limit) AS gross_limit_allocation_for_21_day
                ,SUM({0}.final_7_limit) AS gross_limit_allocation_for_7_day
                ,SUM({0}.final_1_limit) AS gross_limit_allocation_for_1_day
            FROM UBUNTU.{1} {0}
            WHERE {0}.model_version = (SELECT 
                                            MAX({0}.model_version) 
                                        FROM UBUNTU.{1} {0})
            """
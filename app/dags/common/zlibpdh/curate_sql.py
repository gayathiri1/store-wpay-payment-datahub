class CurateSqls():
    @staticmethod
    def get_curation_sql(table_name):
        curate_sql_dict = {
                            "gfs_pdh_txn_interface": "Insert into pdh_rd_data_navigator.gfs_pdh_txn_interface  SELECT SAFE.PARSE_DATE(\'%Y%m%d\',\'{{ file_date }}\') as file_date, \'{{ file_name }}\' as file_name,  SAFE.PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%S\',\'{{ pdh_load_time }}\') as pdh_load_time, \'{{ payload_id }}\' as payload_id,h,tran_uid,SAFE.PARSE_TIMESTAMP(\'%Y%m%d%H%M%S\',substr(tstamp_trans,1,length(tstamp_trans)-2)) as tstamp_trans  ,SAFE.PARSE_TIMESTAMP(\'%Y%m%d%H%M%S\',tstamp_local) as tstamp_local,masked_pan,SAFE.PARSE_DATE(\'%Y%m%d\',date_recon_acq) as date_recon_acq,  net_term_id,rpt_lvl_id_b,mti,tran_type_id,SAFE_CAST(amt_recon_net as NUMERIC) as amt_recon_net,  act_code,response_code,account_type,SAFE_CAST(transaction_total as NUMERIC) as transaction_total,  SAFE_CAST(purchase_total as NUMERIC) as purchase_total,SAFE_CAST(cashout_total as NUMERIC) as cashout_total, retrieval_ref_no,SAFE_CAST(stan as NUMERIC) as stan,  SAFE_CAST(original_stan as NUMERIC) as original_stan,proc_id_acq_b,inst_id_recn_acq_b,card_acpt_name_loc,proc_id_iss_b,  issuer_institution_id,card_owner,pos_crd_dat_in_mod,pos_crdhldr_presnt,order_number,approval_code,  adtnl_response_data,card_acpt_id,merchant_store_id,dom_int_ind,card_type,card_subtype,bin_product_code,scheme,SAFE_CAST(surcharge_amount as NUMERIC) as surcharge_amount,SAFE_CAST(tip_amount as NUMERIC) as tip_amount,SAFE_CAST(inst_id_recon_iss as NUMERIC) as inst_id_recon_iss,inst_id_recon_iss_name,SAFE_CAST(card_acceptor_business_code as NUMERIC) as card_acceptor_business_code,additional_data_acquirer,SAFE_CAST(currency_code as NUMERIC) as currency_code, pymt_acct_ref,adl_data_national,adl_data_priv_acq,wallet_id,scdr_token from {{ staging_table }};",
                            "gfs_pdh_txn_interface_hist": "Insert into pdh_rd_data_navigator.gfs_pdh_txn_interface_hist  SELECT SAFE.PARSE_DATE(\'%Y%m%d\',\'{{ file_date }}\') as file_date, \'{{ file_name }}\' as file_name,  SAFE.PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%S\',\'{{ pdh_load_time }}\') as pdh_load_time, \'{{ payload_id }}\' as payload_id,h,tran_uid,SAFE.PARSE_TIMESTAMP(\'%Y%m%d%H%M%S\',substr(tstamp_trans,1,length(tstamp_trans)-2)) as tstamp_trans  ,SAFE.PARSE_TIMESTAMP(\'%Y%m%d%H%M%S\',tstamp_local) as tstamp_local,masked_pan,SAFE.PARSE_DATE(\'%Y%m%d\',date_recon_acq) as date_recon_acq,  net_term_id,rpt_lvl_id_b,mti,tran_type_id,SAFE_CAST(amt_recon_net as NUMERIC) as amt_recon_net,  act_code,response_code,account_type,SAFE_CAST(transaction_total as NUMERIC) as transaction_total,  SAFE_CAST(purchase_total as NUMERIC) as purchase_total,SAFE_CAST(cashout_total as NUMERIC) as cashout_total, retrieval_ref_no,SAFE_CAST(stan as NUMERIC) as stan,  SAFE_CAST(original_stan as NUMERIC) as original_stan,proc_id_acq_b,inst_id_recn_acq_b,card_acpt_name_loc,proc_id_iss_b,  issuer_institution_id,card_owner,pos_crd_dat_in_mod,pos_crdhldr_presnt,order_number,approval_code,  adtnl_response_data,card_acpt_id,merchant_store_id,dom_int_ind,card_type,card_subtype,bin_product_code,scheme,SAFE_CAST(surcharge_amount as NUMERIC) as surcharge_amount,SAFE_CAST(tip_amount as NUMERIC) as tip_amount,SAFE_CAST(inst_id_recon_iss as NUMERIC) as inst_id_recon_iss,inst_id_recon_iss_name,SAFE_CAST(card_acceptor_business_code as NUMERIC) as card_acceptor_business_code,additional_data_acquirer,SAFE_CAST(currency_code as NUMERIC) as currency_code, pymt_acct_ref,adl_data_national,adl_data_priv_acq,wallet_id,scdr_token from {{ staging_table }};",
                            "gfs_aux_visa": "Insert into pdh_rd_data_navigator.gfs_aux_visa SELECT SAFE.PARSE_DATE(\'%Y%m%d\',\'{{ file_date }}\') as file_date, \'{{ file_name }}\' as file_name,SAFE.PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%S\',\'{{ pdh_load_time }}\') as pdh_load_time, \'{{ payload_id }}\' as payload_id,h,mti,tran_disposition,issuer, pan,SAFE_CAST(tran_amt as NUMERIC) as tran_amt,SAFE.PARSE_TIMESTAMP('%Y%m%d%H%M%S',tstamp_trans) as tstamp_trans, SAFE_CAST(stan as NUMERIC) as stan,SAFE.PARSE_TIMESTAMP('%Y%m%d%H%M%S',tstamp_local) as tstamp_local,SAFE.PARSE_DATE('%Y%m%d',date_recon) as date_recon, rrn,card_acpt_term_id,SAFE_CAST(interchange_fee as NUMERIC) as interchange_fee,domestic_or_international,acct_funding_source,processing_code,moto_indicator  from {{ staging_table }};",
                            "gfs_aux_eftpos": "Insert into pdh_rd_data_navigator.gfs_aux_eftpos SELECT SAFE.PARSE_DATE(\'%Y%m%d\',\'{{ file_date }}\') as file_date, \'{{ file_name }}\' as file_name,SAFE.PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%S\',\'{{ pdh_load_time }}\') as pdh_load_time, \'{{ payload_id }}\' as payload_id,mti,tran_disposition,issuer, pan,SAFE_CAST(tran_amt as NUMERIC) as tran_amt,SAFE.PARSE_TIMESTAMP(\'%Y%m%d%H%M%S\',substr(tstamp_trans,1,length(tstamp_trans)-2)) as tstamp_trans, SAFE_CAST(stan as NUMERIC) as stan,SAFE.PARSE_TIMESTAMP('%Y%m%d%H%M%S',tstamp_local) as tstamp_local,SAFE.PARSE_DATE('%Y%m%d',date_recon) as date_recon, rrn,card_acpt_term_id,SAFE_CAST(issuer_interchange_amt as NUMERIC) as issuer_interchange_amt,SAFE_CAST(acquirer_interchange_amt as NUMERIC) as acquirer_interchange_amt,SAFE_CAST(scheme_fee as NUMERIC) as scheme_fee from {{ staging_table }};",
                            "gfs_merfee": "Insert into pdh_rd_data_navigator.gfs_merfee SELECT SAFE.PARSE_DATE(\'%Y%m%d\',\'{{ file_date }}\') as file_date, \'{{ file_name }}\' as file_name,SAFE.PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%S\',\'{{ pdh_load_time }}\') as pdh_load_time, \'{{ payload_id }}\' as payload_id,record_type,merchant,wow_store_no,merchant_store_no,card_present_indicator,scheme,transaction_type,approval_status,safe_cast(total_amount as numeric),safe_cast(total_count as numeric) from {{ staging_table }};",
                            "digitalpay_gc_txns": "Insert into pdh_rd_data_navigator.digitalpay_gc_txns SELECT SAFE.PARSE_DATE(\'%Y%m%d\',\'{{ file_date }}\') as file_date, \'{{ file_name }}\' as file_name,SAFE.PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%S\',\'{{ pdh_load_time }}\') as pdh_load_time, \'{{ payload_id }}\' as payload_id,SAFE_CAST(containerRef as NUMERIC) as containerRef, SAFE.PARSE_TIMESTAMP(\'%m/%d/%Y %H:%M:%S\',dateWeekStart) as dateWeekStart,SAFE.PARSE_TIMESTAMP(\'%m/%d/%Y %H:%M:%S\',dateWeekEnd) as dateWeekEnd,SAFE.PARSE_TIMESTAMP(\'%m/%d/%Y %H:%M:%S\',txnTime) as txnTime,txnRef,SAFE_CAST(applicationId as NUMERIC) as applicationId,MerchantName,SAFE_CAST(userId as NUMERIC) as userId,username,userStatus,SAFE_CAST(itemId as NUMERIC) as itemId,SAFE_CAST(clientId as NUMERIC) as clientId,SAFE_CAST(containerId as NUMERIC) as containerId,containerStatus,responseCodeContainer,responseTextContainer,transactionType,bin,cardSuffix,SAFE_CAST(amount as NUMERIC) as amount,responseCode,responseText,SAFE_CAST(duration as NUMERIC) as duration,orderNumber,customerRef,authCode,stan,originalTxnRef,originalTxnLogRef,parentTxnRef,schemeName,itemType,itemStatus,SAFE.PARSE_TIMESTAMP(\'%m/%d/%Y %H:%M:%S\',itemLastUsed) as itemLastUsed,SAFE.PARSE_TIMESTAMP(\'%m/%d/%Y %H:%M:%S\',itemLastUpdated) as itemLastUpdated,SAFE_CAST(itemPriority as NUMERIC) as itemPriority,token,fraudRequestId,fraudDecision,fraudReasonCd,ipAddress,gateway,comment,applicationRef,SAFE.PARSE_TIMESTAMP(\'%m/%d/%Y %H:%M:%S\',itemCreatedOn) as itemCreatedOn,item_token,StoreId,RRN,DeviceId,SAFE.PARSE_TIMESTAMP(\'%m/%d/%Y %H:%M:%S\',storeTimeStamp) as storeTimeStamp,terminal_id,rrn_controlblock from {{ staging_table }};",
                            "sap_hybris_qc_item_level_report": "Insert into pdh_rd_quickcilver.sap_hybris_qc_item_level_report SELECT SAFE.PARSE_DATE(\'%Y%m%d\',\'{{ file_date }}\') as file_date, \'{{ file_name }}\' as file_name,SAFE.PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%S\',\'{{ pdh_load_time }}\') as pdh_load_time, \'{{ payload_id }}\' as payload_id, card_no, order_no, invoice_no, SAFE.PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%S\',order_date) as order_date, SAFE.PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%S\',order_completion_date) as order_completion_date, payment_method, customer_no, gc_id, gc_name, SAFE_CAST(gc_face_value as NUMERIC) as gc_face_value,SAFE_CAST(gc_sale_value as NUMERIC) as gc_sale_value, promotion_id, discount_percent, SAFE.PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%S\',promotion_start_date) as promotion_start_date, SAFE.PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%S\',promotion_end_date) as promotion_end_date, retailer, sales_merchant, account_id,customer_id,email,billing_state,usertype,b2b_unit,SAFE_CAST(delivery_fee as NUMERIC) as delivery_fee,SAFE_CAST(donation as NUMERIC) as donation,sales_application from {{ staging_table }};",
                            "qc_partner_summary": "Insert into pdh_rd_quickcilver.qc_partner_summary SELECT SAFE.PARSE_DATE(\'%Y%m%d\',\'{{ file_date }}\') as file_date, \'{{ file_name }}\' as file_name,SAFE.PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%S\',\'{{ pdh_load_time }}\') as pdh_load_time, \'{{ payload_id }}\' as payload_id, SAFE.PARSE_TIMESTAMP(\'%d/%m/%Y %H:%M:%S\',DateFromAEST) as DateFromAEST,SAFE.PARSE_TIMESTAMP(\'%d/%m/%Y %H:%M:%S\',DateToAEST) as DateToAEST,Merchant,SAFE_CAST(MID as NUMERIC) as MID,TransactionType,SAFE_CAST(GrossSalesValue as NUMERIC) as GrossSalesValue,SAFE_CAST(Discount as NUMERIC) as Discount,SAFE_CAST(NetSalesValue as NUMERIC) as NetSalesValue,SAFE_CAST(SalesCount as NUMERIC) as SalesCount,BatchNumber from {{ staging_table }};",
                            "qc_partner_detail": "Insert into pdh_rd_quickcilver.qc_partner_detail SELECT SAFE.PARSE_DATE(\'%Y%m%d\',\'{{ file_date }}\') as file_date, \'{{ file_name }}\' as file_name,SAFE.PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%S\',\'{{ pdh_load_time }}\') as pdh_load_time, \'{{ payload_id }}\' as payload_id, SAFE.PARSE_TIMESTAMP(\'%d/%m/%Y %H:%M:%S\',DateFrom) as DateFrom, SAFE.PARSE_TIMESTAMP(\'%d/%m/%Y %H:%M:%S\',DateTo) as DateTo, Merchant, OutletCode, ProgramGroup, CardNumber, DesignCode, InvoiceNumber, ReferenceNumber, SAFE_CAST(Amount as NUMERIC) as Amount, TransactionType, SAFE.PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%S\', TransactionDate) as TransactionDate, Discount, SAFE_CAST(DiscountValue as NUMERIC) as DiscountValue, SAFE_CAST(NetValue as NUMERIC) as NetValue,BatchNumber from {{ staging_table }};",
                            "alipay_linkly": "Insert into pdh_rd_apm.alipay_linkly SELECT SAFE.PARSE_DATE(\'%Y%m%d\',\'{{ file_date }}\') as file_date, \'{{ file_name }}\' as file_name,SAFE.PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%S\',\'{{ pdh_load_time }}\') as pdh_load_time, \'{{ payload_id }}\' as payload_id, SAFE_CAST(ReqMT as NUMERIC) as ReqMT,ReqType,SAFE_CAST(ReqSTAN as NUMERIC) as ReqSTAN,ReqUID,SAFE_CAST(ReqPAN as NUMERIC) as ReqPAN,ReqTxRef,SAFE_CAST(ReqAmt as NUMERIC),SAFE_CAST(ReqTotalAmt as NUMERIC),ReqStore, SAFE.PARSE_DATE(\'%Y-%m-%d\',ReqDate) as ReqDate,SAFE.PARSE_TIME(\'%H:%M:%E*S\',ReqTime) as ReqTime,SAFE.PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%E*S\',ReqTT) as ReqTT,ResRef,ResRC,ResHRC,ResRT,ResHRT,TerminalID,Reversed,OrigData from {{ staging_table }};",
                            "alipay_stlm": "Insert into pdh_rd_apm.alipay_stlm SELECT SAFE.PARSE_DATE(\'%Y%m%d\',\'{{ file_date }}\') as file_date, \'{{ file_name }}\' as file_name,SAFE.PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%S\',\'{{ pdh_load_time }}\') as pdh_load_time, \'{{ payload_id }}\' as payload_id,Partner_transaction_id,SAFE_CAST(Transaction_id as NUMERIC) as Transaction_id,SAFE_CAST(Amount as NUMERIC) as Amount,SAFE_CAST(Rmb_amount as NUMERIC) as Rmb_amount,SAFE_CAST(Fee as NUMERIC) as Fee,SAFE_CAST(Settlement as NUMERIC) as Settlement,SAFE_CAST(Rmb_settlement as NUMERIC) as Rmb_settlement,Currency,SAFE_CAST(Rate as NUMERIC) as Rate,SAFE.PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%E*S\',Payment_time) as Payment_time,SAFE.PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%E*S\',Settlement_time) as Settlement_time,Type,Status,Remarks,SAFE_CAST(Secondary_merchant_industry as NUMERIC) as Secondary_merchant_industry,Secondary_merchant_name,Secondary_merchant_id,Store_id ,Store_Name ,Terminal_id ,	Operator_name ,Order_scene ,Trans_currency,SAFE_CAST(Trans_amount as NUMERIC) as Trans_amount ,SAFE_CAST(Trans_forex_rate as NUMERIC) as Trans_forex_rate ,SAFE_CAST(m_discount_amount as NUMERIC) as m_discount_amount  from {{ staging_table }};",
                            "chargebacks": "Insert into pdh_rd_data_navigator.chargebacks SELECT SAFE.PARSE_DATE(\'%Y%m%d\',\'{{ file_date }}\') as file_date, \'{{ file_name }}\' as file_name, SAFE.PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%S\',\'{{ pdh_load_time }}\') as pdh_load_time, \'{{ payload_id }}\' as payload_id, SAFE_CAST(id as NUMERIC) as id, region, SAFE.PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%S\',received_dt) as received_dt, bank_ref_no, store_id, store_name, SAFE.PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%S\',txn_dttm_local) as txn_dttm_local, SAFE_CAST(amount as NUMERIC) as amount, bank, rrn, reason, SAFE_CAST(lane as NUMERIC) as lane, status_open, SAFE.PARSE_TIMESTAMP(\'%m/%d/%Y %H:%M:%S\', closed_utc) as closed_utc, SAFE.PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%S\', rr_sent_dttm_utc) as rr_sent_dttm_utc, SAFE.PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%S\', rr_resent_dttm_utc) as rr_resent_dttm_utc, SAFE.PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%S\', response_dttm_utc) as response_dttm_utc, SAFE.PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%S\', response_2_dttm_utc) as response_2_dttm_utc, SAFE.PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%S\', charged_dt) as charged_dt, SAFE.PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%S\', chgbk_challenge_dt) as chgbk_challenge_dt, SAFE.PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%S\', chgbk_rev_dt) as chgbk_rev_dt, signature, orderno, dn_completion_pos_crd_dat_in_mod, all_comments from {{ staging_table }};",
                            "gfs_pdh_txn_interface_super": "INSERT INTO pdh_rd_data_navigator.gfs_pdh_txn_interface_super SELECT SAFE.PARSE_DATE(\'%Y%m%d\',\'{{ file_date }}\') as file_date, \'{{ file_name }}\' as file_name, SAFE.PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%S\',\'{{ pdh_load_time }}\') as pdh_load_time, \'{{ payload_id }}\' as payload_id, h, tran_uid, SAFE.PARSE_TIMESTAMP(\'%Y%m%d%H%M%S\', SAFE.SUBSTR(tstamp_trans, 0, 14)) AS tstamp_trans, SAFE.PARSE_TIMESTAMP(\'%Y%m%d%H%M%S\', tstamp_local) AS tstamp_local, masked_pan, SAFE.PARSE_DATE(\'%Y%m%d\', date_recon_acq) AS date_recon_acq, net_term_id, rpt_lvl_id_b, mti, tran_type_id, SAFE_CAST(amt_recon_net AS NUMERIC) AS amt_recon_net, act_code, response_code, account_type, SAFE_CAST(transaction_total AS NUMERIC) AS transaction_total, SAFE_CAST(purchase_total AS NUMERIC) AS purchase_total, SAFE_CAST(cashout_total AS NUMERIC) AS cashout_total, retrieval_ref_no, SAFE_CAST(stan AS NUMERIC) AS stan, SAFE_CAST(original_stan AS NUMERIC) AS original_stan, proc_id_acq_b, inst_id_recn_acq_b, card_acpt_name_loc, proc_id_iss_b, issuer_institution_id, card_owner, pos_crd_dat_in_mod, pos_crdhldr_presnt, order_number, approval_code, adtnl_response_data, card_acpt_id, merchant_store_id, dom_int_ind, card_type, card_subtype, bin_product_code, scheme, SAFE_CAST(surcharge_amount AS NUMERIC) AS surcharge_amount, SAFE_CAST(tip_amount AS NUMERIC) AS tip_amount, SAFE_CAST(inst_id_recon_iss AS NUMERIC) AS inst_id_recon_iss, inst_id_recon_iss_name, SAFE_CAST(card_acceptor_business_code AS NUMERIC) AS card_acceptor_business_code, additional_data_acquirer, SAFE_CAST(currency_code AS NUMERIC) AS currency_code, pymt_acct_ref, adl_data_national, adl_data_priv_acq, wallet_id, scdr_token FROM {{ staging_table }};",
                            "gfs_pdh_txn_interface_super_hist": "INSERT INTO pdh_rd_data_navigator.gfs_pdh_txn_interface_super_hist SELECT SAFE.PARSE_DATE(\'%Y%m%d\',\'{{ file_date }}\') as file_date, \'{{ file_name }}\' as file_name, SAFE.PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%S\',\'{{ pdh_load_time }}\') as pdh_load_time, \'{{ payload_id }}\' as payload_id, h, tran_uid, SAFE.PARSE_TIMESTAMP(\'%Y%m%d%H%M%S\', SAFE.SUBSTR(tstamp_trans, 0, 14)) AS tstamp_trans, SAFE.PARSE_TIMESTAMP(\'%Y%m%d%H%M%S\', tstamp_local) AS tstamp_local, masked_pan, SAFE.PARSE_DATE(\'%Y%m%d\', date_recon_acq) AS date_recon_acq, net_term_id, rpt_lvl_id_b, mti, tran_type_id, SAFE_CAST(amt_recon_net AS NUMERIC) AS amt_recon_net, act_code, response_code, account_type, SAFE_CAST(transaction_total AS NUMERIC) AS transaction_total, SAFE_CAST(purchase_total AS NUMERIC) AS purchase_total, SAFE_CAST(cashout_total AS NUMERIC) AS cashout_total, retrieval_ref_no, SAFE_CAST(stan AS NUMERIC) AS stan, SAFE_CAST(original_stan AS NUMERIC) AS original_stan, proc_id_acq_b, inst_id_recn_acq_b, card_acpt_name_loc, proc_id_iss_b, issuer_institution_id, card_owner, pos_crd_dat_in_mod, pos_crdhldr_presnt, order_number, approval_code, adtnl_response_data, card_acpt_id, merchant_store_id, dom_int_ind, card_type, card_subtype, bin_product_code, scheme, SAFE_CAST(surcharge_amount AS NUMERIC) AS surcharge_amount, SAFE_CAST(tip_amount AS NUMERIC) AS tip_amount, SAFE_CAST(inst_id_recon_iss AS NUMERIC) AS inst_id_recon_iss, inst_id_recon_iss_name, SAFE_CAST(card_acceptor_business_code AS NUMERIC) AS card_acceptor_business_code, additional_data_acquirer, SAFE_CAST(currency_code AS NUMERIC) AS currency_code, pymt_acct_ref, adl_data_national, adl_data_priv_acq, wallet_id, scdr_token FROM {{ staging_table }};",
                            "dwh_au_feed": "Insert into pdh_rd_external.dwh_au_feed SELECT SAFE.PARSE_DATE(\'%Y%m%d\',\'{{ file_date }}\') as file_date, \'{{ file_name }}\' as file_name, SAFE.PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%S\',\'{{ pdh_load_time }}\') as pdh_load_time, \'{{ payload_id }}\' as payload_id, merch_id, SAFE_CAST(rec_id as NUMERIC) as rec_id, card_num, exp_mmyy, curr, SAFE_CAST(amt as NUMERIC) as amt, auth_code, SAFE.PARSE_DATETIME('%Y%m%d%H%M%S', auth_date) as auth_date, resp_code, int_rc, ext_rc, rev, pos_mode, voice, avs_rslt, cv2_rslt, code_10, card_type, ecom_type, eci_sli, vbv_rslt, SAFE_CAST(ucaf_ind as NUMERIC) as ucaf_ind, ret_refno, acp_det, e_wallet_type, vme_add_auth_method, vme_add_auth_reason_code, ppol_program_data, alternative_merchant_id, SAFE_CAST (mcc as NUMERIC) as mcc, SAFE_CAST (pos_condition_code as NUMERIC) as pos_condition_code, mastercard_banknet_id, SAFE_CAST (trace as NUMERIC) as trace, merchant_number, client_number, mtid,transaction_type, auth_file_source, encrypt_type, security_cap_ind, card_sub_type, additional_pos_info, mastercard_banknet_date, terminal_id, unique_tran_id, acquirer_bin, merchant_country, SAFE_CAST(three_ds_protocol as NUMERIC) three_ds_protocol, SAFE_CAST(sca_exemption_ind as NUMERIC) sca_exemption_ind, cavv, ucaf, pay_token_billing_code from {{ staging_table}};",
                            "dwh_cs_feed": "Insert into pdh_rd_external.dwh_cs_feed SELECT SAFE.PARSE_DATE(\'%Y%m%d\',\'{{ file_date }}\') as file_date, \'{{ file_name }}\' as file_name, SAFE.PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%S\',\'{{ pdh_load_time }}\') as pdh_load_time, \'{{ payload_id }}\' as payload_id, merch_id, SAFE.PARSE_DATE('%Y%m%d',post_date) as post_date, item_type, card_num, arn, rc_code, rc_desc, curr, SAFE_CAST(amt as NUMERIC) as amt, cnn, SAFE.PARSE_DATE('%Y%m%d',o_post_date) as o_post_date, SAFE.PARSE_DATE('%Y%m%d',o_tran_date) as o_tran_date, orig_type, o_tran_curr, SAFE_CAST(o_tran_amt as NUMERIC) as o_tran_amt, m_sett_curr, SAFE_CAST(m_sett_amt as NUMERIC) as m_sett_amt, n_sett_curr, SAFE_CAST(n_sett_amt as NUMERIC) as n_sett_amt,orig_slip, item_slip, auth_code, btch_no, merch_dba, merch_tref, cap_met, mbr_msg, doc_ind, retrieval_request_id, chargeback_reference_id, SAFE.PARSE_DATE('%Y%m%d',due_date) as due_date, SAFE.PARSE_DATE('%Y%m%d',central_processing_date) as central_processing_date, SAFE.PARSE_DATE('%Y%m%d',work_by_date) as work_by_date, SAFE_CAST(chargeback_amount as NUMERIC) as chargeback_amount, chargeback_currency, partial_amt, transfer_or_chargeback_acct_amt, transfer_or_chargeback_acct_curr, e_wallet_type, vme_add_auth_method, vme_add_auth_reason_code, ppol_program_data, SAFE_CAST(case_id as NUMERIC) as case_id, item_type_code, orig_cbk_slip, client_number, merch_fund_currency, SAFE_CAST(merch_fund_amount as NUMERIC) as merch_fund_amount, SAFE_CAST(auth_record_sequence as NUMERIC) as auth_record_sequence,previous_ttm , unique_tran_id, dispute_condition from {{ staging_table }};",
                            "dwh_fa_feed": "Insert into pdh_rd_external.dwh_fa_feed SELECT SAFE.PARSE_DATE(\'%Y%m%d\',\'{{ file_date }}\') as file_date, \'{{ file_name }}\' as file_name, SAFE.PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%S\',\'{{ pdh_load_time }}\') as pdh_load_time, \'{{ payload_id }}\' as payload_id, merch_id, acct_id, acct_type ,SAFE.PARSE_DATE('%Y%m%d',post_date) as post_date, tran_type, SAFE_CAST(slips as NUMERIC) as slips , ref_no ,rev, tran_curr, SAFE_CAST(tran_amt as NUMERIC) as tran_amt, SAFE_CAST(acc_amt as NUMERIC) as acc_amt, SAFE_CAST(acc_chgs as NUMERIC) as acc_chgs, SAFE_CAST(acc_amt_net as NUMERIC) as acc_amt_net,card_type, btch_no, arn, add_arn, fee_seq, fee_seq_desc, SAFE.PARSE_DATE('%Y%m%d',funding_date) as funding_date, SAFE.PARSE_DATE('%Y%m%d',value_date) as value_date, tran_category_code, tran_category, client_number, payment_status, bank_reference, acct_type_code, related_slip, tran_type_code, local_currency, SAFE_CAST(local_amount_gr as NUMERIC) as local_amount_gr from {{ staging_table }};",
                            "dwh_pc_feed": "Insert into pdh_rd_external.dwh_pc_feed SELECT SAFE.PARSE_DATE(\'%Y%m%d\',\'{{ file_date }}\') as file_date, \'{{ file_name }}\' as file_name, SAFE.PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%S\',\'{{ pdh_load_time }}\') as pdh_load_time, \'{{ payload_id }}\' as payload_id, merch_id, SAFE.PARSE_DATE('%Y%m%d',post_date) as post_date, acct_id, presentment_slip, charge_slip,tran_type, tran_curr,SAFE_CAST(tran_amt as NUMERIC) as tran_amt, charge_curr, SAFE_CAST(charge_amt as NUMERIC) as charge_amt, SAFE_CAST(posted_net_amt as NUMERIC) as posted_net_amt,SAFE_CAST(perc as NUMERIC) as perc, SAFE_CAST(base as NUMERIC) as base, rec_id, client_number, service_id, fee_currency from {{ staging_table }};",
                            "dwh_tr_feed": "Insert into pdh_rd_external.dwh_tr_feed SELECT SAFE.PARSE_DATE(\'%Y%m%d\',\'{{ file_date }}\') as file_date, \'{{ file_name }}\' as file_name, SAFE.PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%S\',\'{{ pdh_load_time }}\') as pdh_load_time, \'{{ payload_id }}\' as payload_id, merch_id, SAFE_CAST(btch_no as NUMERIC) as btch_no, SAFE.PARSE_DATE('%Y%m%d',btch_date) as btch_date, SAFE.PARSE_TIMESTAMP('%Y%m%d%H:%M:%S',tran_date) as    tran_date, SAFE.PARSE_DATE('%Y%m%d',post_date) as post_date, item_no, tran_type, card_num, cap_met, term_id, term_cap, auth_code, tran_curr, SAFE_CAST(tran_amt as NUMERIC) as tran_amt, tran_stat, merch_tran_ref, acc_curr, SAFE_CAST(acc_amt_gr as NUMERIC) as acc_amt_gr,SAFE_CAST(acc_amt_ch as NUMERIC) as acc_amt_ch, SAFE_CAST(acc_amt_nt as NUMERIC) as acc_amt_nt, fpi, orig_slip_num, merch_name, arn, dcc_indicator, custom_data, ucaf_or_eci, rt_flag, card_brnd, mcc_code, pos_mode, bin_country, service_type_desc, merchant_country, SAFE_CAST(base as NUMERIC) as base, SAFE_CAST(perc as NUMERIC) as perc, SAFE_CAST(min_val as NUMERIC) as min_val, SAFE_CAST(max_val as NUMERIC) as max_val, record_id, merch_city, area_of_event, e_wallet_type, vme_add_auth_method, vme_add_auth_reason_code, ppol_program_data, service_id, batch_slip, deposit_slip, tran_type_code, term_cap_code, tran_status_code, internal_merch_acc, SAFE_CAST(cross_rate as NUMERIC) as cross_rate, client_number, service_id_code, card_scheme_code, card_scheme, capture_method_code, spend_qualified_indicator, visa_tran_id, mc_banknet_ref, retrieval_reference, mc_banknet_ref_settle_date, fee_currency, unique_tran_id, merch_street, merch_post_code, merch_state_code, custom_tracking_number, dcc_rate, SAFE_CAST(local_amt as NUMERIC) local_amt, local_curr, acquirer_bin_ica, amex_tran_identifier, ardef_country from {{ staging_table }};",
                            "alipay_transaction": "Insert into pdh_rd_apm.alipay_transaction SELECT SAFE.PARSE_DATE(\'%Y%m%d\',\'{{ file_date }}\') as file_date, \'{{ file_name }}\' as file_name,SAFE.PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%S\',\'{{ pdh_load_time }}\') as pdh_load_time, \'{{ payload_id }}\' as payload_id, Partner_transaction_id, SAFE_CAST(Transaction_id as NUMERIC) as Transaction_id,SAFE_CAST(Transaction_amount as NUMERIC) as Transaction_amount,SAFE_CAST(Charge_amount as NUMERIC) as Charge_amount,Currency,SAFE.PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%E*S\',Payment_time) as Payment_time, Transaction_type,Remark,SAFE_CAST(Secondary_merchant_industry as NUMERIC) as Secondary_merchant_industry,Secondary_merchant_name,Secondary_merchant_id,Operator_name,Order_scene ,Trans_currency,SAFE_CAST(Trans_amount as NUMERIC) as Trans_amount,SAFE_CAST(Trans_forex_rate as NUMERIC) as Trans_forex_rate ,Issue,Store_name,Store_id,terminal_id from {{ staging_table }};",
                            "cyb_online_sku_edg": "Insert into pdh_rd_external.cyb_online_sku_edg SELECT SAFE.PARSE_DATE(\'%Y%m%d\',\'{{ file_date }}\') as file_date, \'{{ file_name }}\' as file_name,SAFE.PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%S\',\'{{ pdh_load_time }}\') as pdh_load_time, \'{{ payload_id }}\' as payload_id, request_id, SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez',request_date) as request_date,merchant_id, SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez',localized_request_date) as localized_request_date, overall_req_code, overall_reason_code,overall_req_flag,bill_to_cust_id,bill_to_state,bill_to_zip,store_name_suburb,shipping_method,order_channel,SAFE_CAST(unit_price as NUMERIC) as grand_total, ship_to_country,ship_to_state,ship_to_zip,bin_number,account_suffix,scheme,card_type,merchant_product_sku,line_item_number,product_name, SAFE_CAST(quantity as NUMERIC) as quantity,SAFE_CAST(tax_amount as NUMERIC) as tax_amount,SAFE_CAST(unit_price as NUMERIC) as unit_price from {{ staging_table }};",
                            "cyb_online_sku_other": "Insert into pdh_rd_external.cyb_online_sku_other SELECT SAFE.PARSE_DATE(\'%Y%m%d\',\'{{ file_date }}\') as file_date, \'{{ file_name }}\' as file_name,SAFE.PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%S\',\'{{ pdh_load_time }}\') as pdh_load_time, \'{{ payload_id }}\' as payload_id, request_id, SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez',request_date) as request_date,merchant_id, SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez',localized_request_date) as localized_request_date, overall_req_code,overall_reason_code,overall_req_flag,bill_to_cust_id,bill_to_state,bill_to_zip,store_name_suburb,shipping_method,order_channel,SAFE_CAST(unit_price as NUMERIC) as grand_total, ship_to_country,ship_to_state,ship_to_zip,bin_number,account_suffix,scheme,card_type,merchant_product_sku,line_item_number,product_name, SAFE_CAST(quantity as NUMERIC) as quantity,SAFE_CAST(tax_amount as NUMERIC) as tax_amount,SAFE_CAST(unit_price as NUMERIC) as unit_price from {{ staging_table }};",
                            "qc_detail_transactions": "Insert into pdh_rd_quickcilver.qc_detail_transactions SELECT SAFE.PARSE_DATE(\'%Y%m%d\',\'{{ file_date }}\') as file_date, \'{{ file_name }}\' as file_name,SAFE.PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%S\',\'{{ pdh_load_time }}\') as pdh_load_time, \'{{ payload_id }}\' as payload_id,CardNumber,BookletNumber,MerchantID,Merchant,ActivationOutlet,Outlet,Descriptive_Outlet_Name,OutletCode,OutletGroup,OutletType,ERPCode,Region,Issuer,EmployeeID,'' as CardHolder,'' as FirstName,'' as Last_Name,'' as PhoneNumber, SAFE.PARSE_DATE(\'%d/%m/%Y\',TransactionDate) as TransactionDate,SAFE.PARSE_TIME(\'%H:%M:%S\',TransactionTime) as TransactionTime,TransactionTimeZone,SAFE.PARSE_DATE(\'%d/%m/%Y\',IssuerDate) as IssuerDate,SAFE.PARSE_TIME(\'%H:%M:%S\',IssuerTime) as IssuerTime,IssuerTimeZone, SAFE.PARSE_DATE(\'%d/%m/%Y\',InvoiceDate) as InvoiceDate,SAFE.PARSE_TIME(\'%H:%M:%S\',InvoiceTime) as InvoiceTime,POSName,InitiatedBy,ProgramGroup,ProductCategory,CardProgramGroupType,CardBehaviour,TransactionType,TransactionStatus,RelatedSVCCardNumber,SAFE_CAST(Post_Transaction_SVC_Balance_BaseCurrency_Or_Points as NUMERIC) as Post_Transaction_SVC_Balance_BaseCurrency_Or_Points, SAFE_CAST(CurrencyConvertedAmount_Base_Currency_Or_Points as NUMERIC) as CurrencyConvertedAmount_Base_Currency_Or_Points,BaseCurrency, SAFE_CAST(Amount_Transacting_Merchants_Currency_Or_Points as NUMERIC) as Amount_Transacting_Merchants_Currency_Or_Points, TransactingCurrency,SAFE_CAST(BillAmount_Transacting_Merchants_Currency_Or_Points as NUMERIC) as BillAmount_Transacting_Merchants_Currency_Or_Points,SAFE_CAST(StoredValue_BaseCurrency_Or_Points as NUMERIC) as StoredValue_BaseCurrency_Or_Points, BusinessReferenceNumber,InvoiceNumber,ResponseMessage,ReasonOfCancel,SAFE.PARSE_DATE(\'%d/%m/%Y\',DateAtClient) as DateAtClient,SAFE.PARSE_TIME(\'%H:%M:%S\',TimeAtClient) as TimeAtClient, ParentCardNumber,SAFE_CAST(PreTransactionCardBalance_BaseCurrency_Or_Points as NUMERIC) as PreTransactionCardBalance_BaseCurrency_Or_Points,SAFE_CAST(ExpiredValue_BaseCurrency_Or_Points as NUMERIC) as ExpiredValue_BaseCurrency_Or_Points,TransactionMode, SAFE_CAST(AdjustmentAmount_Transacting_Merchants_Currency_Or_Points as NUMERIC) as AdjustmentAmount_Transacting_Merchants_Currency_Or_Points, SAFE.PARSE_DATE(\'%d/%m/%Y\',TransactionPostDate) as TransactionPostDate,UpgradedProgramGroup, SAFE.PARSE_DATE(\'%d/%m/%Y\',TierUpgradedOn) as TierUpgradedOn,SAFE_CAST(CardBalance_BaseCurrency_Or_Points as NUMERIC) as CardBalance_BaseCurrency_Or_Points,SAFE.PARSE_DATE(\'%d/%m/%Y\',ExpiryDate) as ExpiryDate,ExpiryTimeZone,ReferenceNumber,SAFE_CAST(PromotionalValue_BaseCurrency_Or_Points as NUMERIC) as PromotionalValue_BaseCurrency_Or_Points, OriginalCardNumberBeforeReissue,PromotionalDetails,CorporateName,CardEntryMode,SAFE_CAST(BatchNumber as NUMERIC) as BatchNumber,SAFE_CAST(RequestAmount_Transacting_Merchants_Currency_Or_Points as NUMERIC) as RequestAmount_Transacting_Merchants_Currency_Or_Points, Notes,ApprovalCode,DeactivateReason,SequenceNumber, SequenceNumber_BeforeReissue,DesignCode,GL_Code,CostCentre,OrderType from {{ staging_table }};",
                            "qc_detail_transactions_hourly": "Insert into pdh_rd_quickcilver.qc_detail_transactions_hourly SELECT SAFE.PARSE_DATE(\'%Y%m%d\',\'{{ file_date }}\') as file_date, \'{{ file_name }}\' as file_name,SAFE.PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%S\',\'{{ pdh_load_time }}\') as pdh_load_time, \'{{ payload_id }}\' as payload_id,CardNumber,BookletNumber,MerchantID,Merchant,ActivationOutlet,Outlet,Descriptive_Outlet_Name,OutletCode,OutletGroup,OutletType,ERPCode,Region,Issuer,EmployeeID,'' as CardHolder,'' as FirstName,'' as Last_Name,'' as PhoneNumber, SAFE.PARSE_DATE(\'%d/%m/%Y\',TransactionDate) as TransactionDate,SAFE.PARSE_TIME(\'%H:%M:%S\',TransactionTime) as TransactionTime,TransactionTimeZone,SAFE.PARSE_DATE(\'%d/%m/%Y\',IssuerDate) as IssuerDate,SAFE.PARSE_TIME(\'%H:%M:%S\',IssuerTime) as IssuerTime,IssuerTimeZone, SAFE.PARSE_DATE(\'%d/%m/%Y\',InvoiceDate) as InvoiceDate,SAFE.PARSE_TIME(\'%H:%M:%S\',InvoiceTime) as InvoiceTime,POSName,InitiatedBy,ProgramGroup,ProductCategory,CardProgramGroupType,CardBehaviour,TransactionType,TransactionStatus,RelatedSVCCardNumber,SAFE_CAST(Post_Transaction_SVC_Balance_BaseCurrency_Or_Points as NUMERIC) as Post_Transaction_SVC_Balance_BaseCurrency_Or_Points, SAFE_CAST(CurrencyConvertedAmount_Base_Currency_Or_Points as NUMERIC) as CurrencyConvertedAmount_Base_Currency_Or_Points,BaseCurrency, SAFE_CAST(Amount_Transacting_Merchants_Currency_Or_Points as NUMERIC) as Amount_Transacting_Merchants_Currency_Or_Points, TransactingCurrency,SAFE_CAST(BillAmount_Transacting_Merchants_Currency_Or_Points as NUMERIC) as BillAmount_Transacting_Merchants_Currency_Or_Points,SAFE_CAST(StoredValue_BaseCurrency_Or_Points as NUMERIC) as StoredValue_BaseCurrency_Or_Points, BusinessReferenceNumber,InvoiceNumber,ResponseMessage,ReasonOfCancel,SAFE.PARSE_DATE(\'%d/%m/%Y\',DateAtClient) as DateAtClient,SAFE.PARSE_TIME(\'%H:%M:%S\',TimeAtClient) as TimeAtClient, ParentCardNumber,SAFE_CAST(PreTransactionCardBalance_BaseCurrency_Or_Points as NUMERIC) as PreTransactionCardBalance_BaseCurrency_Or_Points,SAFE_CAST(ExpiredValue_BaseCurrency_Or_Points as NUMERIC) as ExpiredValue_BaseCurrency_Or_Points,TransactionMode, SAFE_CAST(AdjustmentAmount_Transacting_Merchants_Currency_Or_Points as NUMERIC) as AdjustmentAmount_Transacting_Merchants_Currency_Or_Points, SAFE.PARSE_DATE(\'%d/%m/%Y\',TransactionPostDate) as TransactionPostDate,UpgradedProgramGroup, SAFE.PARSE_DATE(\'%d/%m/%Y\',TierUpgradedOn) as TierUpgradedOn,SAFE_CAST(CardBalance_BaseCurrency_Or_Points as NUMERIC) as CardBalance_BaseCurrency_Or_Points,SAFE.PARSE_DATE(\'%d/%m/%Y\',ExpiryDate) as ExpiryDate,ExpiryTimeZone,ReferenceNumber,SAFE_CAST(PromotionalValue_BaseCurrency_Or_Points as NUMERIC) as PromotionalValue_BaseCurrency_Or_Points, OriginalCardNumberBeforeReissue,PromotionalDetails,CorporateName,CardEntryMode,SAFE_CAST(BatchNumber as NUMERIC) as BatchNumber,SAFE_CAST(RequestAmount_Transacting_Merchants_Currency_Or_Points as NUMERIC) as RequestAmount_Transacting_Merchants_Currency_Or_Points, Notes,ApprovalCode,DeactivateReason,SequenceNumber, SequenceNumber_BeforeReissue,DesignCode,GL_Code,CostCentre,OrderType from {{ staging_table }};",
                            "cyb_online_txns_rpt": "Insert into pdh_rd_external.cyb_online_txns_rpt SELECT SAFE.PARSE_DATE(\'%Y%m%d\',\'{{ file_date }}\') as file_date, \'{{ file_name }}\' as file_name,SAFE.PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%S\',\'{{ pdh_load_time }}\') as pdh_load_time, \'{{ payload_id }}\' as payload_id,request_id,transaction_date,merchant_id,LocalizedRequestDate,ics_applications,OverallRcode, OverallReasonCode, OverallRflag,ics_rcode,reason_code, ics_rflag, ics_rmsg,customer_account_id,fulfillment_type,invoice_number,merchant_product_sku,offer_number,product_code,product_name,quantity,tax_amount,amount,merchant_defined_data1,merchant_defined_data10,merchant_defined_data11,merchant_defined_data12,merchant_defined_data13,merchant_defined_data14,merchant_defined_data15,merchant_defined_data16,merchant_defined_data17,merchant_defined_data18,merchant_defined_data19,merchant_defined_data2,merchant_defined_data20,merchant_defined_data3,merchant_defined_data4,merchant_defined_data5,merchant_defined_data6,merchant_defined_data7,merchant_defined_data8,merchant_defined_data9,cavv,ecp_debit_verification_code_raw,ecp_debit_verification_code,aft_indicator,AVSResponse,auth_avs_raw,auth_auth_avs,AVSResultMode,AcquirerMerchantID,AcquirerMerchantNumber,amount_1,auth_factor_code,AuthIndicator,AuthReversalAmount,AuthReversalResult,authentication_mode,auth_code,auth_type,account_balance,account_balance_currency,SAFE_CAST(txn_batch_id as NUMERIC) as txn_batch_id, BinNumber,auth_cv_result,auth_card_category,auth_card_category_code, CardPresent, CardVerificationMethod,category_affiliate,category_campaign,category_group,currency,customer_account_id_1,dcc_indicator,directory_server_transaction_id,merchant_to_acq_route,e_commerce_indicator,EMVRequestFallBack,service_code,auth_ev_email,auth_ev_email_raw,auth_ev_name,auth_ev_name_raw,auth_ev_phone_number,auth_ev_phone_number_raw,auth_ev_postal_code,auth_ev_postal_code_raw,auth_ev_street,auth_ev_street_raw,exchange_rate,exchange_rate_timestamp,SAFE_CAST(GrandTotal as NUMERIC) as GrandTotal,installment_amount,installment_annual_interest_rate,installment_interest_amount,installment_monthly_interest_rate,installment_plan_id,installment_plan_type,installment_sequence,issuer_name,IssuerResponseCode,jpo_jcca_terminal_id,jpo_payment_method,MandateReferenceNumber,merchant_category_code,NetworkCode,NetworkTransactionID,installment_total_count,original_amount,original_currency,cat_level,pos_entry_mode,pos_environment,terminal_capability,PaymentProcessor,PaymentProductCode,SAFE_CAST(PaymentRequestID as NUMERIC) as PaymentRequestID,PinType,points_after_redemption, points_before_redemption, points_redeemed,points_value_after_redemption,points_value_before_redemption,points_value_redeemed,swing_order,ProcessorMID,auth_auth_response,ProcessorResponseID,terminal_id,auth_processor_trans_id,auth_request_amount,auth_request_amount_currency,retrieval_reference_number,RoutingNetwork,RoutingNetworkType,SCAExemption,SCAExemptionValues,sales_slip_number,jpo_business_name_alphanumeric,jpo_business_name_katakana,jpo_business_name_japanese,solution_type,installment_identifier,store_and_forward_indicator,submerchant_city,submerchant_country,submerchant_email,submerchant_id,submerchant_name,submerchant_telephone_number,submerchant_postal_code,submerchant_state,submerchant_street,subsq_auth,subsq_auth_first,subsequent_auth_reason,subsequent_auth_stored_credentials,subsequent_auth_transaction_id,SurchargeFee,SurchargeFeeSign,foreign_amount,foreign_currency,TerminalIDAlternate,pa_specification_version,total_tax_amount,SAFE_CAST(TransactionReferenceNumber as NUMERIC) as TransactionReferenceNumber, xid,eCommerceIndicator,account_no,ecp_account_type,additional_card_type, bank_account_name, bank_code,boleto_payment_bar_code_number,boleto_payment_boleto_number,card_prepaid_reloadable,card_prepaid_type,card_type,card_virtual,ecp_check_no,ecp_effective_date,customer_cc_issue_number,direct_debit_mandate_mandate_id,MandateType,override_payment_method,SignatureDate,customer_cc_startmo,customer_cc_startyr,Type,payment_type,wallet_type,ProfileName,ProfileDecision,ProfileMode,RuleDecision,RuleName,comments,link_to_request,merchant_ref_number,partner_orginal_transaction_id,partner_sdk_version,partner_solution_id,Source,subscription_id,terminal_serial_number,User,AppliedAVS,AppliedCV,AppliedCategoryGift,AppliedCategoryTime,AppliedHostHedge,AppliedThreshold,AppliedTimeHedge,AppliedVelocityHedge,score_bin_account_type,score_bin_country,score_card_issuer,score_card_scheme,score_code_type,CodeValue,ConsumerLoyalty,ConsumerPasswordProvided,ConsumerPromotions,CookiesAccepted,score_device_fingerprint_cookies_enabled,DeviceFingerprint,score_factors,score_device_fingerprint_flash_enabled,GiftWrap,score_host_severity,score_ip_city,score_ip_country,score_ip_routing_method,score_ip_state,score_device_fingerprint_images_enabled,score_device_fingerprint_javascript_enabled,LostPassword,ProductRisk,core_device_fingerprint_proxy_ipaddress,score_device_fingerprint_proxy_ipaddress_activities,score_device_fingerprint_proxy_ipaddress_attributes,score_device_fingerprint_proxy_server_type,RepeatCustomer,ReturnsAccepted,Score,score_time_local,score_device_fingerprint_true_ipaddress, score_device_fingerprint_true_ipaddress_attributes,score_device_fingerprint_true_ipaddress_city, score_device_fingerprint_true_ipaddress_country,score_device_fingerprint_true_ipaddress_activities,NetworkTokenTransType,TokenCode from {{ staging_table }};"
                        }
        sql = curate_sql_dict[table_name]
        return sql
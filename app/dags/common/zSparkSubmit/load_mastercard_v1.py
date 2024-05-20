from pyspark.sql.types import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name
from datetime import datetime
from pytz import timezone
from google.cloud import bigquery,storage
from pyspark.sql import functions as fx
import argparse

bq_client = bigquery.Client()
gcs_client = storage.Client()

spark = SparkSession.builder.master('yarn').appName('spark-load-bigquery').getOrCreate()
sc = spark.sparkContext
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY") ## Added for Composer 2.6 upgrade
tz = timezone('Australia/Sydney')
client = storage.Client()
now = datetime.now(tz)
payload_id = now.strftime("%Y%m%d%H%M%S")

ArgParse = argparse.ArgumentParser(description='Get data-pipeline runtime parameters')
ArgParse.add_argument('bucket',type=str, help='Source GCS bucket name')
ArgParse.add_argument('prefix',type=str, help='Prefix GCS bucket name')
ArgParse.add_argument('dataset',type=str, help='Target dataset name')
ArgParse.add_argument('table',type=str, help='Target table name')
ArgParse.add_argument('load_type',type=str, help='Target load type')
ArgParse.add_argument('project_name',type=str, help='GCP Project Name')


def check_delta(blob_name):
    sql = f'select 1 from pdh_rd_data_navigator.mastercard_TFL6 where file_name =\'{blob_name}\''
    res = bq_client.query(sql)
    for row in res:
        return row


def incremental_load(bucket, prefix, dataset_name, table_name):
    partition = None
    sql = 'select max(file_date) PARTITIONDATE, ' \
          'FORMAT_DATE("%Y%m%d",max(file_date) + 1) as PARTITIONDATE_1 ' \
          'from ' + dataset_name + '.' + table_name + ';'
    query_job = bq_client.query(sql)

    for row in query_job:
        partition = row[1]
        print(row[0],row[1])

    for blob in gcs_client.list_blobs(bucket, prefix=prefix):
        print(blob.name)
        if '.txt' in str(blob.name) and str(blob.name)[-3:] != 'lck' and str(blob.name)[-3:] != 'zip' and partition == '20' + str(blob.name).split('/')[3].split('.')[5][1:]:
            flag = check_delta('gs://' + str(blob.bucket.name) + '/' + str(blob.name))
            execute_pipeline(blob,flag)


def history_load(bucket, prefix, dataset_name, table_name):
    truncate = 'truncate table ' + dataset_name + '.' + table_name
    print(truncate)
    res = bq_client.query(truncate)

    for row in res:
        print(row)

    for blob in client.list_blobs(bucket, prefix=prefix):
        if ('.csv' in str(blob.name).lower() or '.txt' in str(blob.name).lower()) and str(blob.name)[-3:] != 'lck' and str(blob.name)[-3:] != 'zip':
            execute_pipeline(blob,None)


def reflect_bq_schema(dataset_name, table_name):
    sql = 'select \'SELECT to_date(\\\'{0}\\\',\\\'yyyyMMdd\\\') as file_date,\\\'{1}\\\' as file_name, ' \
      'to_timestamp(\\\'{2}\\\',\\\'yyyy-MM-dd HH:mm:ss\\\') as pdh_load_time,\\\'{3}\\\' as payload_id,\'|| column_name from ' \
      '(select string_agg(' \
        'case data_type' \
            ' when \'NUMERIC\' then \'CAST(\' || column_name || \' as DECIMAL) as \' ||  column_name' \
            ' when \'TIMESTAMP\' then \'date_format(to_timestamp(\' || column_name || \', \' || \'\\\'yyyyMMddHHmmssSSS\\\'\' ||\'), \\\'yyyy-MM-dd HH:mm:ss\\\') as \' || column_name' \
            ' when \'DATE\' then \'to_date(\' || column_name || \', \' || \'\\\'yyyyMMdd\\\'\' ||\') as \' || column_name' \
            ' else column_name end, "," order by ordinal_position) as column_name' \
        ' from  '+ dataset_name +'.INFORMATION_SCHEMA.COLUMNS where table_name=\''+ table_name +'\' '\
        ' and ordinal_position not in (1,2,3,4))'
    print(sql)
    query_job = bq_client.query(sql)
    for row in query_job:
        return str(row[0]) + ' from txn_activity'


def execute_pipeline(blob,flag):
    print('Executing Pipeline')
    if 'MCI.AR.TFL6' in blob.name and flag != 1:
        print('gs://' + str(blob.bucket.name) +'/' + str(blob.name))
        target_bucket = 'gs://' + str(blob.bucket.name) + '/' + args.prefix.replace('landing', 'curated')
        mastercard_rdd = sc.textFile('gs://' + str(blob.bucket.name) + '/' + str(blob.name))
        file_date = '20' + str(blob.name).split('/')[3].split('.')[5][1:]
        file_name = str(blob.name)
        rowRDD1 = mastercard_rdd.map(lambda row:'' if row[0:2] != '01' else Row(rec_part=row[0:2],txn_cnt=row[2:9],iso_message_type=row[9:13],pan=row[13:32].strip(' '), processing_code=row[32:38],transaction_amount=row[38:50],fee_amount=row[50:59], replacement_amounts=row[59:101],response_code=row[101:103],reason_code=row[103:107], card_acceptor_terminal_id=row[107:115],retrieval_reference_number=row[115:127],time_local=row[127:133], local_transaction_date=row[133:137],system_trace_audit=row[137:143], acquirer_institution_id_code=row[143:154].strip(' '),settlement_amount=row[154:166], settlement_date=row[166:170],visa_planning_option=row[170:175]))
        print(f'rowRDD1 : {rowRDD1.take(10)}')
        rowRDD1F = rowRDD1.filter(lambda row : row != '')
        print(f'rowRDD1F : {rowRDD1F.take(10)}')
        rowRDD1Df = rowRDD1F.toDF()
        print(f'rowRDD1Df : {rowRDD1Df.take(10)}')
        rowRDD1Df.withColumn("filename", input_file_name()).createOrReplaceTempView('v_part1')

        rowRDD2 = mastercard_rdd.map(lambda row:'' if row[0:2] != '02' else Row(rec_part=row[0:2],txn_cnt=row[2:9],card_acceptor_namelocation=row[9:50], additional_data_private=row[50:72].strip(' '), acq_inst_country_code=row[72:75],pointofservice_entry_mode=row[75:78],account_id_1=row[78:106], account_id_2=row[106:134],transmission_datetime=row[134:144],card_sequence_number=row[144:147], national_pos_condition_code_1=row[147:157],auth_id_response=row[157:163],user_data=row[163:185], auth_agent_inst_id=row[185:196],acq_crossborder_transaction_fee_indicator=row[196:197], acq_crossborder_currency_indicator=row[197:198].strip(' ')))
        rowRDD2F = rowRDD2.filter(lambda row : row != '')
        rowRDD2Df = rowRDD2F.toDF()
        rowRDD2Df.withColumn("filename", input_file_name()).createOrReplaceTempView('v_part2')

        rowRDD3 = mastercard_rdd.map(lambda row:'' if row[0:2] != '03' else Row(rec_part=row[0:2],txn_cnt=row[2:9],amount_cardholder_billing=row[9:21], amount_cardholder_billing_fee=row[21:29], conversion_rate_settlement=row[29:37], conversion_rate_cardholder_bill=row[37:45], date_expiration=row[45:49], date_conversion=row[49:53], date_conversion_1=row[53:57], merchant_type=row[57:61], pan_extended_country_code=row[61:64], forwarding_institution_country_code=row[64:67], network_international_id=row[67:70], pos_condition_code=row[70:72], pos_pin_capture_code=row[72:74], authorization_id_response_length=row[74:75], amount_settlement_fee=row[75:83], amount_transaction_processing_fee=row[83:91], amount_settlement_processing_fee=row[91:99], forwarding_institution_id_code=row[99:110], track_2_data=row[110:147], service_restriction_code=row[147:150], card_acceptor_identification_code=row[150:170], additional_response_data=row[170:195], currency_code_transaction=row[195:198]))
        rowRDD3F = rowRDD3.filter(lambda row : row != '')
        rowRDD3Df = rowRDD3F.toDF()
        rowRDD3Df.withColumn("filename", input_file_name()).createOrReplaceTempView('v_part3')

        rowRDD4 = mastercard_rdd.map(lambda row:'' if row[0:2] != '04' else Row(rec_part=row[0:2],txn_cnt=row[2:9],currency_code_settlement=row[9:12], currency_code_cardholder_billing=row[12:15], pin_data=row[15:23], authorization_life_cycle=row[23:26], national_pos_geographic_data=row[26:43], settlement_code=row[43:44], extended_payment_code=row[44:46], receiving_institution_country_code=row[46:49], settlement_institution_country_code=row[49:52], date_action=row[52:58], credits_number=row[58:68], credits_reversal_number=row[68:78], debits_number=row[78:88], debits_reversal_number=row[88:98], transfer_number=row[98:108], transfer_reversal_number=row[108:118], inquiry_number=row[118:128], authorization_number=row[128:138], credits_processing_fee_amount=row[138:150], credits_transaction_fee_amount=row[150:162], debits_processing_fee_amount=row[162:174], debits_transaction_fee_amount=row[174:186], country_code_authorizing_agent=row[186:189]))
        rowRDD4F = rowRDD4.filter(lambda row : row != '')
        rowRDD4Df = rowRDD4F.toDF()
        rowRDD4Df.withColumn("filename", input_file_name()).createOrReplaceTempView('v_part4')

        rowRDD5 = mastercard_rdd.map(lambda row:'' if row[0:2] != '05' else Row(rec_part=row[0:2],txn_cnt=row[2:9],credits_amount=row[9:25], credits_reversal_amount=row[25:41], debits_amount=row[41:57], debits_reversal_amount=row[57:73], account_qualifiers=row[73:79], original_data_elements=row[79:121], amount_net_settlement=row[121:137], sponsor_bank_id=row[137:148], payee=row[148:173]))
        rowRDD5F = rowRDD5.filter(lambda row : row != '')
        rowRDD5Df = rowRDD5F.toDF()
        rowRDD5Df.withColumn("filename", input_file_name()).createOrReplaceTempView('v_part5')

        rowRDD6 = mastercard_rdd.map(lambda row:'' if row[0:2] != '06' else Row(rec_part=row[0:2],txn_cnt=row[2:9],issuer_trace_data=row[9:41], ddname_of_user_file=row[41:49], file_run_date=row[49:53], file_run_time=row[53:63], settlement_date_1=row[63:67], milestone_0=row[67:77], milestone_1=row[77:85], milestone_2=row[85:93], milestone_3=row[93:101], milestone_4=row[101:109], milestone_5=row[109:117], milestone_6=row[117:125], creditdebit_flag=row[125:126], transaction_code=row[126:128], response_code_1=row[128:130], national_pos_condition_code=row[130:136], sign_surchargerebate_transaction=row[136:137], sign_surchargerebate_reversal=row[137:138], amount_transfers=row[138:154], amount_transfer_reversals=row[154:170], amount_authorizations=row[170:186], count_authorization_reversals=row[186:196]))
        rowRDD6F = rowRDD6.filter(lambda row : row != '')
        rowRDD6Df = rowRDD6F.toDF()
        rowRDD6Df.withColumn("filename", input_file_name()).createOrReplaceTempView('v_part6')

        rowRDD7 = mastercard_rdd.map(lambda row:'' if row[0:2] != '07' else Row(rec_part=row[0:2],txn_cnt=row[2:9],amount_authorization_reversals=row[9:25], count_surchargerebate_transaction=row[25:35], count_surchargerebate_reversals=row[35:45], amount_surchargerebate_reversals=row[45:61], amount_acquirer_reconciliation=row[61:73], amount_issuer_reconciliation=row[73:85], conversion_rate_issuer_reconciliation=row[85:93], currency_code_issuer_reconciliation=row[93:96], amount_acquirer_reconciliation_fee=row[96:104], amount_issuer_reconciliation_fee=row[104:112], completion_amount_cardholder_billing=row[112:124], completion_amount_acquirer_currency=row[124:136], completion_amount_issuer_currency=row[136:148], adjustment_trace_data=row[148:174], batch_debit_indicator=row[174:175], banknet_reference_data_transaction_id=row[175:190], charge_indicator=row[190:191], currency_conversion_fee_in_cardbill_currency=row[191:199]))
        rowRDD7F = rowRDD7.filter(lambda row : row != '')
        rowRDD7Df = rowRDD7F.toDF()
        rowRDD7Df.withColumn("filename", input_file_name()).createOrReplaceTempView('v_part7')

        rowRDD8 = mastercard_rdd.map(lambda row:'' if row[0:2] != '08' else Row(rec_part=row[0:2],txn_cnt=row[2:9],currency_conversion_fee_in_settlement_currency=row[9:17], issuer_currency_conversion_fee_in_cardbill_currency=row[17:25], issuer_crossborder_transaction_indicator=row[25:26], issuer_crossborder_currency_indicator=row[26:27], additional_amounts=row[27:47]))
        rowRDD8F = rowRDD8.filter(lambda row : row != '')
        rowRDD8Df = rowRDD8F.toDF()
        rowRDD8Df.withColumn("filename", input_file_name()).createOrReplaceTempView('v_part8')

        rowRDD10 = mastercard_rdd.map(lambda row:'' if row[0:2] != '10' else Row(rec_part=row[0:2],txn_cnt=row[2:9],clearing_indicator=row[9:10],mti=row[10:14]))
        rowRDD10F = rowRDD10.filter(lambda row : row != '')
        rowRDD10Df = rowRDD10F.toDF()
        rowRDD10Df.withColumn("filename", input_file_name()).createOrReplaceTempView('v_part10')

        tbl_mastercard = spark.sql('select p1.txn_cnt, iso_message_type, pan, processing_code, transaction_amount, fee_amount, replacement_amounts, response_code, reason_code, card_acceptor_terminal_id, retrieval_reference_number, time_local, local_transaction_date, system_trace_audit, acquirer_institution_id_code, settlement_amount, settlement_date, visa_planning_option, card_acceptor_namelocation, additional_data_private, acq_inst_country_code, pointofservice_entry_mode, account_id_1, account_id_2, transmission_datetime, card_sequence_number, national_pos_condition_code, auth_id_response, user_data, auth_agent_inst_id, acq_crossborder_transaction_fee_indicator, acq_crossborder_currency_indicator, amount_cardholder_billing, amount_cardholder_billing_fee, conversion_rate_settlement, conversion_rate_cardholder_bill, date_expiration, date_conversion, date_conversion_1, merchant_type, pan_extended_country_code, forwarding_institution_country_code, network_international_id, pos_condition_code, pos_pin_capture_code, authorization_id_response_length, amount_settlement_fee, amount_transaction_processing_fee, amount_settlement_processing_fee, forwarding_institution_id_code, track_2_data, service_restriction_code, card_acceptor_identification_code, additional_response_data, currency_code_transaction, currency_code_settlement, currency_code_cardholder_billing, pin_data, authorization_life_cycle, national_pos_geographic_data, settlement_code, extended_payment_code, receiving_institution_country_code, settlement_institution_country_code, date_action, credits_number, credits_reversal_number, debits_number, debits_reversal_number, transfer_number, transfer_reversal_number, inquiry_number, authorization_number, credits_processing_fee_amount, credits_transaction_fee_amount, debits_processing_fee_amount, debits_transaction_fee_amount, country_code_authorizing_agent, credits_amount, credits_reversal_amount, debits_amount, debits_reversal_amount, account_qualifiers, original_data_elements, amount_net_settlement, sponsor_bank_id, payee, issuer_trace_data, ddname_of_user_file, file_run_date, file_run_time, settlement_date_1, milestone_0, milestone_1, milestone_2, milestone_3, milestone_4, milestone_5, milestone_6, creditdebit_flag, transaction_code, response_code_1, national_pos_condition_code_1, sign_surchargerebate_transaction, sign_surchargerebate_reversal, amount_transfers,  amount_transfer_reversals, amount_authorizations, count_authorization_reversals, amount_authorization_reversals, count_surchargerebate_transaction,  count_surchargerebate_reversals, amount_surchargerebate_reversals, amount_acquirer_reconciliation, amount_issuer_reconciliation, conversion_rate_issuer_reconciliation, currency_code_issuer_reconciliation, amount_acquirer_reconciliation_fee, amount_issuer_reconciliation_fee, completion_amount_cardholder_billing, completion_amount_acquirer_currency, completion_amount_issuer_currency, adjustment_trace_data, batch_debit_indicator, banknet_reference_data_transaction_id, charge_indicator, currency_conversion_fee_in_cardbill_currency, currency_conversion_fee_in_settlement_currency, issuer_currency_conversion_fee_in_cardbill_currency, issuer_crossborder_transaction_indicator, issuer_crossborder_currency_indicator, additional_amounts, clearing_indicator, mti, p1.filename from v_part1 p1 inner join v_part2 p2 on p1.txn_cnt = p2.txn_cnt and int(p1.rec_part) = int(p2.rec_part) -1 and p1.filename = p2.filename inner join v_part3 p3 on p1.txn_cnt = p3.txn_cnt and int(p2.rec_part) = int(p3.rec_part) -1 and p1.filename = p3.filename inner join v_part4 p4 on p1.txn_cnt = p4.txn_cnt and int(p3.rec_part) = int(p4.rec_part) -1 and p1.filename = p4.filename inner join v_part5 p5 on p1.txn_cnt = p5.txn_cnt and int(p4.rec_part) = int(p5.rec_part) -1 and p1.filename = p5.filename inner join v_part6 p6 on p1.txn_cnt = p6.txn_cnt and int(p5.rec_part) = int(p6.rec_part) -1 and p1.filename = p6.filename inner join v_part7 p7 on p1.txn_cnt = p7.txn_cnt and int(p6.rec_part) = int(p7.rec_part) -1 and p1.filename = p7.filename inner join v_part8 p8 on p1.txn_cnt = p8.txn_cnt and int(p7.rec_part) = int(p8.rec_part) -1 and p1.filename = p8.filename inner join v_part10 p10 on p1.txn_cnt = p10.txn_cnt and int(p8.rec_part) = int(p10.rec_part) -2  and p1.filename = p10.filename')
        tbl_mastercard.createOrReplaceTempView('txn_activity')
        sql = reflect_bq_schema(args.dataset, args.table).format(file_date, file_name, now, payload_id)
        res = spark.sql(sql)
        print(f'res.show table {args.table} MCI.AR.TFL6 :{res.show()}')
        res.withColumn("date", fx.col("file_date")).write.mode("append").partitionBy("date").format("parquet").save(target_bucket)

    if 'MCI.AR.T1G0' in blob.name and flag != 1:
        print('gs://' + str(blob.bucket.name) +'/' + str(blob.name))
        target_bucket = 'gs://' + str(blob.bucket.name) + '/' + args.prefix.replace('landing', 'curated')
        file_date = '20' + str(blob.name).split('/')[3].split('.')[5][1:]
        file_name = str(blob.name)
        mstrRDD = sc.textFile('gs://' + str(blob.bucket.name) + '/' + str(blob.name))
        count = mstrRDD.count()
        txn_noHeaderFooterRDD = mstrRDD.zipWithIndex().filter(lambda row_index: row_index[1] not in (0,count -1)).keys()
        rowRDD = txn_noHeaderFooterRDD.map(lambda row: Row(dr=row[0:1], distribution_ica=row[1:12], mti=row[12:16], pan=row[16:35], processing_code=row[35:41], amount_txn=row[41:53], amount_recon=row[53:65], recon_conv_rate=row[65:73], datetime_local_txn=row[73:85], pos_data_code=row[85:97], function_code=row[97:100], mssg_reason_code=row[100:104], card_acceptor_business_code=row[104:108], original_amount_txn=row[108:120], acquirer_ref_data=row[120:143], approval_code=row[143:149], service_code=row[149:152], card_acceptor_terminal_id=row[152:160], card_acceptor_id_code=row[160:175], card_acceptor_name_loc=row[175:258], card_acceptor_post_code=row[258:268], card_acceptor_state=row[268:271], card_acceptor_country_code=row[271:274], GCMS_product_identifier=row[274:277], licensed_product_identifier=row[277:280], terminal_type=row[280:283], mssg_reversal_indicator=row[283:284], ecom_security_level_indicator=row[284:287], file_id=row[287:312], fee_type_code=row[312:314], fee_processing_code=row[314:316], fee_settlement_indicator=row[316:318], currency_code=row[318:321], amount_txn_fee=row[321:333], currency_code_fee_recon=row[333:336], amount_fee_recon=row[336:348], extended_fee_type_code=row[348:350], extended_fee_processing_code=row[350:352], extended_fee_settlement_indicator=row[352:354], extended_currency_code_fee=row[354:357],extended_interchange_amount_fee=row[357:375], extended_currency_code_fee_recon=row[375:378], extended_interchange_amount_fee_recon=row[378:396],currency_exponents=row[396:400],currency_code_original_txn_amount=row[400:403],card_program_identifier=row[403:406],business_service_arrangement_type_code=row[406:407],business_service_id_code=row[407:413],interchange_rate_designator=row[413:415],central_site_business_date=row[415:421],business_cycle=row[421:423],card_acceptor_classification_override_ind=row[423:424],product_class_override_ind=row[424:427],corporate_incentives_rates_apply_ind=row[427:428],special_conditions_ind=row[428:429],mastercard_assigned_id_override_ind=row[429:430],alm_account_category_code=row[430:431],rate_ind=row[431:432],masterpass_incentive_ind=row[432:433],settlement_svc_transfer_agent_id=row[433:444],settlement_svc_transfer_agent_accnt=row[444:472], settlement_svc_level_code=row[472:473],settlement_svc_id_code=row[473:483],settlement_foreign_exch_rate_class_code=row[483:484],recon_date=row[484:490],recon_cycle=row[490:492],settlement_date=row[492:498],settlement_cycle=row[498:500],txn_currency_code=row[500:503],recon_currency_code=row[503:506],additional_amnt=row[506:526],trace_id=row[526:541],txn_dest_inst_id_code=row[541:552],txn_origin_inst_id_code=row[552:563],date_record=row[563:663]))
        df = rowRDD.toDF()
        # df.withColumn("filename",input_file_name()).createOrReplaceTempView('mastercard_settlement')
        # tbl_mastercard =('select dr, distribution_ica, mti, pan, processing_code, amount_txn, amount_recon, recon_conv_rate, datetime_local_txn,pos_data_code, function_code, mssg_reason_code, card_acceptor_bussiness_code, original_amount_txn, acquirer_ref_data, approval_code, service_code, card_acceptor_terminal_id, card_acceptor_id_code, card_acceptor_name_loc, card_acceptor_post_code, card_acceptor_state, card_acceptor_country_code, GCMS_product_identifier, licensed_product_identifier, terminal_type, mssg_reversal_indicator, ecom_security_lvl_indicator, file_id, fee_type_code, fee_processing_code, fee_settlement_indicator, currency_code, amount_txn_fee, currency_code_fee_recon, amount_fee_recon, extended_fee_type_code, extended_fee_processing_code, extended_fee_settlement_indicator, extended_currency_code_fee, extended_interchange_amount_fee, extended_currency_code_fee_recon, filename from mastercard_settlement')
        # tbl_mastercard.createOrReplaceTempView('txn_activity')
        df.withColumn("filename", input_file_name()).createOrReplaceTempView('txn_activity')
        sql = reflect_bq_schema(args.dataset, args.table).format(file_date, file_name, now, payload_id)
        print(sql)
        res = spark.sql(sql)
        print(f'res.show table {args.table} MCI.AR.T1G0 :{res.show()}')

        res.write.format('bigquery').option('table',args.dataset + '.' + args.table).option("temporaryGcsBucket",args.project_name).mode('append').save()
        # res.withColumn("date", fx.col("file_date")).write.mode("append").partitionBy("date").format("parquet").save(target_bucket)

args = ArgParse.parse_args()
bucket = client.get_bucket(args.bucket)
prefix = args.prefix
load_type = args.load_type
dataset = args.dataset
table = args.table
if load_type == 'H':
    history_load(bucket, prefix, dataset, table)
elif load_type == 'I':
    print('incremental load')
    incremental_load(bucket, prefix, dataset, table)
else:
    print('Invalid load type.')
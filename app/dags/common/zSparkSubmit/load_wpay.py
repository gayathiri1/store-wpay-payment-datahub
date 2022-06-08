from pyspark.sql.types import Row
from pyspark.sql import SparkSession
from google.cloud import bigquery,storage
from datetime import datetime
from pytz import timezone
from jinja2 import Template
import argparse

bq_client = bigquery.Client()
gcs_client = storage.Client()

tz = timezone('Australia/Sydney')
client = storage.Client()

spark = SparkSession.builder.master('yarn').appName('spark-load-bigquery').getOrCreate()
sc = spark.sparkContext

ArgParse = argparse.ArgumentParser(description='Get data-pipeline runtime parameters')
ArgParse.add_argument('bucket',type=str, help='Source GCS bucket name')
ArgParse.add_argument('prefix',type=str, help='Prefix GCS bucket name')
ArgParse.add_argument('dataset',type=str, help='Target dataset name')
ArgParse.add_argument('table',type=str, help='Target table name')
ArgParse.add_argument('load_type',type=str, help='Target load type')
ArgParse.add_argument('project_name',type=str, help='GCP Project Name')


def reflect_bq_schema(dataset_name, table_name):
    sql = 'select \'SELECT to_date(\\\'{0}\\\',\\\'yyyyMMdd\\\') as file_date,\\\'{1}\\\' as file_name, ' \
      'to_timestamp(\\\'{2}\\\',\\\'yyyy-MM-dd HH:mm:ss\\\') as pdh_load_time,\\\'{3}\\\' as payload_id,\'|| column_name from ' \
      '(select string_agg(' \
        'case data_type' \
            ' when \'NUMERIC\' then \'CAST(\' || column_name || \' as STRING) as \' ||  column_name' \
            ' when \'TIMESTAMP\' then \'to_timestamp(\' || column_name || \', \' || \'\\\'yyyyMMddHHmmssSSS\\\'\' ||\') as \' || column_name' \
            ' when \'DATE\' then \'to_date(\' || column_name || \', \' || \'\\\'yyyyMMdd\\\'\' ||\') as \' || column_name' \
            ' else column_name end, "," order by ordinal_position) as column_name' \
        ' from  '+ dataset_name +'.INFORMATION_SCHEMA.COLUMNS where table_name=\''+ table_name +'\' '\
        ' and ordinal_position not in (1,2,3,4))'
    # print(sql)
    query_job = bq_client.query(sql)
    for row in query_job:
        print(f'Schema for {table_name} is {row[0]}')
        return (str(row[0]) + ' from txn_activity')


def incremental_load(bucket, prefix, dataset_name, table_name):
    partition = None
    sql = 'select max(file_date) PARTITIONDATE, ' \
          'FORMAT_DATE("%Y%m%d",max(file_date) + 1) as PARTITIONDATE_1 ' \
          'from ' + dataset_name + '.' + table_name + ';'
    query_job = bq_client.query(sql)
    #print(sql)
    for row in query_job:
        partition = row[1]
        print(row[0],row[1])

    for blob in gcs_client.list_blobs(bucket, prefix=prefix):
        if '.csv' in str(blob.name) and str(blob.name)[-3:] != 'lck' and str(blob.name)[-3:] != 'zip' and partition == blob.name.split('.')[0][-8:]:
            execute_pipeline(blob,'I')


def history_load(bucket, prefix, dataset_name, table_name):
    truncate = 'truncate table ' + dataset_name + '.' + table_name
    print(truncate)
    res = bq_client.query(truncate)

    for row in res:
        print(row)

    for blob in client.list_blobs(bucket, prefix=prefix):
        if ('.csv' in str(blob.name).lower() or '.txt' in str(blob.name).lower()) and str(blob.name)[-3:] != 'lck' and str(blob.name)[-3:] != 'zip':
            execute_pipeline(blob,'H')


def execute_pipeline(blob, load_type):
        print('gs://' + str(blob.bucket.name) +'/' + str(blob.name))
        now = datetime.now(tz)
        payload_id = now.strftime("%Y%m%d%H%M%S")
        target_bucket = 'gs://'+ str(blob.bucket.name) + '/' + args.prefix.replace('landing','curated')
        if ('.csv' in str(blob.name).lower() or '.txt' in str(blob.name).lower()) and  str(blob.name)[-3:] != 'lck' and  str(blob.name)[-3:] != 'zip':
            txrdd = sc.textFile('gs://' + str(blob.bucket.name) +'/' + str(blob.name))
            header = txrdd.zipWithIndex().filter(lambda row_index: row_index[1] == 0).keys()
            count = txrdd.count()
            if count > 3:
                print(f'blob name is : {blob.name}')
                file_date = str(blob.name).split('/')[3].split('.')[0][-8:]
                print(f'file_date is : {file_date}')
                file_name = str(blob.name)
                footer = txrdd.zipWithIndex().filter(lambda row_index: row_index[1] == count -1).keys()
                txn_noHeaderFooterRDD = txrdd.zipWithIndex().filter(lambda row_index: row_index[1] not in (0,1,count -1)).keys()
                txnRDDSplitRDD = txn_noHeaderFooterRDD.map(lambda x:x.split(','))
                rowRDD = txnRDDSplitRDD.map(lambda row:Row(Status=row[0].strip(' '),insert_seq_num=row[1].strip(' '),MTI=row[2].strip(' '),pan=row[3].strip(' '),txn_type_indicator=row[4].strip(' '),currency_code_txn=row[5].strip(' '),amount_txn=row[6].strip(' '),curency_code_recn=row[7].strip(' '),amount_recn=row[8].strip(' '),currency_code_cb=row[9].strip(' '),amount_cb=row[10].strip(' '),txn_timestamp=row[11].strip(' '),currency_code_cf=row[12].strip(' '),amount_cf=row[13].strip(' '),conv_rate_recn_network=row[14].strip(' '),conv_rate_recn_acquirer=row[15].strip(' '),conv_rate_recn_issuer=row[16].strip(' '),conv_rate_cb=row[17].strip(' '),sys_trace_audit_num=row[18].strip(' '),local_timestamp=row[19].strip(' ') + '00',effective_date=row[20].strip(' '),expiry_date=row[21].strip(' '),settlement_date=row[22].strip(' '),conv_date_acquirer=row[23].strip(' '),conv_date_issuer=row[24].strip(' '),capture_date=row[25].strip(' '),ccode_acquiring_inst=row[26].strip(' '),ccode_issuing_inst=row[27].strip(' '),lifecycle_support_indicator=row[28].strip(' '),lifecycle_trace_indicator=row[29].strip(' '),lifecycle_seq_num=row[30].strip(' '),lifecycle_token=row[31].strip(' '),pos_card_data_input_method=row[32].strip(' '),pos_cardholder_auth_method=row[33].strip(' '),pos_operating_env=row[34].strip(' '),pos_cardholder_present=row[35].strip(' '),pos_card_present=row[36].strip(' '),pos_cardholder_auth_entity=row[37].strip(' '),card_seq_num=row[38].strip(' '),function_code=row[39].strip(' '),mssg_reason_code_acquirer=row[40].strip(' '),merchant_cat_code=row[41].strip(' '),pos_card_data_input_capability=row[42].strip(' '),pos_cardholder_auth_capability=row[43].strip(' '),pos_terminal_output_capability=row[44].strip(' '),pos_card_data_output_capability=row[45].strip(' '),pos_card_capture_capability=row[46].strip(' '),pos_pin_capture_capability=row[47].strip(' '),recn_date_acquirer=row[48].strip(' '),recn_date_issuer=row[49].strip(' '),original_ccode_txn=row[50].strip(' '),original_amount_txn=row[51].strip(' '),original_ccode_recn=row[52].strip(' '),original_amount_recn=row[53].strip(' '),acquirer_reference_num=row[54].strip(' '),acquiring_institution_id=row[55].strip(' '),forwarding_institution_id=row[56].strip(' '),track_2_data=row[57].strip(' '),retrieval_ref_num=row[58].strip(' '),approval_code=row[59].strip(' '),action_code=row[60].strip(' '),service_code=row[61].strip(' '),card_acceptor_terminal_id=row[62].strip(' '),card_acceptor_id=row[63].strip(' '),clerk_id=row[64].strip(' '),card_acceptor_ccode=row[65].strip(' '),card_acceptor_postal_code=row[66].strip(' '),card_acceptor_region=row[67].strip(' '),card_acceptor_name_location=row[68].strip(' '),adtnl_response_data=row[69].strip(' '),fee_type_code_1=row[70].strip(' '),fee_ccode_1=row[71].strip(' '),fee_amount_1=row[72].strip(' '),fee_conv_rate_acquirer_1=row[73].strip(' '),recn_fee_ccode_1=row[74].strip(' '),recn_fee_amount_1=row[75].strip(' '),fee_type_code_2=row[76].strip(' '),fee_ccode_2=row[77].strip(' '),fee_amount_2=row[78].strip(' '),fee_conv_rate_acquirer_2=row[79].strip(' '),recn_fee_ccode_2=row[80].strip(' '),recn_fee_amount_2=row[81].strip(' '),fee_type_code_3=row[82].strip(' '),fee_ccode_3=row[83].strip(' '),fee_amount_3=row[84].strip(' '),fee_conv_rate_acquirer_3=row[85].strip(' '),recn_fee_ccode_3=row[86].strip(' '),recn_fee_amount_3=row[87].strip(' '),fee_type_code_4=row[88].strip(' '),fee_ccode_4=row[89].strip(' '),fee_amount_4=row[90].strip(' '),fee_conv_rate_acquirer_4=row[91].strip(' '),recn_fee_ccode_4=row[92].strip(' '),recn_fee_amount_4=row[93].strip(' '),fee_type_code_5=row[94].strip(' '),fee_ccode_5=row[95].strip(' '),fee_amount_5=row[96].strip(' '),fee_conv_rate_acquirer_5=row[97].strip(' '),recn_fee_ccode_5=row[98].strip(' '),recn_fee_amount_5=row[99].strip(' '),fee_type_code_6=row[100].strip(' '),fee_ccode_6=row[101].strip(' '),fee_amount_6=row[102].strip(' '),fee_conv_rate_acquirer_6=row[103].strip(' '),recn_fee_ccode_6=row[104].strip(' '),recn_fee_amount_6=row[105].strip(' '),addtnl_data_acquirer=row[106].strip(' '),addtnl_data_issuer=row[107].strip(' '),cvv_cvc_result=row[108].strip(' '),cvv2_cvc2_result=row[109].strip(' '),cavv_result=row[110].strip(' '),addtnl_request_accnt_type=row[111].strip(' '),addtnl_request_amount_type=row[112].strip(' '),addtnl_request_ccode=row[113].strip(' '),addtnl_request_amount=row[114].strip(' '),ode_mti=row[115].strip(' '),ode_sys_trace_audit_num=row[116].strip(' '),ode_timestamp=row[117].strip(' '),ode_acquiring_inst_id=row[118].strip(' '),auth_lifecycle_code=row[119].strip(' '),ntwrk_terminal_id=row[120].strip(' '),reporting_level_id=row[121].strip(' '),ntwrk_id_acquirer=row[122].strip(' '),ntwrk_id_issuer=row[123].strip(' '),acquirer_processor_group_id=row[124].strip(' '),issuer_processor_group_id=row[125].strip(' '),processor_id_acquirer=row[126].strip(' '),processor_id_issuer=row[127].strip(' '),original_fee_type_code_1=row[128].strip(' '),original_fee_ccode_1=row[129].strip(' '),original_fee_amount_1=row[130].strip(' '),original_fee_conv_rate_acquirer_1=row[131].strip(' '),original_recn_fee_ccode_1=row[132].strip(' '),original_recn_fee_amount_1=row[133].strip(' '),original_fee_type_code_2=row[134].strip(' '),original_fee_ccode_2=row[135].strip(' '),original_fee_amount_2=row[136].strip(' '),original_fee_conv_rate_acquirer_2=row[137].strip(' '),original_recn_fee_ccode_2=row[138].strip(' '),original_recn_fee_amount_2=row[139].strip(' '),original_fee_type_code_3=row[140].strip(' '),original_fee_ccode_3=row[141].strip(' '),original_fee_amount_3=row[142].strip(' '),original_fee_conv_rate_acquirer_3=row[143].strip(' '),original_recn_fee_ccode_3=row[144].strip(' '),original_recn_fee_amount_3=row[145].strip(' '),original_fee_type_code_4=row[146].strip(' '),original_fee_ccode_4=row[147].strip(' '),original_fee_amount_4=row[148].strip(' '),original_fee_conv_rate_acquirer_4=row[149].strip(' '),original_recn_fee_ccode_4=row[150].strip(' '),original_recn_fee_amount_4=row[151].strip(' '),original_fee_type_code_5=row[152].strip(' '),original_fee_ccode_5=row[153].strip(' '),original_fee_amount_5=row[154].strip(' '),original_fee_conv_rate_acquirer_5=row[155].strip(' '),original_recn_fee_ccode_5=row[156].strip(' '),original_recn_fee_amount_5=row[157].strip(' '),original_fee_type_code_6=row[158].strip(' '),original_fee_ccode_6=row[159].strip(' '),original_fee_amount_6=row[160].strip(' '),original_fee_conv_rate_acquirer_6=row[161].strip(' '),original_recn_fee_ccode_6=row[162].strip(' '),original_recn_fee_amount_6=row[163].strip(' '),extended_pay_data=row[164].strip(' '),action_date=row[165].strip(' '),txn_dest_institution_id=row[166].strip(' '),txn_origin_institution_id=row[167].strip(' '),ref_data_issuer_format=row[168].strip(' '),ref_data_issuer=row[169].strip(' '),ccode_recn_acquirer=row[170].strip(' '),amount_recn_acquirer=row[171].strip(' '),ccode_recn_issuer=row[172].strip(' '),amount_recn_issuer=row[173].strip(' '),payee=row[174].strip(' '),settlement_institution_id=row[175].strip(' '),receiving_institution_id=row[176].strip(' '),account_id_1=row[177].strip(' '),account_id_2=row[178].strip(' '),txn_desc=row[179].strip(' '),txn_disposition=row[180].strip(' '),auth_by=row[181].strip(' '),terminal_class=row[182].strip(' '),transaction_class=row[183].strip(' '),role_flag=row[184].strip(' '),accounting_type=row[185].strip(' '),card_owner=row[186].strip(' '),card_type=row[187].strip(' ')))
                df = rowRDD.toDF()
                df.createOrReplaceTempView('txn_activity')
                sql = reflect_bq_schema(args.dataset, args.table).format(file_date, file_name, now, payload_id)
                print(f'sql from reflect_bq_schema : {sql}')
                res = spark.sql(sql)
                print(f'res.show : {res.show()}')
                res.write.format('bigquery')\
                    .option('table', 'pdh_staging_ds' + '.' + args.table)\
                    .option("temporaryGcsBucket", args.project_name).mode('overwrite')\
                    .save()
                insert_stg_to_rd(args.table,args.project_name)
            else:
                print(f'File : {blob.name} is empty. Skipping the load')
        else:
            print('File :{} ignored'.format(blob.name))


def insert_stg_to_rd(table_name,project_name):
    print('Inside BQ final insert module')
    etl_bucket = 'pdh_' + project_name.split('-')[-1] + '_etls'
    bucket = gcs_client.bucket(etl_bucket)
    blob = bucket.blob('insert_txnact/insert_txnact.sql')
    insert_txnact = blob.download_as_string()
    insert_txnact_text = insert_txnact.decode('utf-8')
    template = Template(insert_txnact_text)
    sqlFinal = template.render(src_table='pdh_staging_ds.' + table_name,
                               trgt_table='pdh_rd_data_navigator.' + table_name
                               )
    query_job = bq_client.query(sqlFinal)
    for row in query_job:
        print(row)


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
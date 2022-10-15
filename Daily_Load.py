# CDC
import boto3
import json
import argparse
import traceback
import pandas as pd

from datetime import timedelta, date, datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from sqlalchemy import create_engine

def log_message(level, fun, message):
    print(f"{datetime.now()} :: {level} :: {fun}() :: {message}")

def get_load_file_date(all_variables):
    df_load = spark.read.parquet(f"s3a://{all_variables.get('land_bucket')}/{all_variables.get('prefix')}/LOAD*")
    log_message('info', 'get_load_file_date', f'count of load file is: {df_load.count()}')
    load_file_str_date = df_load.select(df_load.header__timestamp).limit(1).collect()[0][0][0:10].replace('-','')
    log_message('info', 'get_load_file_date', f'return value of load_file_str_date: {load_file_str_date}')
    return load_file_str_date

def get_details_cdcfile_bydate(all_variables, check_date, timestamp):
    log_message('info', 'get_details_cdcfile_bydate', f'received values check_date and ts: {check_date} and {timestamp}')
    results = s3.list_objects_v2(Bucket = all_variables.get('land_bucket'), Prefix = all_variables.get('prefix')+f'/{check_date}'+'-')
    load_date_cdc_flag = False
    s3_files = []
    f_names = []
    if results.get('Contents') != None:
        for result in results.get('Contents'):
            f_timestamp = result.get('Key').split('/')[-1].split('.')[0].split('-')[1]
            if timestamp < f_timestamp:
                s3_files.append(f"s3://{all_variables.get('land_bucket')}/{result.get('Key')}")
                f_names.append(result.get('Key').split('/')[-1].split('.')[0])
                load_date_cdc_flag = True
        log_message('info', 'get_details_cdcfile_bydate', f'return values load_date_cdc_flag, s3_files, max(f_names): {load_date_cdc_flag} , {s3_files}, {max(f_names)}')
    else:
        log_message('info', 'get_details_cdcfile_bydate', f'return values load_date_cdc_flag, s3_files, max(f_names): {load_date_cdc_flag} , {s3_files}, "Empty f_names Files"')

    return load_date_cdc_flag, s3_files, max(f_names)

def get_cdc_gt_ge_loaddate(all_variables, load_date, e_flag):
    log_message('info', 'get_cdc_gt_ge_loaddate', f'received values load_date and e_flag: {load_date} and {e_flag}')
    results = s3.list_objects_v2(Bucket = all_variables.get('land_bucket'), Prefix = all_variables.get('prefix')+f'/')
    cdc_flag = False
    cdc_files_dates = []
    cdc_files_dt = []
    load_date = int(load_date)
    for result in results.get('Contents'):
        if 'LOAD' not in result.get('Key').split('/')[-1].split('.')[0]:
            f_date = int(result.get('Key').split('/')[-1].split('.')[0].split('-')[0])
            f_dt = result.get('Key').split('/')[-1].split('.')[0]
            if e_flag:
                if load_date <= f_date:
                    cdc_files_dates.append(f_date)
                    cdc_files_dt.append(f_dt)
                    cdc_flag = True
            else:
                if load_date < f_date:
                    cdc_files_dates.append(f_date)
                    cdc_files_dt.append(f_dt)
                    cdc_flag = True
    log_message('info', 'get_cdc_gt_ge_loaddate', f'return values cdc_flag, cdc_files_dates, cdc_files_dt: {cdc_flag} , {cdc_files_dates} , {cdc_files_dt}')
    return cdc_flag, cdc_files_dates, cdc_files_dt

def insert_metadata(all_variables, FullLoad, LPF ):
    meta_conn_str = f"mysql+pymysql://{all_variables.get('usrname')}:{all_variables.get('password')}@{all_variables.get('host')}/{all_variables.get('meta_db')}"
    meta_conn = create_engine(meta_conn_str).connect()
    meta_conn.execute(f"Insert into {all_variables.get('meta_table')} (`Source`, `TableName`, `FullLoad`, `LPF` ) values ('{all_variables.get('database')}', '{all_variables.get('table')}', '{FullLoad}', '{LPF}');")
    log_message('info', 'insert_metadata', f"executed following query\n: Insert into {all_variables.get('meta_table')} (`Source`, `TableName`, `FullLoad`, `LPF` ) values ('{all_variables.get('database')}', '{all_variables.get('table')}', '{FullLoad}', '{LPF}');")
    meta_conn.close()
    
def update_metadata(all_variables, FullLoad, LPF ):
    meta_conn_str = f"mysql+pymysql://{all_variables.get('usrname')}:{all_variables.get('password')}@{all_variables.get('host')}/{all_variables.get('meta_db')}"
    meta_conn = create_engine(meta_conn_str).connect()
    meta_conn.execute(f"UPDATE {all_variables.get('meta_table')} SET LPF = '{LPF}', FullLoad = '{FullLoad}' where Source = '{all_variables.get('database')}' and TableName = '{all_variables.get('table')}'; ")
    log_message('info', 'update_metadata', f"executed following query\n: UPDATE {all_variables.get('meta_table')} SET LPF = '{LPF}', FullLoad = '{FullLoad}' where Source = '{all_variables.get('database')}' and TableName = '{all_variables.get('table')}'; ")
    meta_conn.close()
    

def cal_dates_inclusive(s_date, e_date):
    log_message('info', 'cal_dates_inclusive', f'received values s_date, e_date: {s_date} and {e_date}')
    tmp = date(int(s_date[0:4]),int(s_date[4:6]),int(s_date[6:8]))
    cal_dates = [tmp.strftime('%Y%m%d')]
    while tmp < date(int(e_date[0:4]),int(e_date[4:6]),int(e_date[6:8])):
        tmp = tmp + timedelta(days=1)
        cal_dates.append(tmp.strftime('%Y%m%d'))
    log_message('info', 'cal_dates_inclusive', f'return values cal_dates: {cal_dates}')
    return cal_dates

def cdc(all_variables):
    log_message('info', 'cdc', f'started cdc processing')
    LPF, _ = get_lpf_ff(all_variables)
    cdc_flag, _ , MF = get_cdc_gt_ge_loaddate(all_variables, LPF.split('-')[0], True)
    MF = max(MF)
    if cdc_flag: 
        if LPF == MF:
            log_message('info', 'cdc', f'LPF is equal to MF')
            pass
        else:
            if LPF.split('-')[0] == MF.split('-')[0]:
                log_message('info', 'cdc', f'LPF.date is equal to MF.date')
                df_cdc = spark.read.parquet(f"s3://{all_variables.get('land_bucket')}/{all_variables.get('prefix')}/{LPF.split('-')[0]}-*")
                df_cdc.coalesce(1).write.mode('overwrite').parquet(f"s3://{all_variables.get('rawbucket')}/{all_variables.get('prefix')}/{LPF.split('-')[0]}/")
                update_metadata(all_variables, 'True', MF)
            elif LPF.split('-')[0] < MF.split('-')[0]:
                log_message('info', 'cdc', f'LPF.date < MF.date')
                cal_dates = cal_dates_inclusive(LPF.split('-')[0], MF.split('-')[0])
                for date in cal_dates:
                    df_cdc = spark.read.parquet(f"s3://{all_variables.get('land_bucket')}/{all_variables.get('prefix')}/{date}-*")
                    df_cdc.coalesce(1).write.mode('overwrite').parquet(f"s3://{all_variables.get('rawbucket')}/{all_variables.get('prefix')}/{date}/")
                update_metadata(all_variables, 'True', MF)
            else:
                log_message('error', 'cdc', f'CODE ISSUE - ERROR')
                exit(1)
    else:
        pass

def get_lpf_ff(all_variables):
    meta_conn_str = f"mysql+pymysql://{all_variables.get('usrname')}:{all_variables.get('password')}@{all_variables.get('host')}/{all_variables.get('meta_db')}"
    meta_conn = create_engine(meta_conn_str).connect()
    meta_df = pd.read_sql(f"select FullLoad, LPF from {all_variables.get('meta_table')} where Source = '{all_variables.get('database')}' and TableName = '{all_variables.get('table')}'", con = meta_conn)
    if len(meta_df) == 0: 
        last_processed_date = '' 
        full_load_flag = 'False'
    else:
        last_processed_date = meta_df.iloc[0]['LPF']
        full_load_flag = meta_df.iloc[0]['FullLoad']
    meta_conn.close()
    log_message('info', 'get_lpf_ff', f'return values last_processed_date, full_load_flag are: {last_processed_date} , {full_load_flag} ')
    return last_processed_date, full_load_flag

if __name__ == '__main__':

    ##### Argument check and read #####
    parser = argparse.ArgumentParser()
    parser.add_argument('--env', required=True)
    parser.add_argument('--config', required=True)
    args = parser.parse_args()
    env = args.env
    config_file_name = args.config

    ##### Boto clients #####
    global s3 
    s3 = boto3.client('s3')
    sm = boto3.client(service_name='secretsmanager',region_name='us-east-1')


    ##### Spark configuration #####
    spark = SparkSession.builder.enableHiveSupport()\
        .config("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED") \
        .config("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED") \
        .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED") \
        .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
        .getOrCreate()

    ##### Read Config #####
    response = s3.get_object(Bucket = "project2-inputfiles", Key = f"config/{env}/{config_file_name}.json")
    content = response['Body']
    l2r_config = json.loads(content.read())

    ##### Read SecretManager ####
    try:
        secret = sm.get_secret_value(SecretId='SourceDBCreds')
        usr = json.loads(secret['SecretString'])['usrname']
        password = json.loads(secret['SecretString'])['password']
        hostname = json.loads(secret['SecretString'])['host']
    except:
        traceback.print_exc()
        exit(1)

    
    ##### Variable definitions #####
    all_variables = {}
    all_variables.update({'land_bucket' : l2r_config['land_bucket']}) 
    all_variables.update({'database' : f"{l2r_config['database']}"}) 
    all_variables.update({'rawbucket' : l2r_config['raw_bucket']}) 
    all_variables.update({'usrname' : usr}) 
    all_variables.update({'password' : password}) 
    all_variables.update({'host' : hostname}) 
    all_variables.update({'meta_db' : l2r_config['meta_db']}) 
    all_variables.update({'meta_table' : l2r_config['meta_table']}) 
    

    ####### Meta data ########
    any_exp = []
    for table_index in range(len(l2r_config['table'])):
        try:
            all_variables.update({'table' : f"{l2r_config['table'][table_index]['t']}"}) 
            all_variables.update({'prefix' : f"{l2r_config['database']}/{l2r_config['table'][table_index]['t']}"})
            
            ##
            # force_full_load  is true
            # delete record from meta for table-database combination
            # delete s3 raw, raw current 
            ##

            last_processed_date, full_load_flag = get_lpf_ff(all_variables)
            
            if len(last_processed_date) == 0: 
                log_message('info', 'main', f'last_processed_date length is 0')
                load_file_str_date = get_load_file_date(all_variables)
                
                load_files = []
                results = s3.list_objects_v2(Bucket = all_variables.get('land_bucket'), Prefix = all_variables.get('prefix')+f'/LOAD')
                for result in results.get('Contents'):
                    load_files.append(f"s3://{all_variables.get('land_bucket')}/{result.get('Key')}")

                load_date_cdc_flag, cdc_files, max_cdc_filename = get_details_cdcfile_bydate(all_variables, load_file_str_date, '000000000') 

                ## Initial day Load
                if load_date_cdc_flag :
                    log_message('info', 'main', f'load_date_cdc_flag is: {load_date_cdc_flag}')
                    # Reading load files 
                    df_land_load = spark.read.parquet(*load_files)
                    df_land_load = df_land_load.withColumn('Op',lit('I'))
                    # Reading cdc files of the data as load file
                    df_land_loaddate_cdc = spark.read.parquet(*cdc_files)
                    # Union all files of load date and write to RAW layer
                    df_land_load_date = df_land_load.unionByName(df_land_loaddate_cdc)
                    df_land_load_date.coalesce(1).write.mode('overwrite').parquet(f"s3://{all_variables.get('rawbucket')}/{all_variables.get('prefix')}/{load_file_str_date}/")
                    log_message('info', 'main', f"overwritten initial day load file and same day cdc to \ns3://{all_variables.get('rawbucket')}/{all_variables.get('prefix')}/{load_file_str_date}/")
                    LPF = max_cdc_filename

                else :
                    df_land_load_date = spark.read.parquet(*load_files)
                    df_land_load_date = df_land_load_date.withColumn('Op',lit('I'))
                    df_land_load_date.coalesce(1).write.mode('overwrite').parquet(f"s3://{all_variables.get('rawbucket')}/{all_variables.get('prefix')}/{load_file_str_date}/")
                    log_message('info', 'main', f"overwritten initial day load file to \ns3://{all_variables.get('rawbucket')}/{all_variables.get('prefix')}/{load_file_str_date}/")
                    LPF = load_file_str_date + '-000000000'
                
                ## CDC Day Load 
                cdc_flag, cdc_files_dates, _ = get_cdc_gt_ge_loaddate(all_variables, load_file_str_date, False)
                if cdc_flag :
                    insert_metadata(all_variables, 'True', str(min(cdc_files_dates))+'-000000000')
                    cdc(all_variables)
                else:
                    insert_metadata(all_variables, 'False', LPF)
            
            elif len(last_processed_date) > 0 and full_load_flag == 'True':
                log_message('info', 'main', f'last_processed_date length is not 0 and full_load_flag is true')
                cdc( all_variables)

            elif len(last_processed_date) > 0 and full_load_flag == 'False':
                log_message('info', 'main', f'last_processed_date length is not 0 and full_load_flag is false')
                LPF = last_processed_date
                unprocessd_load_date_cdc_flag, unprocessed_cdc_files, unprocessed_max_cdc_filename = get_details_cdcfile_bydate(all_variables, LPF.split('-')[0], LPF.split('-')[1])
                if unprocessd_load_date_cdc_flag :
                    log_message('info', 'main', f'unprocessed files present, unprocessed_max_cdc_filename is {unprocessed_max_cdc_filename}')
                    load_file_str_date = LPF.split('-')[0]
                    # Reading unprocessed cdc files of the data as load file
                    df_land_loaddate_cdc = spark.read.parquet(*unprocessed_cdc_files)
                    df_land_loaddate_cdc.coalesce(1).write.mode('append').parquet(f"s3://{all_variables.get('rawbucket')}/{all_variables.get('prefix')}/{load_file_str_date}/")
                    log_message('info', 'main', f"appended unprocessed files to \ns3://{all_variables.get('rawbucket')}/{all_variables.get('prefix')}/{load_file_str_date}/")
                    LPF = unprocessed_max_cdc_filename
                    ## CDC Day Load 
                    cdc_flag, cdc_files_dates, _ = get_cdc_gt_ge_loaddate(all_variables, load_file_str_date, False)
                    if cdc_flag :
                        update_metadata(all_variables, 'True', str(min(cdc_files_dates))+'-000000000' )
                        cdc(all_variables)
                    else:
                        update_metadata(all_variables, 'False', LPF )
                else:
                    log_message('info', 'main', f'unprocessed files not present')
                    load_file_str_date = LPF.split('-')[0]
                    ## CDC Day Load 
                    cdc_flag, cdc_files_dates, _ = get_cdc_gt_ge_loaddate(all_variables, load_file_str_date, False)
                    if cdc_flag :
                        update_metadata(all_variables, 'True', str(min(cdc_files_dates))+'-000000000')
                        cdc( all_variables)
                    else:
                        pass
        except :
            any_exp.append(str(traceback.format_exc()))
            continue

    if len(any_exp) > 0:
        for e in any_exp:
            print(f"{e}")
        exit(1)
    else:
        print('all tables loaded without any error')
        exit(0)


        


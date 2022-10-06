# CDC
import boto3
import json
import argparse  

from datetime import timedelta, date
from pyspark.sql import SparkSession
from sqlalchemy import create_engine


if __name__ == '__main__':

    ##### Argument check and read #####
    parser = argparse.ArgumentParser()
    parser.add_argument('--env', required=True)
    parser.add_argument('--config', required=True)
    args = parser.parse_args()
    env = args.env
    config_file_name = args.config

    ##### Boto clients #####
    s3 = boto3.client('s3')

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

    ##### Variable definitions #####
    land_bucket = l2r_config['land_bucket']
    prefix = f"{l2r_config['database']}/{l2r_config['table']}"
    rawbucket = l2r_config['raw_bucket']

    ##### Find LOAD File's date  ######
    df_load = spark.read.parquet(f"s3a://{land_bucket}/{prefix}/LOAD*")
    print(f'count of load file is: {df_load.count()}')
    load_file_str_date = df_load.select(df_load.header__timestamp).limit(1).collect()[0][0][0:10].replace('-','')
    print(load_file_str_date)

    ##### CDC max ######
    cdc_files_list = [] 
    result = s3.list_objects_v2(Bucket = land_bucket, Prefix = prefix+'/')
    for obj in result.get('Contents'):
        key = obj.get('Key')
        filename = key.split('/')[-1]
        if 'LOAD' not in filename:
            cdc_files_list.append(filename)
    cdc_max_end_date = max(cdc_files_list)[0:8]

    ##### Cal dates btw Load (start_date) and Max CDC (end date) #####
    '''
    tmp = date(load_file_str_date[0:4],load_file_str_date[4:6],load_file_str_date[6:8])
    cal_dates = [load_file_str_date] 
    while tmp < cdc_max_end_date:
        tmp = tmp + timedelta(days=1) 
        cal_dates.append(tmp)
    '''

    tmp = date(int(load_file_str_date[0:4]),int(load_file_str_date[4:6]),int(load_file_str_date[6:8]))
    cal_dates = [tmp.strftime('%Y%m%d')]
    while tmp < date(int(cdc_max_end_date[0:4]),int(cdc_max_end_date[4:6]),int(cdc_max_end_date[6:8])):
        tmp = tmp + timedelta(days=1)
        cal_dates.append(tmp.strftime('%Y%m%d'))

    ##### Write Landing to RAW with Partitiond Date ######
    s3_files = []
    for cal_date in cal_dates:
        print(f'prefix is {prefix} and Cal_date is {cal_date}')
        results = s3.list_objects_v2(Bucket = land_bucket, Prefix = prefix+'/' + cal_date).get('Contents')
        for result in results:
            s3_files.append(f"s3://{land_bucket}/{result.get('Key')}")

        if load_file_str_date == cal_date:
            results = s3.list_objects_v2(Bucket = land_bucket, Prefix = f"{prefix}/LOAD").get('Contents')
            for result in results:
                s3_files.append(f"s3://{land_bucket}/{result.get('Key')}")

        if len(s3_files) > 0: 
            df_land = spark.read.parquet(*s3_files)
            df_land.write.mode('overwrite').parquet(f's3://{rawbucket}/{prefix}/{cal_date}/')
        s3_files.clear()

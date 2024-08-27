import sys
import os
from dotenv import load_dotenv
import json
import shutil
from Utils.common_sql_utils import SqlLogger
from Utils.spark_utils import SparkSessionFactory, SparkAzureWriter,SparkManager
from Utils.octo_api import OctopusApi
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import Row
import pyspark.sql.functions as F 
from datetime import datetime, date, timezone
from pyspark.sql.types import DateType, IntegerType, StringType, StructField,StructType

def main():
    #octo secrets
    key = os.getenv('OCTOAPIKEY')
    account = os.getenv('OCTOACCOUNT')
    #azure secrets
    storage_account_name = os.getenv('BLOBSTORAGE')
    sas_key = os.getenv('BLOBSASKEY')
    container_name = 'sandboxes'
    # path to spark jar directory
    jar_dir = os.getenv('JAR_DIR')

    #sql message logging params
    database = 'metadata'
    server = os.getenv('SQL_SERVER')
    password = os.getenv('SA_PASSWORD')
    username = os.getenv('SQL_USERNAME')

    spark = SparkSessionFactory.create_spark_session(jar_dir,'Octo Test',True)

    sql_logger = SqlLogger(server, database, username, password)
    sql_logger.setup_logging(schema = 'metadata',table = 'raw_bronze_staging',columns=['UtilId','RowCount','FirstDate','LastDate'])

    # Elec Import Serial: 23J0439951
    # Elec Export Serial: 21M0116985
    #Create caller for Octopus API
    apiCaller = OctopusApi(account, key)
    apiCaller.account_generator()
    apiCaller.extract_elec_details()
    url = apiCaller.consumption_url_gen(util ='Electric',pagesize=10000)
    apiCaller.consumption_get(url)

    schema = StructType([
        StructField('interval_start',StringType(), True),
        StructField('interval_end',StringType(), True),
        StructField('consumption',StringType(), True),
    ]
    )
    empty_rdd = spark.sparkContext.emptyRDD()

    max_date = datetime(1970,1,1,tzinfo=timezone.utc)

    consumption_df = spark.createDataFrame(data= empty_rdd, schema=schema)

    for df, file_path, file_name, df_date in apiCaller.generate_dataframe(spark):

        if df_date > max_date:
            max_date = df_date
        consumption_df = consumption_df.union(df)

    path_list = os.path.abspath(os.path.curdir).split('/Jobs')
    row_count = consumption_df.count()
    base_path = path_list[0]
    full_path = f"{base_path}/data/landing/{file_path}/"

    if os.path.exists(full_path):
        shutil.rmtree(full_path)

    consumption_df.write.parquet(f"{full_path}staging.parquet")


    message = [file_path,row_count,str(df_date),str(max_date)]

    sql_logger.log_message(message)

if __name__=='__main__':
    main()
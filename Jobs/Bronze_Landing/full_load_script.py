import json
import os
from Utils.spark_utils import SparkSessionFactory
from Utils.octo_api import OctopusApi
from Utils.common_sql_utils import SqlUtil
from dotenv import load_dotenv
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as F 
from pyspark.sql.types import DateType, IntegerType, StringType

key = os.getenv('OCTOAPIKEY')
account = os.getenv('OCTOACCOUNT')

storage_account_name = os.getenv('BLOBSTORAGE')
sas_key = os.getenv('BLOBSASKEY')
container_name = 'sandboxes'

jar_dir = os.getenv('JAR_DIR')

#Start Spark Session


spark = SparkSessionFactory.create_spark_session(jar_dir)

# Elec Import Serial: 23J0439951
# Elec Export Serial: 21M0116985
#Create caller for Octopus API
apiCaller = OctopusApi(account, key)
apiCaller.account_generator()

apiCaller.extract_elec_details()
apiCaller.consumption_url_gen(util ='Electric',pagesize=5000)

#loop through api calls to collect data
apiCaller.dataframe_writer(spark=spark, container_name=container_name, storage_account_name=storage_account_name)
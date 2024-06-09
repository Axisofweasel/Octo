
import json
import os
from SparkAzureBuilder import SparkAzure
from OctoAPI import OctopusApi
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import Row
import pyspark.sql.functions as F 
from datetime import datetime
from pyspark.sql.types import DateType, IntegerType, StringType

key = os.getenv('OCTOAPIKEY')
account = os.getenv('OCTOACCOUNT')

storage_account_name = os.getenv('BLOBSTORAGE')
sas_key = os.getenv('BLOBSASKEY')
container_name = 'sandboxes'
path_to_data = 'test.csv'
jar_dir = os.getenv('JAR_DIR')

# Elec Import Serial: 23J0439951
# Elec Export Serial: 21M0116985
apiCaller = OctopusApi(account, key)
apiCaller.account_generator()
apiCaller.extract_elec_details()
con_url = apiCaller.consumption_url_gen(util ='Electric')
while con_url is not None:
    data, con_url = apiCaller.consumption_get(con_url=con_url)
    print(con_url)
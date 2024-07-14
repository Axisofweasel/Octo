import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

class SparkAzure:
    
    def __init__(self, storage_account_name=None, sas_key=None):
        """
        Initialise SparkAzure Class and pass storage account name and sas key into object.

        Args:
            storage_account (string, optional): storage account to be written or read from current spark session. When left as Default None will pull local Env 'BLOBSTORAGE'
            sas_key (_type_, optional): storage account SAS key for storage account in current spark session. When left as Default None will pull local Env 'BLOBSASKEY'
        """        
        self.storage_account_name = storage_account_name or os.getenv('BLOBSTORAGE')
        self.sas_key = sas_key or os.getenv('BLOBSASKEY')
        
        if not self.storage_account_name:
            raise ValueError("Storage account name must be provided either as an argument or through the 'BLOBSTORAGE' environment variable.")
        
        if not self.sas_key:
            raise ValueError("SAS key must be provided either as an argument or through the 'BLOBSASKEY' environment variable.")
        
        return
        
    def spark_session_builder_jar(self, jar_dir=None, container_name=None):
        """
        Create the SparkSession while passing the local Jar Directory either as path or from .env file from JAR_DIR variable
        
        Args:
            jar_dir (string, optional): path to directory holding spark session Jars. Defaults to None. When left as Default will pull local Env 'JAR_DIR'
            container_name (string, optional): Azure Blob Storage container name. Defaults to None.
        Returns:
            SparkSession: New Spark Session with local Jar Files installed.
        """
        self.jar_dir = jar_dir or os.getenv('JAR_DIR')
        self.container_name = container_name
            
        files = os.listdir(self.jar_dir)
        jar_files = [file for file in files if file.endswith('.jar')]
        jar_paths = [os.path.join(self.jar_dir, jar_file) for jar_file in jar_files]
        self.jar_paths = ','.join(jar_paths)  
            
        spark = SparkSession.builder \
                .appName("Azure Blob Storage Access")\
                .config("spark.jars", self.jar_paths)\
                .getOrCreate()
        
        if self.container_name:
            spark.conf.set(
                f"fs.azure.sas.{self.container_name}.{self.storage_account_name}.blob.core.windows.net",
                f"{self.sas_key}"
            )
        
        return spark
    
    def write_to_wasbs(self):
        """Generates Stringpath to write to axzure storage using WASBS protocol.

        Returns:
            string to be passed to write method in the current spark environment as stringpath/PathToWriteTo
        """
        stringpath = f"wasbs://{self.container_name}@{self.storage_account_name}.blob.core.windows.net/"

        return stringpath
    
    def kill_spark_session():
        """
        Kills the current spark session.
        """        
        spark.stop()
        
        print(f'Spark Session Azure Blob Storage Access is stopped')
        
        return
    
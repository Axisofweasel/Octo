#!/usr/bin/env python

import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession


class SparkManager:
    
    def __init__(self):

        self.jar_dir = None
        self.container_name = None
        self.jar_paths = None
        self.storage_account_name = None
        self.sas_key = None
        self.JDBC_SERVER = None
        self.JDBC_DATABASE = None
        self.JDBCcoconnectionString = None
        self.spark = None
        
        return
    
    def wasbs_config(self, storage_account_name=None, sas_key=None):
        """
        Initialise Spark Class and pass storage account name and sas key into object.

        Args:
            storage_account (string, optional): storage account to be written or read from current spark session. When left as Default None will pull local Env 'BLOBSTORAGE'
            sas_key (_type_, optional): storage account SAS key for storage account in current spark session. When left as Default None will pull local Env 'BLOBSASKEY'
        """
        self.storage_account_name = storage_account_name or os.getenv('BLOBSTORAGE')
        self.sas_key = sas_key or os.getenv('BLOBSASKEY')
        
        if not self.storage_account_name:
            ValueError("Storage account name must be provided either as an argument or through the 'BLOBSTORAGE' environment variable.")
        
        if not self.sas_key:
            ValueError("SAS key must be provided either as an argument or through the 'BLOBSASKEY' environment variable.")
            
        return 
        
    def spark_session_builder_jar(self, jar_dir=None, container_name=None, print_jars:bool = False):
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
            
        if os.path.exists(self.jar_dir) == False:
            raise FileNotFoundError('Path does not Exist')
        
        files = os.listdir(self.jar_dir)
        jar_files = [file for file in files if file.endswith('.jar')]
        jar_paths = [os.path.join(self.jar_dir, jar_file) for jar_file in jar_files]
        self.jar_paths = ','.join(jar_paths)
        
        self.spark = (SparkSession.builder
                                    .appName("Azure Blob Storage Access")
                                    .config("spark.jars", self.jar_paths)
                                    .getOrCreate()
                                    )

        if self.container_name:
            self.spark = self.spark.conf.set(
                f"fs.azure.sas.{self.container_name}.{self.storage_account_name}.blob.core.windows.net",
                f"{self.sas_key}"
            )
            
        if print_jars == True:
            [print(jar) for jar in list(self.jar_paths.split(','))]
        
        return self
    
    def write_to_wasbs(self):
        """Generates Stringpath to write to azure storage using WASBS protocol.

        Returns:
            string to be passed to write method in the current spark environment as stringpath/PathToWriteTo
        """
        stringpath = f"wasbs://{self.container_name}@{self.storage_account_name}.blob.core.windows.net/"

        return stringpath
    
    def jdbc_config(self, SERVER :str = None, DATABASE:str = None):
        """ Sets up the necessary connection string for writing to SQL server

        Args:
            SERVER (str, optional): target server name. Defaults to None.
            DATABASE (str, optional): target database name. Defaults to None.
        """
        
        self.JDBC_SERVER = SERVER
        self.JDBC_DATABASE = DATABASE

        self.JDBCcoconnectionString = f'jdbc:sqlserver://{SERVER}:1433;database={DATABASE}'
        
        return
    
    def jdbc_writer(self, DATAFRAME = None, TABLE: str = None, USERNAME: str = None, PASSWORD: str = None, TRUNCATE: str = 'true', WRITEMODE: str = 'overwrite', ENCRYPT: str = 'false'):
    
        if DATAFRAME is None:
            ValueError('DATAFRAME variable must be a spark dataframe')
        if TABLE is None:
            ValueError(f'TABLE variable must be an existing table name in {self.JDBC_SERVER}{self.JDBC_DATABASE} string format')
        if USERNAME is None:
            ValueError(f'USERNAME variable must be a valid username for {self.JDBC_SERVER}{self.JDBC_DATABASE} as string')
        if PASSWORD is None:
            ValueError(f'PASSWORD variable must be a valid username for {self.JDBC_SERVER}{self.JDBC_DATABASE} as string')
        if TRUNCATE in ['true','false']:
            ValueError(f'TRUNCATE value must be true or false')
        if WRITEMODE in ['overwrite','append','error','errorifexists', 'ignore']:
            ValueError(f'WRITEMODE must be a string representation of the following: overwrite, append, error, errorifexists, ignore')
        if TRUNCATE in ['true','false']:
            ValueError(f'TRUNCATE must be a string value of true or false')
        if ENCRYPT in ['true','false']:
            ValueError(f'TRUNCATE must be string value of true or false')
        
        try:
            (DATAFRAME
             .write
             .format('jdbc')
             .option('url', f'{self.connectionString}')
             .option('dbtable', f'{TABLE}')
             .option('user', f'{USERNAME}')
             .option('password', f'{PASSWORD}')
             .option('encrypt', f'{ENCRYPT}')
             .option('truncate', f'{TRUNCATE}')
             .mode(f'{WRITEMODE}')
             .save())

            count = DATAFRAME.count()

        except ConnectionError() as e:
            print(f'Failed to connect to {self.JDBC_DATABASE}.{TABLE}:{e}')
            raise
        except ConnectionRefusedError() as e:
            print(f'Connection refused for {self.JDBC_DATABASE}.{TABLE}: {e}')
            raise
        except PermissionError() as e:
            print(f'Incorrect permissions to write to {self.JDBC_DATABASE}.{TABLE}: {e}')
            raise
        
        return print(f'{count} rows written to {self.JDBC_DATABASE}.{TABLE} as {WRITEMODE}')
    
    
    def kill_spark_session(self):
        """
        Kills the current spark session.
        """        
        self.spark.stop()
        
        print('Spark Session Azure Blob Storage Access is stopped')
        
        return
    

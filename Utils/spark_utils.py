#!/usr/bin/env python

import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from abc import ABC, abstractmethod


class SparkSessionFactory:
    
    @staticmethod
    def create_spark_session(jar_dir:str = None, app_name:str = 'Azure blob writer', print_jars: bool = False):
        
        if os.path.exists(jar_dir) == False:
            raise FileNotFoundError('Path does not Exist')
        
        files = os.listdir(jar_dir)
        jar_files = [file for file in files if file.endswith('.jar')]
        jar_paths = [os.path.join(jar_dir, jar_file) for jar_file in jar_files]
        jar_paths = ','.join(jar_paths)
        
        return (SparkSession
                .builder
                .appName(app_name)
                .config("spark.jars", jar_paths)
                .getOrCreate())


class AzureBlobStorageConfig:
    
    def __init__(self, storage_account_name: str = None, sas_key: str = None, jar_dir: str = None, container_name: str = None):
    
        self.storage_account_name = storage_account_name
        self.sas_key = sas_key
        self.jar_dir = jar_dir
        self.container_name = container_name
 

    def configure_spark(self, spark: SparkSession):

            spark.conf.set(
                f"fs.azure.sas.{self.container_name}.{self.storage_account_name}.blob.core.windows.net",
                f"{self.sas_key}"
            )
            
class SparkManager(ABC):
    
    def __init( self, spark: SparkSession, storage_config: AzureBlobStorageConfig):
        self.spark = spark
        storage_config.configure_spark(spark)
    
    @abstractmethod
    def generate_wasbs_string(self):
        pass
    
    @abstractmethod
    def dataframe_writer(self):
        pass
    
    
class SparkAzureWriter(SparkManager):
    
    def __init__(self,spark:SparkSession, storage_config:AzureBlobStorageConfig):
        super().__init__(spark, storage_config)
    
    def generate_wasbs_string(self):

        self.write_path = f"wasbs://{self.storage_config.container_name}@{self.storage_config.storage_account_name}.blob.core.windows.net/"


    def dataframe_writer(self, spark:SparkSession, file_path:str = None, file_name:str = None):

        if not any([file_path, file_name]):
            raise ValueError('All path parameters must be provided')
        
        path_to_data = f'{filepath}/{file_name}.parquet'
        wasbs_path = f"{self.write_path}{path_to_data}"
        try:
            spark.write.mode("overwrite").parquet(wasbs_path)
        except:
            print(f'Failed to write file:{file_name}')
            raise
        else:
            print(f'Wrote file: {file_name}')
    
    
class SparkJdbcWriter:
    
    def __init__(self, server :str = None, database:str = None):
        """ Sets up the necessary connection string for writing to SQL server

        Args:
            server (str, optional): target server name. Defaults to None.
            database (str, optional): target database name. Defaults to None.
        """
        
        self.JDBC_server = server
        self.JDBC_database = database

        self.JDBCcoconnectionString = f'jdbc:sqlserver://{server}:1433;database={database}'
        
        return
    
    def jdbc_writer(self, dataframe = None, table: str = None, username: str = None, password: str = None, truncate: str = 'true', writemode: str = 'overwrite', encrypt: str = 'false'):
    
        if dataframe is None:
            ValueError('Dataframe variable must be a spark dataframe')
        if table is None:
            ValueError(f'table variable must be an existing table name in {self.JDBC_server}{self.JDBC_database} string format')
        if username is None:
            ValueError(f'username variable must be a valid username for {self.JDBC_server}{self.JDBC_database} as string')
        if password is None:
            ValueError(f'password variable must be a valid username for {self.JDBC_server}{self.JDBC_database} as string')
        if truncate in ['true','false']:
            ValueError(f'truncate value must be true or false')
        if writemode in ['overwrite','append','error','errorifexists', 'ignore']:
            ValueError(f'writemode must be a string representation of the following: overwrite, append, error, errorifexists, ignore')
        if truncate in ['true','false']:
            ValueError(f'truncate must be a string value of true or false')
        if encrypt in ['true','false']:
            ValueError(f'truncate must be string value of true or false')
        
        try:
            (dataframe
             .write
             .format('jdbc')
             .option('url', f'{self.connectionString}')
             .option('dbtable', f'{table}')
             .option('user', f'{username}')
             .option('password', f'{password}')
             .option('encrypt', f'{encrypt}')
             .option('truncate', f'{truncate}')
             .mode(f'{writemode}')
             .save())

            count = DATAFRAME.count()

        except ConnectionError() as e:
            print(f'Failed to connect to {self.JDBC_database}.{table}:{e}')
            raise
        except ConnectionRefusedError() as e:
            print(f'Connection refused for {self.JDBC_database}.{table}: {e}')
            raise
        except PermissionError() as e:
            print(f'Incorrect permissions to write to {self.JDBC_database}.{table}: {e}')
            raise
        
        return print(f'{count} rows written to {self.JDBC_database}.{table} as {writemode}')
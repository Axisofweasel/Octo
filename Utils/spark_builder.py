#!/usr/bin/env python

import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession


class SparkManager:
    
    def __init__(self, storage_account_name: str = None, sas_key: str = None, jar_dir: str = None, container_name: str = None, print_jars: bool = False):
    
        self.storage_account_name = storage_account_name
        self.sas_key = sas_key
        self.jar_dir = jar_dir
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
                                    )

        if self.container_name:
            if not self.storage_account_name:
                ValueError("Storage account name must be provided either as an argument or through the 'BLOBSTORAGE' environment variable.")
            if not self.sas_key:
                ValueError("SAS key must be provided either as an argument or through the 'BLOBSASKEY' environment variable.")
            self.spark = self.spark.conf.set(
                f"fs.azure.sas.{self.container_name}.{self.storage_account_name}.blob.core.windows.net",
                f"{self.sas_key}"
            )
            
        if print_jars == True:
            [print(jar) for jar in list(self.jar_paths.split(','))]
        
        return self.spark.getOrCreate()
    
    def generate_wasbs_string(self):
        """Generates Stringpath to write to azure storage using WASBS protocol.

        Returns:
            string to be passed to write method in the current spark environment as stringpath/PathToWriteTo
        """
        stringpath = f"wasbs://{self.container_name}@{self.storage_account_name}.blob.core.windows.net/"

        return stringpath
    
    
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
            ValueError('DATAFRAME variable must be a spark dataframe')
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
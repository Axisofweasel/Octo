import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
from datetime import datetime
import urllib.parse

#Set environment variables

class SqlUtil:
    
    def __init__(self, SERVER:str =None, DATABASE:str =None, USERNAME:str =None, PASSWORD:str=None, ENCRYPT: str ='no'):
        
        if SERVER is None:
            SERVER = os.getenv('SQL_SERVER')
            if SERVER is None:
                raise ValueError('SERVER needs to be provided or set in environment variables')
        
        if DATABASE is None:
            DATABASE = os.getenv('SQL_DATABASE')
            if DATABASE is None:
                raise ValueError('DATABASE needs to be provided or set in environment variables')
        
        if USERNAME is None:
            USERNAME = os.getenv('SQL_USERNAME')
            if USERNAME is None:
                raise ValueError('USERNAME needs to be provided or set in environment variables')
        
        if PASSWORD is None:
            PASSWORD = os.getenv('SA_PASSWORD')
            if PASSWORD is None:
                raise ValueError('PASSWORD needs to be provided or set in environment variables')
        
        self.SERVER = SERVER
        self.DATABASE = DATABASE
        self.USERNAME = USERNAME
        self.PASSWORD = PASSWORD
        
        self.SERVER = SERVER
        self.DATABASE = DATABASE
        self.USERNAME = USERNAME
        self.PASSWORD = PASSWORD

        self.params = urllib.parse.quote_plus(
                f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                f"SERVER={SERVER};"
                f"DATABASE={DATABASE};"
                f"UID={USERNAME};"
                f"PWD={PASSWORD};"
                f"encrypt={ENCRYPT}"
            )

        self.engine = create_engine(f'mssql+pyodbc:///?odbc_connect={params}')
        self.logging =[]
        pass
    
    def setup_logging(self, schema: str, table: str, if_exists: str = 'append', index: bool = False, columns: list = None):
        
        self.schema=schema
        self.table=table
        self.if_exists = if_exists
        self.index = index 
        self.logging_df = pd.DataFrame(columns=columns)
        self.log_columns
        pass
    
    def log_message(self,log_messages:list):
        
        if len(log_messages)!= len(self.logging_df.index):
            raise ValueError(f'Logging message list must have {len(self.logging_df.index)} items')
        
        new_df = pd.DataFrame(columns=self.columns, data=log_messages)
        
        self.logging_df =pd.concat(self.logging_df, new_df,ignore_index=False)
        return
    
    def write_logging(self):
        
        try:
            rows = len(self.logging_df.index)
            self.logging_df.to_sql(name=self.schema, con=self.engine, if_exists=self.if_exists, index=self.index)
            print('Logging succesful:{rows} inserted to {self.schema}.{self.table} ')
        except ValueError(f'Table {self.table} already exists and if_exist set to {self.if_exists}'):
            raise
        
        return
        
        


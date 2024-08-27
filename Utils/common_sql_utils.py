import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
from datetime import datetime
import urllib.parse
from abc import ABC, abstractmethod

#Set environment variables

class SqlUtil(ABC):
    
    def __init__(self, server:str =None, database:str =None, username:str =None, password:str=None, encrypt: str ='no'):
        
        self.server = server
        self.database = database
        self.username = username
        self.password = password

        if not all([self.server, self.database, self.username, self.password]):
            raise ValueError('All Database connection parameters must be provided')

        self.params = urllib.parse.quote_plus(
                f"DRIVER={{ODBC Driver 18 for SQL server}};"
                f"server={server};"
                f"database={database};"
                f"UID={username};"
                f"PWD={password};"
                f"encrypt={encrypt}"
            )
        
        self.engine = create_engine(f'mssql+pyodbc:///?odbc_connect={self.params}')
        
    @abstractmethod
    def setup_logging(self, schema: str, table: str, if_exists: str = 'append', index: bool = False, columns: list = None):
        pass

    @abstractmethod
    def log_message(self, log_messages: list):
        pass

    @abstractmethod
    def write_logging(self):
        pass
    
class SqlLogger(SqlUtil):
    
    def __init__(self, server:str =None, database:str =None, username:str =None, password:str=None, encrypt: str ='no'):
        super().__init__(server, database, username, password, encrypt)

        self.logging = []
        self.logging_df = None
        self.schema = None
        self.table = None
        self.if_exists = 'append'
        self.index = False
        self.columns = None
        
        return
    
    def setup_logging(self, schema: str, table: str, if_exists: str = 'append', index: bool = False, columns: list = None):
        
        self.schema=schema
        self.table=table
        self.if_exists = if_exists
        self.index = index 
        self.logging_df = pd.DataFrame(columns=columns)
        self.columns = columns

    
    def log_message(self,log_messages:list):
        
        if len(log_messages)!= len(self.logging_df.columns):
            raise ValueError(f'Logging message list must have {len(self.logging_df.index)} items')
        
        new_df = pd.DataFrame(columns = self.columns, data = [log_messages])
        self.logging_df =pd.concat([self.logging_df, new_df],ignore_index=False)

    
    def write_logging(self):
        
        if self.logging_df is None or self.logging_df.empty:
            print('No logs to write.')
        
        try:
            rows = len(self.logging_df.index)
            self.logging_df.to_sql(name=self.table, con=self.engine, if_exists=self.if_exists, index=self.index)
            print(f'Logging succesful:{rows} inserted to {self.schema}.{self.table} ')
        except Exception as e:
            print(f'Error writing logs to database:{str(e)}')
            raise
        
        return
        
        


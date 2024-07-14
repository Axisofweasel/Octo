import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
from datetime import datetime
import urllib.parse

#Set environment variables

class SqlUtil:
    
    def __init__(self, SERVER:str =None, DATABASE:str =None, USERNAME:str =None, PASSWORD:str=None, ENCRYPT: str ='no'):
        
        if SERVER == None:
            try:
                SERVER = os.getenv('SQL_SERVER')
            except TypeError(f'SERVER needs to be a string'):
        if DATABASE == None:
            try:
                DATABASE = os.getenv('SQL_DATABASE')
            except TypeError(f'DATABASE needs to be a string'):
        if USERNAME == None:
            try:
                USERNAME = os.getenv('SQL_USERNAME')
            except TypeError(f'USERNAME needs to be a string'):
        if PASSWORD == None:
            try:
                PASSWORD = os.getenv('SA_PASSWORD')
            except TypeError(f'PASSWORD needs to be a string'):
        
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
    
    def setup_logging(self, schema:str, table:str, if_exists='append', index=False, columns=None:list):
        
        self.schema=schema
        self.table=table
        self.if_exists = if_exists
        self.index = index 
        self.logging_df = pd.DataFrame(columns=columns)
        pass
    
    def log_message(self,log_messages:list):
        
        self.logging.append([list])
        return
    
    def write_logging(self):
        
        try:
            rows = len(self.logging_df.index)
            self.logging_df.to_sql(name=self.schema, con=self.engine, if_exists=self.if_exists, index=self.index)
            print('Logging succesful:{rows} inserted to {self.schema}.{self.table} ')
        except ValueError(f'Table {self.table} already exists and if_exist set to {self.if_exists}'):
        return
        
        


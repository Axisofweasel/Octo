#!/usr/bin/env python

import requests
import os
import pandas as pd
from dotenv import load_dotenv
import datetime
from abc import ABC, abstractmethod
import pyspark.sql.functions as F 
from datetime import datetime

class ApiCaller(ABC):
    
    def __init__(self,account:str = None, key:str = None) -> None:
        super().__init__()
        self._account = account
        self._key = key
        self._url = None
        
    
    def api_call(self):
        pass
    
    def perform_api_call(self, url):
        
        try:
            response = requests.get(url=url, auth=(self._key,''))
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(f'HTTP Error occurred: {e}')
            raise 
        except requests.exceptions.ConnectionError as e:
            print(f'Connection error occurred: {e}')
            raise
        except requests.exceptions.Timeout as e:
            print(f'Timeout error occurred: {e}')
            raise
        except requests.exceptions.RequestException as e:
            print(f'An error occurred: {e}')
            raise
        self.response = response.json()
        return response.json()

    
class OctopusAccount(ApiCaller):
    
    def __init__(self,account:str = None, key:str = None, file:str = None):
        super().__init__(account, key)
        self._base_url= f'https://api.octopus.energy/v1/accounts/{self.account}'
        self.api_call()
        self.file = file
        self.url_dict = {
            'pagesize': 'page_size={pagesize}',
            'period_from': 'period_from={startdatetime}',
            'period_to': 'period_to={enddatetime}',
            'orderby': 'order_by={orderby}'
        }
        
    def api_call(self):
        return self.perform_api_call(self._base_url)

    def account_details(self):
        
        elecmeters = self.response['properties'][0]['electricity_meter_points'][0]['meters']
        self.mpan =  self.response['properties'][0]['electricity_meter_points'][0]['mpan']
        self.electric_meter_list = [_['serial_number'] for _ in elecmeters if _['serial_number']!='']
        self.electric_agreement = self.response['properties'][0]['electricity_meter_points'][0]['agreements']
        
        gas_meters = self.response['properties'][0]['gas_meter_points'][0]['meters']
        self.mprn =  self.response['properties'][0]['gas_meter_points'][0]['mprn']
        self.gas_meter_list = [_['serial_number'] for _ in gas_meters if _['serial_number']!='']
        self.gas_agreement = self.response['properties'][0]['gas_meter_points'][0]['agreements']
        
    
    def generate_url(self, con_url:str = None , pagesize = None, startdatetime = None, enddatetime = None, orderby = None):
        
        def validate_iso8601(date_str):
            try: 
                datetime.datetime.fromisoformat(date_str)
            except ValueError:
                raise ValueError(f"Date {'date_str'} is not in valid ISO 8601 format")

        if pagesize is not None:
            if not isinstance(pagesize, int):
                raise TypeError('Pagesize must be an integer.')
            con_url += '&' if '?' in con_url else '?'
            con_url += self.url_dict['pagesize'].format(pagesize = pagesize)
        if startdatetime is not None:
            validate_iso8601(startdatetime)
            con_url += '&' if '?' in con_url else '?'
            con_url += self.url_dict['period_from'].format(startdatetime = startdatetime)
        if enddatetime is not None:
            validate_iso8601(enddatetime)
            con_url += '&' if '?' in con_url else '?'
            con_url += self.url_dict['period_from'].format(enddatetime = enddatetime)
        if orderby is not None:
            if orderby not in ['period', '-period']:
                raise ValueError("OrderBy must be either 'period' or '-period' ")
            con_url += '&' if '?' in con_url else '?'
            con_url += self.url_dict['orderby'].format(orderby = orderby)

        self._con_url = con_url
        
    def consumption_get(self, response_object):
            
        if 'accounts' in self._con_url:
            print('No consumption data for account')
            return
        try:
            response_object
            print('consumption_get')
            response_object.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(f'HTTP Error occurred: {e}')
            raise 
        except requests.exceptions.ConnectionError as e:
            print(f'Connection error occurred: {e}')
            raise
        except requests.exceptions.Timeout as e:
            print(f'Timeout error occurred: {e}')
            raise
        except requests.exceptions.RequestException as e:
            print(f'An error occurred: {e}')
            raise
        
        try:
            response_object= response_object.json()
            print('generating consumption dict')
        except ValueError as e:
            print(f'Error parsing JSON: {e}')
        next_url = response_object.json()['next']
        self._next_url = next_url
        self.consumption_list = response_object['results']
        return response_object, next_url 
    
    def consumption_loop(self):
        self.consumption_get(self.perform_api_call(self._con_url))
        self.results_df = pd.DataFrame([c for c in self.consumption_list],columns=['consumption','interval_start','interval_end'])
        while self._next_url is not None:
            self.consumption_get(self.perform_api_call(self._next_url))
            new_df = pd.DataFrame([c for c in self.consumption_list],columns=['consumption','interval_start','interval_end'])
            self.results_df = pd.concat([self.results_df,new_df],ignore_index=False)
            if self._next_url is None
                return self.results_df
            
    def writer(self, message):
        self.file.write(message)
        self.file.flush()
        
    
class OctopusElecConsumption(OctopusAccount):
    
    def __init__(self, account: str = None, key: str = None, meter_index:int =0, file:str = None):
        super().__init__(account, key, file=file)
        self._base_url = f'https://api.octopus.energy/v1/electricity-meter-points/{self.mpan}/meters/{self.electric_meter_list[meter_index]}/consumption'

        
    def api_call(self,pagesize = None, startdatetime = None, enddatetime = None, orderby = None ):
        self._con_url = self.generate_url(self._base_url, pagesize, startdatetime , enddatetime, orderby)
        return self.perform_api_call(self._con_url)

class OctopusGasConsumption(OctopusAccount):
    
    def __init__(self, account: str = None, key: str = None, meter_index:int =0, file):
        super().__init__(account, key, file=file)
        self._base_url = f'https://api.octopus.energy/v1/gas-meter-points/{self.mprn}/meters/{self.gas_meter_list[meter_index]}/consumption'
        
    def api_call(self,pagesize = None, startdatetime = None, enddatetime = None, orderby = None ):
        self._con_url = self.generate_url(self._base_url, pagesize, startdatetime , enddatetime, orderby)
        return self.perform_api_call(self._con_url)
    

class JsonToDataFrame():
    
    def __init__(self, json):
        self.json()
        
    def create_dataframe(self):
        pass
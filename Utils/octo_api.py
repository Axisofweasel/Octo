#!/usr/bin/env python

import requests
import os
from dotenv import load_dotenv
import datetime
from abc import ABC, abstractmethod
import pyspark.sql.functions as F 
from datetime import datetime

load_dotenv()

key = os.getenv('OCTOAPIKEY')
account = os.getenv('OCTOACCOUNT')


class OctopusApi(ABC):
    """
    A class to interact with the Octopus Energy API.

    Attributes:
        account (str): The account identifier.
        key (str): The API key for authentication.
        url_dict (dict): A dictionary containing various API endpoints.
    """
    
    def __init__(self, account, key):
        """
        Initializes the OctopusEnergyAPI class with the account and key, and sets up the URL dictionary.

        Args:
            account (str): The account identifier.
            key (str): The API key for authentication.
        """
        self.account = account
        self.key = key
        self.url_dict = self._initialise_url_dict()
        
        
        
    def _initialise_url_dict(self):
        """
        Initializes the URL dictionary with various API endpoints.
        Returns:
            dict: A dictionary containing the API endpoints.
        """
        
        url_dict = {
        'elec_con': 'https://api.octopus.energy/v1/electricity-meter-points/{mpan}/meters/{serial_number}/consumption',
        'gas_con': 'https://api.octopus.energy/v1/gas-meter-points/{mprn}/meters/{serial_number}/consumption',
        'pagesize': 'page_size={pagesize}',
        'period_from': 'period_from={startdatetime}',
        'period_to': 'period_to = {enddatetime}',
        'groupby': 'group_by={groupby}',
        'orderby': 'order_by={orderby}',
        'baseurl': 'https://api.octopus.energy/',
        'account': 'https://api.octopus.energy/v1/accounts/{account}'
        }
        return url_dict

    def account_generator(self):
        """
        Generate a dictionary containing account information by making a GET request to a specified URL.

        This function constructs a URL using the provided account identifier, makes a GET request to that URL
        with the provided authentication key, and returns the parsed JSON response as a dictionary.

        Args:
            account (str): The account identifier used to format the URL.
            key (str): The authentication key used for the GET request.

        Returns:
            dict: A dictionary containing the account information obtained from the JSON response.

        Raises:
            requests.exceptions.HTTPError: If an HTTP error occurs during the GET request.

        Example:
            account_info = account_dict_generator("12345", "my_secret_key")
            print(account_info)
        """
        accounturl = self.url_dict['account'].format(account = self.account)
        try:
            response = requests.get(url=accounturl, auth=(self.key,''))
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
        
        self.account_dict = response.json()
        print('account_generator')
        return 
        
    def extract_elec_details(self):
        """Extract electricity details from the account data.

        Args:
            account_dict (dict): The account data.

        Returns:
            dict: A dictionary containing the electricity details:
                - 'mpan': The MPAN of the electricity meter point.
                - 'serial_number': A list of serial numbers of the electricity meters.
        """
        elec_details = {}
        elec_details['mpan'] = self.account_dict['properties'][0]['electricity_meter_points'][0]['mpan']
        elec_meter_dict = self.account_dict['properties'][0]['electricity_meter_points'][0]['meters']
        elec_details['serial_number'] = [d['serial_number'] for d in elec_meter_dict if d['serial_number'] !='']
        self.elec_details = elec_details
        
        [print(f'MPAN: {elec_details['mpan']} SN {v} Index {i}') for i,v in enumerate(elec_details['serial_number'])]
        
        return 

    def extract_gas_details(self):
        """Extract gas details from the account data.

        Args:
            account_dict (dict): The account data.

        Returns:
            dict: A dictionary containing the gas details:
                - 'mprn': The MPRN of the gas meter point.
                - 'serial_number': A list of serial numbers of the gas meters.
        """
        gas_details = {}
        gas_details['mprn'] = self.account_dict['properties'][0]['gas_meter_points'][0]['mprn']
        gas_details['serial_number']= [e['serial_number'] for e  in gas_meter_dict if e['serial_number'] !='']
        self.gas_details = gas_details
        
        [print(f'MPAN: {gas_details['mprn']} SN {v} Index: {i}') for i, v in enumerate(gas_details['serial_number'])]
        return 
    
    
    def consumption_url_gen(self, util = 'Electric', serial_list = 0 , pagesize = None, startdatetime = None, enddatetime = None, orderby = None):
        """
        Generates a consumption URL based on the specified utility type and optional query parameters.
        Args:
            util (str, optional): The type of utility. Must be either 'Electric' or 'Gas'. Defaults to 'Electric'.
            serial_list (int, optional): The index of the serial number in the list of serial numbers. Defaults to 0.
            pagesize (int, optional): The number of records per page for pagination. Defaults to None.
            startdatetime (str, optional): The start date and time for the consumption data in ISO 8601 format. Defaults to None.
            enddatetime (str, optional): The end date and time for the consumption data in ISO 8601 format. Defaults to None.
            orderby (str, optional): Valid values: * ‘period’, to give results ordered forward. * ‘-period’, (default), to give results ordered from most recent backwards.
        Returns:
            str: The generated consumption URL.
        Raises:
            KeyError: If the utility type is not 'Electric' or 'Gas'.
        Example:
            >>> consumption_url_gen(util='Electric', serial_list=1, pagesize=50, startdatetime='2024-05-30T01:00:00+01:00', enddatetime='2024-06-01T01:00:00+01:00', orderby='date')
            'https://example.com/electricity?mpan=123456789&serial_number=987654321&pagesize=50&period_from=2024-05-30T01:00:00+01:00&period_to=2024-06-01T01:00:00+01:00&orderby=date'
        """
        self.util = util
        if util == 'Electric':
            con_url = self.url_dict['elec_con'].format(mpan=self.elec_details['mpan'], serial_number=self.elec_details['serial_number'][serial_list])
        elif util == 'Gas':
            con_url = self.url_dict['gas_con'].format(mprn=self.gas_details['mprn'], serial_number=self.gas_details['serial_number'][serial_list])
        else:
            raise KeyError("Utility type must be 'Electric' or 'Gas'.")
        
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

        self.con_url = con_url
        print('consumption_url_gen')
        return con_url
    
    def consumption_get(self, con_url):
        """
        Fetches consumption data from the generated URL and handles HTTP errors.
        This method sends a GET request to the URL specified in `self.con_url` with the provided key for authentication.
        It handles various HTTP errors, JSON parsing errors, and connection errors. The method stores the consumption
        data and the next URL for pagination in instance variables and returns them.
        Returns:
            tuple: A tuple containing:
                - consumption_dict (dict): The dictionary containing the consumption data.
                - next_url (str): The URL for the next page of results, if available.
        Raises:
            requests.exceptions.HTTPError: If an HTTP error occurs during the GET request.
            requests.exceptions.RequestException: For other types of requests-related errors.
            ValueError: If the response cannot be parsed as JSON or required keys are missing.
        """
        try:
            consumption_response = requests.get(url = con_url, auth=(self.key,''))
            print('consumption_get')
            consumption_response.raise_for_status()
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
            consumption_dict = consumption_response.json()
            print('generating consumption dict')
        except ValueError as e:
            print(f'Error parsing JSON: {e}')
        next_url = consumption_response.json()['next']
        
        self.next_url = next_url
        self.consumption_dict = consumption_dict
        
        return consumption_dict, next_url 
    
    def generate_dataframe(self, spark):
        
        while self.con_url is not None:
            consumption_df = spark.createDataFrame([c for c in self.consumption_dict['results']])
            consumption_df = consumption_df.select('interval_start','interval_end','consumption')

            max_date = (consumption_df.agg(F.max(F.col('interval_end'))).collect()[0][0])
            max_date = datetime.strptime(max_date, '%Y-%m-%dT%H:%M:%S%z')
            meter = self.elec_details['serial_number'][0]
            file_name = max_date.strftime('%Y_%M_%d_%H_%M_%S')
            file_path = f'{self.util}_{meter}'
            try:
                yield consumption_df, file_path, file_name, max_date
            finally:
                if consumption_df.is_cached:
                        consumption_df.unpersist()
            if self.con_url is None:
                break
            self.consumption_dict, self.con_url = self.consumption_get(self.con_url)
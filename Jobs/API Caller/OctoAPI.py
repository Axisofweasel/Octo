import requests
import os
from dotenv import load_dotenv

load_dotenv()

key = os.getenv('OCTOAPIKEY')
account = os.getenv('OCTOACCOUNT')


class OctopusApi:
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
        self.url_dict = self._initialize_url_dict()
        
        
        def _initialise_url_dict(self):
            """
        Initializes the URL dictionary with various API endpoints.

        Returns:
            dict: A dictionary containing the API endpoints.
        """
            
        url_dict = {
        'elec_con': 'https://api.octopus.energy/v1/electricity-meter-points/{mpan}/meters/{serial_number}/consumption'
        'gas_con': 'https://api.octopus.energy/v1/gas-meter-points/{mprn}/meters/{serial_number}/consumption'
        'pagesize': 'page_size={pagesize}'
        'period_from': 'period_from={startdatetime}'
        'period_to': 'period_to = {enddatetime}'
        'groupby': 'group_by={groupby}'
        'orderby': 'order_by={orderby}'
        'baseurl': 'https://api.octopus.energy/'
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
            response = requests.get(url=accounturl, auth=(key,''))
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise e
        self.account_dict = response.json()
        
        return response.json()
        
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
        elec_details['mpan'] = account_dict['properties'][0]['electricity_meter_points'][0]['mpan']
        elec_details['serial_number'] = [d['serial_number'] for d in elec_meter_dict if d['serial_number'] !='']
        self.elec_details_dict = elec_details
        
        return elec_details

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
        gas_details['mprn'] = account_dict['properties'][0]['gas_meter_points'][0]['mprn']
        gas_details['serial_number']= [e['serial_number'] for e  in gas_meter_dict if e['serial_number'] !='']
        self.gas_details = gas_details
        
        return gas_details
    
    
    def consumption_url_gen(util = 'Electric', serial_list = 0 , pagesize = None, startdatetime = None, enddatetime = None, orderby = None):

        if util == 'Electric':
            con_url = url_dict['elec_con'].format(mpan=elec_details['mpan'], serial_number=elec_details['serial_number'][serial_list])
        elif util == 'Gas'
            con_url = url_dict['gas_con'].format(mprn=gas_details['mprn'], serial_number=gas_details['serial_number'][serial_list])
        else:
            raise KeyError("Utility type must be 'Electric' or 'Gas'.")
        if pagesize is not None:
            con_url += '&' if '?' in con_url else '?'
            con_url += url_dict['pagesize'].format(pagesize = pagesize)
        if startdatetime is not None:
            con_url += '&' if '?' in con_url else '?'
            con_url += url_dict['period_from'].format(startdatetime = startdatetime)
        if enddatetime is not None:
            con_url += '&' if '?' in con_url else '?'
            con_url += url_dict['period_from'].format(enddatetime = enddatetime)
        if orderby is not None:
            con_url += '&' if '?' in con_url else '?'
            con_url += url_dict['orderby'].format(orderby = orderby)

        self.con_url = con_url
        
        return con_url

    
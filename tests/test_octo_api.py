from Utils.octo_api import OctopusApi
from tests.data.json_test_data import octo_account_response, octo_consumption_response, test_url
import pytest 
from unittest.mock import Mock, MagicMock, patch
from requests.exceptions import Timeout, HTTPError, ConnectionError, RequestException
import validators
from pyspark.sql import SparkSession
import datetime
requests = Mock()

octo = OctopusApi('ABCD', 'ABCDEGF')

def test_account_generator_http():
    with patch('Utils.octo_api.requests.get') as mock_get:
        mock_get.side_effect = HTTPError
        with pytest.raises(HTTPError):
            octo.account_generator()

def test_account_generator_timeout():
    with patch('Utils.octo_api.requests.get') as mock_get:
        mock_get.side_effect = Timeout
        with pytest.raises(Timeout):
            octo.account_generator()
            
#just playing about with a different syntax
@patch('Utils.octo_api.requests.get')
def test_account_generator_connection(mock_requests):
    mock_requests.side_effect = ConnectionError
    with pytest.raises(ConnectionError):
        octo.account_generator()
        
@patch('Utils.octo_api.requests.get')
def test_account_generator_positive_response(mock_get):
    mock_requests = Mock()
    mock_requests.json.return_value = octo_account_response
    mock_requests.status_code = 200
    mock_get.return_value = mock_requests
    octo.account_generator()
    assert octo.account_dict == octo_account_response
    assert octo.account_dict['number']==octo_account_response['number']
    assert octo.account_dict['properties'][0]['id']==octo_account_response['properties'][0]['id']
    
def test_extracting_elec_details():
    octo.account_dict = octo_account_response
    octo.extract_elec_details()
    assert octo.elec_details['mpan'] == octo_account_response['properties'][0]['electricity_meter_points'][0]['mpan']
    assert octo.elec_details['serial_number'][0] == octo_account_response['properties'][0]['electricity_meter_points'][0]['meters'][0]['serial_number']
    
def test_extracting_gas_details():
    octo.account_dict = octo_account_response
    octo.extract_gas_details()
    assert octo.gas_details['mprn'] == octo_account_response['properties'][0]['gas_meter_points'][0]['mprn']
    assert octo.gas_details['serial_number'][0] == octo_account_response['properties'][0]['gas_meter_points'][0]['meters'][0]['serial_number']
    
def test_incorrect_utility_raises_error():
    octo.account_dict = octo_account_response
    octo.extract_elec_details()
    with pytest.raises(KeyError) as exception_output:
        octo.consumption_url_gen(util='wrong_key')
    assert exception_output.type == KeyError
    
def test_incorrect_datetype_raises_error():
    octo.account_dict = octo_account_response
    octo.extract_elec_details()
    with pytest.raises(ValueError) as exception_output:
        octo.consumption_url_gen(util = 'Electric', startdatetime='08/05/24')
    assert exception_output.type == ValueError
        
def test_incorrect_pagesize_type_raises_error():
    octo.account_dict = octo_account_response
    octo.extract_elec_details()
    with pytest.raises(TypeError) as exception_output:
        octo.consumption_url_gen(util = 'Electric', pagesize='abc')
    assert exception_output.type == TypeError

def test_incorrect_orderby_value_raises_error():
    octo.account_dict = octo_account_response
    octo.extract_elec_details()
    with pytest.raises(ValueError) as exception_output:
        octo.consumption_url_gen(util = 'Electric', orderby='wrong_order')
    assert exception_output.type == ValueError
    
def test_valid_con_url_created():
    octo.account_dict = octo_account_response
    octo.extract_elec_details()
    url = octo.consumption_url_gen(util = 'Electric')
    url_validators = validators.url(url)
    assert url_validators is True
    
@patch('Utils.octo_api.requests.get')
def test_consumption_get_returns_json(mock_get):
    mock_requests = Mock()
    mock_requests.json.return_value = octo_consumption_response
    mock_requests.status_code = 200
    mock_get.return_value = mock_requests
    octo.consumption_get(test_url)
    assert octo.next_url == octo_consumption_response['next']
    assert octo.consumption_dict == octo_consumption_response

def test_consumption_gen_httperror():
    with patch('Utils.octo_api.requests.get') as mock_get:
        mock_get.side_effect = HTTPError
        with pytest.raises(HTTPError):
            octo.consumption_get(test_url)

@patch('Utils.octo_api.requests.get')
def test_consumption_gen_connection_error(mock_get):
    mock_get.side_effect = ConnectionError
    with pytest.raises(ConnectionError):
        octo.consumption_get(test_url)

@patch('Utils.octo_api.requests.get')
def test_consumption_gen_timeout(mock_get):
    mock_get.side_effect = Timeout
    with pytest.raises(Timeout):
        octo.consumption_get(test_url)

@patch('Utils.octo_api.requests.get')
def test_consumption_gen_request_exception(mock_get):
    mock_get.side_effect = RequestException
    with pytest.raises(RequestException):
        octo.consumption_get(test_url)

def spark_session():
    spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
    return spark

@patch('pyspark.sql.SparkSession')
def test_generate_dataframe(mock_spark):
    
    spark = SparkSession.builder.master("local").appName("test").getOrCreate()
    
    mock_df = MagicMock()
    mock_spark.createDataFrame.return_value = mock_df
    mock_df.select.return_value = mock_df
    mock_agg_result = MagicMock()
    mock_df.agg.return_value.collect.return_value = [("2023-03-26T00:00:00Z",)]
    
    mock_df.is_cached = True
    mock_df.unpersist.return_value = None
    
    octo.consumption_dict = {
            'results': [{'interval_start': '2024-08-08T10:00:00+00:00', 'interval_end': '2024-08-08T12:00:00+00:00', 'consumption': 10}]
        }
    octo.con_url = 'http://next-url.com'
    octo.elec_details = {'serial_number': ['123456']}
    octo.util = 'electric'
    octo.consumption_get = MagicMock(return_value=(octo.consumption_dict, None))
    
    generator = octo.generate_dataframe(mock_spark)
    consumption_df, file_path, file_name, max_date = next(generator)

    mock_spark.createDataFrame.assert_called_once()
    mock_df.select.assert_called_once_with('interval_start','interval_end','consumption')
    assert isinstance(consumption_df, MagicMock)
    assert file_path == 'electric_123456'
    assert file_name == '2023_03_26_00_00_00'
    assert max_date == datetime.datetime(2023,3,26,0,0,0,tzinfo=datetime.timezone.utc)
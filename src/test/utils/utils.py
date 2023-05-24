import sys
sys.path.append("..")

import great_expectations as gx
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from great_expectations.profile.user_configurable_profiler import UserConfigurableProfiler
from great_expectations.checkpoint import SimpleCheckpoint

from ruamel import yaml
from config.config import config

SFACCOUNT   = config['SNOWFLAKE']['ACCOUNT']
SFREGION    = config['SNOWFLAKE']['REGION']
SFUSER      = config['SNOWFLAKE']['USER']
SFPASSWORD  = config['SNOWFLAKE']['PASSWORD']
SFDATABASE  = config['SNOWFLAKE']['DATABASE']
SFWAREHOUSE = config['SNOWFLAKE']['WAREHOUSE']
SFROLE      = config['SNOWFLAKE']['ROLE']

context = gx.get_context()

def create_datasource(datasource_name, sf_schema):
  if SFPASSWORD is None:
    snowflake_conn = f"snowflake://{SFUSER}@{SFACCOUNT}.{SFREGION}/{SFDATABASE}/{sf_schema}?authenticator=externalbrowser&warehouse={SFWAREHOUSE}&role={SFROLE}"
  else:
    snowflake_conn = f"snowflake://{SFUSER}:{SFPASSWORD}@{SFACCOUNT}.{SFREGION}/{SFDATABASE}/{sf_schema}?warehouse={SFWAREHOUSE}&role={SFROLE}"

  datasource_config = {
    "name": datasource_name,
    "class_name": "Datasource",
    "module_name": "great_expectations.datasource",
    "execution_engine": {
      "class_name": "SqlAlchemyExecutionEngine",
      "module_name": "great_expectations.execution_engine",
      "connection_string": snowflake_conn
    },
    "data_connectors": {
      "default_runtime_data_connector_name": {
        "class_name": "RuntimeDataConnector",
        "module_name": "great_expectations.datasource.data_connector",
        "batch_identifiers": ["default_identifier_name"]
      }
    }
  }

  try:
    context.test_yaml_config(yaml.dump(datasource_config))
    print('Data source config successful')
  except:
    print('Please check datasource config, the connection is UNSUCCESSFUL')

  if not check_datasource_config_exists(datasource_name):
    context.add_datasource(**datasource_config)
    print("Create datasource successful")

def check_datasource_config_exists(datasource_name):
  for datasource in context.list_datasources():
    if datasource["name"] == datasource_name:
      print(f'Datasource with name \"{datasource_name}\" configured already, will proceed with the configured details')
      return True
    else:
      return False

def create_expectation_suite(expectation_suite_name):
  try:
    suite = context.get_expectation_suite(expectation_suite_name=expectation_suite_name)
    print(f'Loaded ExpectationSuite "{suite.expectation_suite_name}" containing {len(suite.expectations)} expectations.')
  except:
    suite = context.create_expectation_suite(expectation_suite_name=expectation_suite_name)
    print(f'Created ExpectationSuite "{suite.expectation_suite_name}".')  

def get_batch_request(datasource_name, sf_schema, sf_table, query):
  batch_request = RuntimeBatchRequest(
    datasource_name=datasource_name,
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name=f"{SFDATABASE}.{sf_schema}.{sf_table}",
    runtime_parameters={
      'query': query
    },
    batch_identifiers={
      "default_identifier_name": "default_identifier"
    }
  )

  return batch_request

def setup_expectations_validator(datasource_name, expectation_suite_name, sf_schema, sf_table, query):
  create_datasource(datasource_name, sf_schema)
  create_expectation_suite(expectation_suite_name)

  batch_request = get_batch_request(datasource_name, sf_schema, sf_table, query)

  validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name
  )

  return validator

def show_validator_columns_and_head(validator):
  column_names = [f'"{column_name}"' for column_name in validator.columns()]
  print(f"Columns: {', '.join(column_names)}.")
  print(validator.head(n_rows=5, fetch_all=False))

def close_validator(validator):
  validator.execution_engine.close()

def save_expectation_suite(validator):
  validator.save_expectation_suite(discard_failed_expectations=False)

def configure_checkpoint(checkpoint_name, datasource_name, expectation_suite_name, sf_schema, sf_table, query):
  create_datasource(datasource_name, sf_schema)
    
  batch_request = get_batch_request(datasource_name, sf_schema, sf_table, query)

  checkpoint_config = {
    "name": checkpoint_name,
    "config_version": 1,
    "class_name": "SimpleCheckpoint",
    "validations": [
      {
        "batch_request": batch_request,
        "expectation_suite_name": expectation_suite_name
      }
    ]
  }

  context.add_checkpoint(**checkpoint_config)

def run_checkpoint(checkpoint_name):
  checkpoint_result = context.run_checkpoint(checkpoint_name)
  print(f'{checkpoint_result}')
  create_reports(checkpoint_result)

def create_reports(checkpoint_result):
  context.build_data_docs()
  validation_result_identifier = checkpoint_result.list_validation_result_identifiers()[0]
  context.open_data_docs(resource_identifier=validation_result_identifier)

def delete_datasource(datasource_name):
  context.delete_datasource(datasource_name)

def delete_all_datasource():
  for datasource in context.list_datasources(): 
    context.delete_datasource(datasource["name"])
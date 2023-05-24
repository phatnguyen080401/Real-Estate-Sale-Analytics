import sys
sys.path.append(".")

import utils

SF_SCHEMA = "sale_batch"
SF_TABLE  = "total_customer_by_property_type"

DATASOURCE_NAME = "snowflake_db"
EXPECTATION_SUITE_NAME = f"{SF_TABLE}_suite"
CHECKPOINT_NAME = f"{SF_TABLE}_checkpoint"


validator = utils.setup_expectations_validator(
  datasource_name=DATASOURCE_NAME,
  expectation_suite_name=EXPECTATION_SUITE_NAME,
  sf_schema=SF_SCHEMA,
  sf_table=SF_TABLE,
  query="SELECT * FROM total_customer_by_property_type LIMIT 1000;"
)

# View table head
# utils.show_validator_columns_and_head(validator)

validator.expect_table_columns_to_match_ordered_list(
  column_list=[
    "property_type",
    "total_customer",
    "started_at",
    "ended_at"
  ]
)

validator.expect_column_values_to_be_in_set(
  column="property_type",
  value_set=[
    "Residential",
    "Commercial",
    "Industrial",
    "Apartments",
    "Vacant",
    "Single Family",
    "Two Family",
    "Three Family",
    "Four Family",
    "Public Utility",
    "Condo",
    "Unknown"
  ],
)
validator.expect_column_values_to_not_be_null(column="property_type")
validator.expect_column_values_to_be_in_type_list(
  column="property_type",
  type_list=["VARCHAR"]
)

validator.expect_column_values_to_not_be_null(column="total_customer")
validator.expect_column_values_to_be_in_type_list(
  column="total_customer",
  type_list=["INTEGER", "FLOAT"]
)

validator.expect_column_values_to_not_be_null(column="started_at")
validator.expect_column_values_to_be_in_type_list(
  column="started_at",
  type_list=["TIMESTAMP"]
)

validator.expect_column_values_to_not_be_null(column="ended_at")
validator.expect_column_values_to_be_in_type_list(
  column="ended_at",
  type_list=["TIMESTAMP"]
)

utils.save_expectation_suite(validator)

utils.configure_checkpoint(
    checkpoint_name=CHECKPOINT_NAME,
    datasource_name=DATASOURCE_NAME,
    expectation_suite_name=EXPECTATION_SUITE_NAME,
    sf_schema=SF_SCHEMA,
    sf_table=SF_TABLE,
    query="SELECT * FROM total_customer_by_property_type LIMIT 1000;"
)

utils.run_checkpoint(checkpoint_name=CHECKPOINT_NAME)
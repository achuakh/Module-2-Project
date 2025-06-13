import numpy as np
import pandas as pd
import duckdb
import sqlalchemy
import great_expectations as gx
from great_expectations import expectations as gxe
import pprint
import os


def run_gx_tests():
    # Initialize GX context
    context = gx.get_context()
    
    # Configuration
    source_folder = "data/"
    data_source_name = "olist_geolocation_dataset"
    asset_name = "olist_geolocation_dataset_files"
    batch_definition_name = "olist_geolocation_dataset.csv"
    batch_definition_path = "olist_geolocation_dataset.csv"
    suite_name = "br_ecom_expectation"
    definition_name = "br_ecom_validation_definition"

    # Set up data source and asset
    data_source = context.data_sources.add_pandas_filesystem(
        name=data_source_name,
        base_directory=source_folder
    )
    
    file_csv_asset = data_source.add_csv_asset(name=asset_name)
    file_data_asset = context.data_sources.get(data_source_name).get_asset(asset_name)

    # Create batch definition and get batch
    batch_definition = file_data_asset.add_batch_definition_path(
        name=batch_definition_name,
        path=batch_definition_path
    )
    batch = batch_definition.get_batch()

    # Define expectations
    preset_lat_expectation = gx.expectations.ExpectColumnValuesToBeBetween(
        column="geolocation_lat", min_value=-35, max_value=5
    )

    preset_long_expectation = gx.expectations.ExpectColumnValuesToBeBetween(
        column="geolocation_lng", min_value=-75, max_value=-35
    )

    # Create expectation suite and add expectations
    suite = gx.ExpectationSuite(name=suite_name)
    suite = context.suites.add(suite)
    suite.add_expectation(preset_lat_expectation)
    suite.add_expectation(preset_long_expectation)

    # Define and run validation
    validation_definition = gx.ValidationDefinition(
        data=batch_definition, suite=suite, name=definition_name
    )

    validation_results = validation_definition.run()

    # Save full results to file
    output_folder = "gx_output"
    os.makedirs(output_folder, exist_ok=True)
    result_path = os.path.join(output_folder, "gx_full_results.txt")

    with open(result_path, "w") as f:
        f.write(pprint.pformat(validation_results))

    print(f" Full GX test results saved to {result_path}")

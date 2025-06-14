import numpy as np
import pandas as pd
from pandas_gbq import read_gbq
import great_expectations as gx
from great_expectations import expectations as gxe
import pprint
import os

def run_GX_tests():
    # Set up Great Expectations context
    context = gx.get_context()

    # Query BigQuery
    project_id = "projectm2-aiess"
    query = "SELECT * FROM olist_brazilian_ecommerce_DS.DS_land_geolocation"
    df_geolocation = read_gbq(query, project_id=project_id)

    # Register pandas data source
    data_source_name = "geolocation_df"
    data_source = context.data_sources.add_pandas(name=data_source_name)

    # Create asset
    data_asset_name = "geolocation_asset"
    data_asset = data_source.add_dataframe_asset(name=data_asset_name)

    # Create batch definition
    batch_definition_name = "batch_geolocation_dataframe"
    batch_definition = data_asset.add_batch_definition_whole_dataframe(batch_definition_name)

    # Get batch
    batch_parameters = {"dataframe": df_geolocation}
    new_batch = batch_definition.get_batch(batch_parameters=batch_parameters)

    # Define expectation suite
    suite_name = "br_ecom_expectation"
    suite = gx.ExpectationSuite(name=suite_name)

    # Create expectations
    preset_lat_expectation = gx.expectations.ExpectColumnValuesToBeBetween(
        column="geolocation_lat", min_value=-35, max_value=5
    )
    preset_long_expectation = gx.expectations.ExpectColumnValuesToBeBetween(
        column="geolocation_lng", min_value=-75, max_value=-35
    )

    # Add suite and expectations
    context.suites.add_or_update(suite)
    suite.add_expectation(preset_lat_expectation)
    suite.add_expectation(preset_long_expectation)

    # Run validation
    definition_name = "br_ecom_validation_definition_V2"
    validation_definition = gx.ValidationDefinition(
        data=batch_definition,
        suite=suite,
        name=definition_name
    )
    validation_results = validation_definition.run(batch_parameters=batch_parameters)

    # Save validation results
    output_folder = "gx_output"
    os.makedirs(output_folder, exist_ok=True)
    result_path = os.path.join(output_folder, "gx_results_geo.txt")

    with open(result_path, "w") as f:
        f.write(pprint.pformat(validation_results))

    print(f" Full GX test results saved to {result_path}")


if __name__ == "__main__":
    run_GX_tests()
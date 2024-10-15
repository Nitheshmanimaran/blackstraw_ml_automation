import great_expectations as gx
from great_expectations.data_context import DataContext
from great_expectations.core.batch import BatchRequest
from great_expectations.exceptions import DataContextError
import warnings

def test_ge_configuration():
    try:
        context = DataContext("/home/username/blackstraw_ml_automation/references/gx")
        batch_request = BatchRequest(
            datasource_name="my_datasource",
            data_connector_name="default_inferred_data_connector_name",
            data_asset_name="extracted_data",
        )
        validator = context.get_validator(batch_request=batch_request, expectation_suite_name="extracted_data.csv.warning")
        checkpoint = context.get_checkpoint("my_checkpoint")
        checkpoint_result = checkpoint.run()
        assert checkpoint_result["success"], "Data quality check failed."
    except DataContextError as e:
        assert False, f"Error loading GE context: {e}"
    except Exception as e:
        assert False, f"An error occurred: {e}"
import great_expectations as gx
from great_expectations.data_context import DataContext
from great_expectations.core.batch import BatchRequest
from great_expectations.exceptions import DataContextError


def test_ge_configuration():
    try:
        # Initialize the GE context with the specified project directory
        context = DataContext("/home/username/blackstraw_ml_automation/references/gx")
        print("GE context loaded successfully.")

        # Create a BatchRequest to load the data
        batch_request = BatchRequest(
            datasource_name="my_datasource",
            data_connector_name="default_inferred_data_connector_name",
            data_asset_name="extracted_data",
        )

        # Retrieve the validator
        validator = context.get_validator(batch_request=batch_request, expectation_suite_name="extracted_data.csv.warning")
        print("Batch loaded successfully.")
        print(validator.head())  # Check if data is loaded

        # Load the checkpoint configuration
        checkpoint_name = "my_checkpoint"
        checkpoint = context.get_checkpoint(checkpoint_name)
        print(f"Checkpoint '{checkpoint_name}' loaded successfully.")

        # Run the checkpoint
        checkpoint_result = checkpoint.run()

        # Check if the validation was successful
        if checkpoint_result["success"]:
            print("Data quality check passed.")
        else:
            print("Data quality check failed.")
            for validation_result in checkpoint_result["run_results"].values():
                if not validation_result["validation_result"]["success"]:
                    print(validation_result["validation_result"])

        # Build and open Data Docs
        context.build_data_docs()
        validation_result_identifier = checkpoint_result.list_validation_result_identifiers()[0]
        context.open_data_docs(resource_identifier=validation_result_identifier)

    except DataContextError as e:
        print(f"Error loading GE context: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    test_ge_configuration()


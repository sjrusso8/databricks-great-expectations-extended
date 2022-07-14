import json
import os

from datetime import datetime
from great_expectations.dataset import SparkDFDataset

class ExtendedSparkDFDataset(SparkDFDataset):
    """ExtendedSparkDFDataset extends the GE data SparkDFDataset for Databricks
    
    Attributes:
        dbutils: the Databricks utilitie module
        path: String value of lexical file path
        file_path: concat of path and the GE batch id as json
    """
    
    def __init__(self, spark_df, dbutils, path="/dbfs/data-quality", **kwargs):
        super().__init__(spark_df=spark_df, **kwargs)
        self.dbutils = dbutils
        self.path = f"{path}/{datetime.today().strftime('%Y/%m/%d/%H/%M')}"
        self.file_path = f"{self.path}/{self.batch_kwargs['ge_batch_id']}.json"
        
    def get_notebook_metadata(self):
        return json.loads(
            self.dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
        )
    
    def _create_output_file(self):
        if not os.path.exists(self.file_path):
            try:
                os.makedirs(os.path.dirname(self.file_path), exist_ok=True)
            except:
                raise ValueError(
                    "Failed to create the file path. Please check the user write permissions"
                )
                
    def _write_results_to_json(self, validation_object):
        self._create_output_file()
        with open(self.file_path, "w") as f:
            f.write(json.dumps(validation_object.to_json_dict()))
        return True
    
    def _assert_expectations(self, validation_object):
        results = validation_object.get("results")
        
        failures = [res for res in results if not res.get("success")]

        if failures:
            assert_message = [
                f"\n\n{len(failures):d} expectations out of {len(results):d} were not met:\n"
            ]
            for failed_result in failures:
                test_type = failed_result.get("expectation_config").get("expectation_type")
                test_config = failed_result.get("expectation_config").get("kwargs")
                test_results = failed_result.get("result")
                
                assert_message.append(f'\t - Failed Expectation: "{test_type}"')
                assert_message.append(f'\t\t - Expectation Config: "{test_config}"')

                if test_results:
                    assert_message.append(f'\t\t - Expectation Result: "{test_results}"')

            assert_message.append(f"\nView logs of expectations at {self.file_path}")

            assert False, "\n".join(assert_message)
        else:
            print(
                f"""All Expectations Passed: {len(failures):d} expectations out of {len(results):d} were not met
                
                - View logs of expectations at {self.file_path}
                """
            )
            assert True
    
    def validate_and_save(self):
        self.add_citation(self.get_notebook_metadata())
        
        validation_object = self.validate()
        
        self._write_results_to_json(validation_object)
        self._assert_expectations(validation_object)

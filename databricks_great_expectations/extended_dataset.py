import json
from pathlib import Path

from great_expectations.dataset import SparkDFDataset


class ExtendedSparkDFDataset(SparkDFDataset):
    """ExtendedSparkDFDataset extends the GE data SparkDFDataset for Databricks

    Attributes:
        dbutils: the Databricks utilitie module
        save_path: String value of file path. Needs to start with dbfs:/ or /dbfs/
        **kwargs: additional keyword arguments are passed to the parent class
    """

    def __init__(self, spark_df, dbutils, save_path: str, **kwargs):
        super().__init__(spark_df=spark_df, **kwargs)
        self.dbutils = dbutils

        if save_path.startswith("dbfs:/"):
            self.save_path = Path(save_path.replace("dbfs:/", "/dbfs/"))
        elif save_path.startswith("/dbfs/"):
            self.save_path = Path(save_path)
        else:
            raise ValueError("save_path must be a dbfs path.")

    def get_notebook_metadata(self) -> dict:
        return json.loads(
            self.dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .toJson()
        )

    def set_validation_results(self) -> None:
        self.add_citation(self.get_notebook_metadata())
        self.results = self.validate()

    def write_results_to_json(self):
        self.file_path = self.save_path / f"{self.batch_id}.json"

        self.file_path.parent.mkdir(parents=True, exist_ok=True)

        with open(self.file_path, "w") as f:
            results_dict = self.results.to_json_dict()
            json.dump(results_dict, f, indent=2)
        return True

    def _assert_expectations(self):
        result = self.results.get("results")
        failures = [res for res in result if not res.get("success")]

        failure_count = len(failures)
        expectation_count = len(result)

        if failures:

            failed_expectations = [
                f"\t - Failed Expectation: {result.get('expectation_config').get('expectation_type')}\n"
                f"\t\t - Expectation Config: {result.get('expectation_config').get('kwargs')}\n"
                f"\t\t - Expectation Result: {result.get('result')}\n"
                for result in failures
            ]

            message = (
                f"\n\n{failure_count:d} expectations out of {expectation_count:d} were not met:\n",
                f"\n".join(failed_expectations),
                f"View logs of expectations at {self.file_path}",
            )

            assert False, "\n".join(message)
        else:
            print(
                f"""All Expectations Passed: {failure_count:d} expectations out of {expectation_count:d} were not met
                
                - View logs of expectations at {self.file_path}
                """
            )
            assert True

    def validate_and_save(self):
        self.set_validation_results()
        self.write_results_to_json()
        self._assert_expectations()

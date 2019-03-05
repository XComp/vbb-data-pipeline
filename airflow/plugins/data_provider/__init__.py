from airflow.plugins_manager import AirflowPlugin
from .operators import ExtractURLOperator, DownloadOperator, FakeDownloadOperator, ChecksumOperator


class DataProviderPlugin(AirflowPlugin):
    name = "data_provider_plugin"
    operators = [ExtractURLOperator, DownloadOperator, FakeDownloadOperator, ChecksumOperator]

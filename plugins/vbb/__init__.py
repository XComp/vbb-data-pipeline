from airflow.plugins_manager import AirflowPlugin
from .operators import CheckURIOperator, DownloadOperator, UnzipOperator, GZipOperator


class VBBPlugin(AirflowPlugin):
    name = "vbb_plugin"
    operators = [CheckURIOperator, DownloadOperator, UnzipOperator, GZipOperator]

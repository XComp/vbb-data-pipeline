from airflow.plugins_manager import AirflowPlugin
from .operators import ChecksumOperator, DownloadOperator, UnzipOperator, GZipOperator


class VBBPlugin(AirflowPlugin):
    name = "vbb_plugin"
    operators = [ChecksumOperator, DownloadOperator, UnzipOperator, GZipOperator]

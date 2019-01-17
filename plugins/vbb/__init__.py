from airflow.plugins_manager import AirflowPlugin
from .operators import CheckURLOperator, DownloadOperator, UnzipOperator, GZipOperator


class VBBPlugin(AirflowPlugin):
    name = "vbb_plugin"
    operators = [CheckURLOperator, DownloadOperator, UnzipOperator, GZipOperator]

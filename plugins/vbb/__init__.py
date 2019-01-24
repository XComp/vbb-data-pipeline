from airflow.plugins_manager import AirflowPlugin
from .operators import DagRunInitOperator, ExtractURLOperator, DownloadOperator, ChecksumOperator, UnzipOperator, \
    GZipOperator


class VBBPlugin(AirflowPlugin):
    name = "vbb_plugin"
    operators = [DagRunInitOperator,
                 ExtractURLOperator,
                 DownloadOperator,
                 ChecksumOperator,
                 UnzipOperator,
                 GZipOperator]

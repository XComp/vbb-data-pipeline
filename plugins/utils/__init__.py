from airflow.plugins_manager import AirflowPlugin
from .operators import DagInitOperator, DagRunInitOperator


class UtilsPlugin(AirflowPlugin):
    name = "utils"
    operators = [DagInitOperator, DagRunInitOperator]

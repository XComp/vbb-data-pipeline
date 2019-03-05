from airflow.plugins_manager import AirflowPlugin
from .operators import DataSelectOperator, NewDataIdentifyOperator, LoadNewDataOperator


class DatabasePlugin(AirflowPlugin):
    name = "database_plugin"
    operators = [DataSelectOperator, NewDataIdentifyOperator, LoadNewDataOperator]

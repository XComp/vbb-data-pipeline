from airflow.plugins_manager import AirflowPlugin
from .operators import CheckSchemaOperator, CreateSchemaOperator, Copy2DatabaseOperator


class DatabasePlugin(AirflowPlugin):
    name = "database_plugin"
    operators = [CheckSchemaOperator, CreateSchemaOperator, Copy2DatabaseOperator]

from os import listdir
from os.path import join, exists

from airflow.models import BaseOperator, SkipMixin
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.postgres_operator import PostgresHook
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults


class PostgresMixin:

    def __init__(self, postgres_conn_id, schema, *args, **kwargs):
        super(PostgresMixin, self).__init__(*args, **kwargs)

        self.postgres_conn_id = postgres_conn_id
        self.schema = schema

    def get_hook(self):
        return PostgresHook(postgres_conn_id=self.postgres_conn_id)


class CheckSchemaOperator(PostgresMixin, BranchPythonOperator):

    @apply_defaults
    def __init__(self, create_schema_task_id, load_data_task_id, *args, **kwargs):
        super(CheckSchemaOperator, self).__init__(python_callable=self._schema_exists, *args, **kwargs)

        self.create_schema_task_id = create_schema_task_id
        self.load_data_task_id = load_data_task_id

    def _schema_exists(self):
        hook = self.get_hook()
        rows = hook.get_records(
            sql="SELECT COUNT(schema_name) FROM information_schema.schemata WHERE schema_name = %s;",
            parameters=(self.schema,))

        self.log.info(rows)

        if not rows:
            raise ValueError("An error occurred while fetching the schema information")

        return self.create_schema_task_id if rows[0][0] == 0 else self.load_data_task_id


class CreateSchemaOperator(PostgresMixin, BaseOperator):

    def __init__(self, *args, **kwargs):
        super(CreateSchemaOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        sql_queries = ""
        with open("plugins/sql/schema.sql", "r") as schema_file:
            for line in schema_file:
                if not line.startswith("--"):
                    sql_queries += line

        hook = self.get_hook()
        hook.run(sql=sql_queries)

        for output in hook.conn.notices:
            self.log.info(output)


class Copy2DatabaseOperator(PostgresMixin, SkipMixin, BaseOperator):

    def __init__(self, base_folder, task_folder_name, *args, **kwargs):
        super(Copy2DatabaseOperator, self).__init__(*args, **kwargs)

        self.base_folder = base_folder
        self.task_folder_name = task_folder_name

    def _skip_downstream_tasks(self, context):
        downstream_tasks = context['task'].get_flat_relatives(upstream=False)
        self.log.debug("Downstream task_ids %s", downstream_tasks)

        if downstream_tasks:
            self.skip(context['dag_run'], context['ti'].execution_date, downstream_tasks)

    def execute(self, context):
        source_folder = join(self.base_folder, context["ds"], self.task_folder_name)
        if not exists(source_folder):
            self.log.info("No source data available.")
            self._skip_downstream_tasks(context=context)

            return

        sql_queries = []
        for file_name in listdir(self.source_folder):
            file_path = join(self.source_folder, file_name)
            table_name = file_name.rsplit(".", 1)[0]
            sql_queries.append("\\COPY %s FROM '%s' DELIMITER ';' CSV;".format(table_name, file_path))

        hook = self.get_hook()
        hook.run(sql=sql_queries)
        for output in hook.conn.notices:
            self.log.info(output)


class DatabasePlugin(AirflowPlugin):
    name = "database_plugin"
    operators = [CheckSchemaOperator, CreateSchemaOperator, Copy2DatabaseOperator]

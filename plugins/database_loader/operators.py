from os import listdir
from os.path import join, isfile

from airflow.models import BaseOperator
from airflow.operators.postgres_operator import PostgresHook
from airflow.utils.decorators import apply_defaults


class PostgresMixin:

    def __init__(self, postgres_conn_id, schema, *args, **kwargs):
        super(PostgresMixin, self).__init__(*args, **kwargs)

        self.postgres_conn_id = postgres_conn_id
        self.schema = schema

    def get_hook(self):
        return PostgresHook(postgres_conn_id=self.postgres_conn_id)


class DataSelectOperator(BaseOperator):

    @apply_defaults
    def __init__(self, base_folder: str, *args, **kwargs):
        super(DataSelectOperator, self).__init__(*args, **kwargs)

        self.base_folder = base_folder

    def execute(self, context):
        data = dict()
        for f in listdir(self.base_folder):
            p = join(self.base_folder, f)
            if isfile(p):
                self.log.info("{} is a file and will be ignored.".format(f))
                continue

            if f not in data:
                self.log.info("{} wasn't processed, yet.".format(f))
                data[f] = set()

            for zip_archive in listdir(p):
                if not zip_archive.endswith("zip"):
                    self.log.info("{} is not a ZIP archive and will be ignored.".format(zip_archive))
                    continue

                self.log.info("{} is going to be processed.".format(zip_archive))
                data[f].add(zip_archive.split(".")[0])

        return data


class NewDataIdentifyOperator(PostgresMixin, BaseOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(NewDataIdentifyOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        hook = self.get_hook()
        available_data = context["ti"].xcom_pull("data_select_task")

        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT provider_id, run_date FROM run;")

        row = cursor.fetchone()
        while row:
            provider_id = row[0]
            run_date = row[1]

            if provider_id in available_data:
                available_data[provider_id].discard(run_date)

                if not available_data[provider_id]:
                    # remove provider entry entirely if no data is left for it
                    available_data.pop(provider_id, None)

            row = cursor.fetchone()

        return available_data


class LoadNewDataOperator(PostgresMixin, BaseOperator):

    @apply_defaults
    def __init__(self, base_folder: str, *args, **kwargs):
        super(LoadNewDataOperator, self).__init__(*args, **kwargs)

        self.base_folder = base_folder

    def execute(self, context):
        new_data = context["ti"].xcom_pull("new_data_task")

        self.log.info("The following files are going to be loaded:")
        for provider_id, run_dates in new_data.items():
            for run_date in run_dates:
                self.log.info("  {}/{}/{}.zip".format(self.base_folder, provider_id, run_date))

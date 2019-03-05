from os import listdir
from os.path import join, isfile
from zipfile import ZipFile

import csv
import io
import re

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

        assert new_data, "No new data was detected."

        self.log.info("The following files are going to be loaded:")
        for provider_id, run_dates in new_data.items():
            for run_date in run_dates:
                zip_archive_path = "{}/{}/{}.zip".format(self.base_folder, provider_id, run_date)
                self.log.info(zip_archive_path)

                self.load(zip_archive_path=zip_archive_path, provider_id=provider_id, run_date=run_date)

    @staticmethod
    def get_row_count(cursor, table_name):
        cursor.execute("SELECT COUNT(*) FROM {}".format(table_name))
        return cursor.fetchone()[0]

    def load(self, zip_archive_path, provider_id, run_date):
        with self.get_hook().get_conn() as conn:
            with conn, conn.cursor() as cursor:
                # insert provider if it does not exist, yet
                provider_query = """INSERT INTO gtfs.provider (provider_id, created) 
                                    VALUES ('{}', NOW()) ON CONFLICT DO NOTHING;""".format(provider_id)
                cursor.execute(provider_query)

                # insert run record returning the new ID
                run_query = """INSERT INTO gtfs.run (run_date, provider_id) 
                               VALUES ('{}', '{}') RETURNING run_id;""".format(run_date, provider_id)
                cursor.execute(run_query)
                run_id = cursor.fetchone()[0]

                # retrieving the available tables
                cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'gtfs';")
                available_tables = [row[0] for row in cursor.fetchall()]

                # extract data from zip archive
                # we need to apply some ordering to ensure that the foreign key constraint never fails
                zip_member_order = {"agency.txt": 0, "calendar.txt": 0, "shapes.txt": 0, "stops.txt": 0,
                                    "calendar_dates.txt": 1, "routes.txt": 1,
                                    "trips.txt": 2,
                                    "frequencies.txt": 3, "stop_times.txt": 3, "transfers.txt": 3}
                with ZipFile(zip_archive_path, "r") as zip_archive:
                    for zip_member in sorted(zip_archive.namelist(), key=lambda k: zip_member_order.get(k, 9999)):
                        table_name = zip_member.split(".")[0]

                        # don't load data if the table does not exist
                        if table_name not in available_tables:
                            self.log.warn("Table '{}' is not initialized in the database.".format(table_name))
                            continue

                        old_row_count = LoadNewDataOperator.get_row_count(cursor=cursor, table_name=table_name)

                        # process CSV file
                        with zip_archive.open(zip_member, "r") as csv_file:

                            def insert_data(cols, d_cache):
                                if not d_cache:
                                    return

                                insert_query = """INSERT INTO gtfs.{} ({}) VALUES {} 
                                                  ON CONFLICT DO NOTHING""".format(
                                    table_name,
                                    ",".join(cols),
                                    ",".join(d_cache)
                                )
                                cursor.execute(insert_query)

                            csv_reader = csv.DictReader(io.TextIOWrapper(csv_file))
                            columns = ["run_id", "provider_id"]
                            data_cache = []
                            for i, row in enumerate(csv_reader):
                                if len(columns) < 3:
                                    # we have to clean the fields from special characters - KVV has strange characters
                                    # in the header
                                    columns.extend([re.sub(r'[^a-z,_]', "", field.strip()) for field in row])
                                data_cache.append("({}, '{}', {})".format(
                                    run_id,
                                    provider_id,
                                    ",".join(["'{}'".format(value) if value else "NULL" for _, value in row.items()])
                                ))

                                if i % 50000 == 0:
                                    insert_data(cols=columns, d_cache=data_cache)
                                    self.log.debug("50000 rows inserted into {}.".format(table_name))
                                    data_cache = []

                            insert_data(cols=columns, d_cache=data_cache)

                        new_row_count = LoadNewDataOperator.get_row_count(cursor=cursor, table_name=table_name)

                        self.log.info("Finished processing {}/{}/{}: {} rows added"
                                      .format(provider_id, run_date, zip_member, new_row_count - old_row_count))

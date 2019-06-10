from contextlib import closing

from airflow.operators.postgres_operator import PostgresHook


class ExtendedPostgresHook(PostgresHook):

    def __init__(self, postgres_conn_id, schema, *args, **kwargs):
        super(PostgresHook, self).__init__(*args, **kwargs)

        self.postgres_conn_id = postgres_conn_id
        self.schema = schema

    def get_row_count(self, table_name):
        return self.get_first("SELECT COUNT(*) FROM {}.{}".format(self.schema, table_name))[0]

    def insert_rows(self, table, rows, target_fields=None, commit_every=1000, sql_ext=""):
        if target_fields:
            target_fields = ", ".join(target_fields)
            target_fields = "({})".format(target_fields)
        else:
            target_fields = ''
        i = 0
        with closing(self.get_conn()) as conn:
            if self.supports_autocommit:
                self.set_autocommit(conn, False)

            conn.commit()

            with closing(conn.cursor()) as cur:
                for i, row in enumerate(rows, 1):
                    lst = []
                    for cell in row:
                        lst.append(self._serialize_cell(cell, conn))
                    values = tuple(lst)
                    placeholders = ["%s", ] * len(values)
                    sql = "INSERT INTO {}.{} {} VALUES ({}) {}".format(
                        self.schema,
                        table,
                        target_fields,
                        ",".join(placeholders),
                        sql_ext
                    )
                    cur.execute(sql, values)
                    if commit_every and i % commit_every == 0:
                        conn.commit()
                        self.log.info(
                            "Loaded %s into %s rows so far", i, table
                        )

            conn.commit()

        self.log.info("Done loading. Loaded a total of %s rows", i)

    def insert_rows_ignore_on_conflict(self, table, rows, target_fields=None, commit_every=1000):
        self.insert_rows(table, rows, target_fields, commit_every, sql_ext=" ON CONFLICT DO NOTHING;")

    def insert_row_and_get_id(self, table, target_fields, data, id_field):
        query = "INSERT INTO {}.{} ({}) VALUES ('{}') RETURNING {}".format(
            self.schema,
            table,
            ", ".join(target_fields),
            "', '".join(data),
            id_field
        )
        with closing(self.get_conn()) as conn:
            with closing(conn.cursor()) as cur:
                cur.execute(query)
                returned_id = cur.fetchone()
            conn.commit()

        return returned_id[0]

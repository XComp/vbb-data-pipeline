from os import makedirs
from os.path import join, exists

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class PipelineOperator(BaseOperator):

    @apply_defaults
    def __init__(self, provider_id: str, create_task_folder: bool = True, *args, **kwargs):
        super(PipelineOperator, self).__init__(*args, **kwargs)

        self.provider_id = provider_id
        self.dag_folder = None
        self.dagrun_folder = None
        self.task_folder = None
        self.create_task_folder = create_task_folder

    def execute(self, context):
        self.dag_folder = context["task_instance"].xcom_pull(dag_id="gtfs_pipeline",
                                                             task_ids="init_dag_task",
                                                             key="dag_folder")
        self.dagrun_folder = context["task_instance"].xcom_pull(dag_id="gtfs_pipeline",
                                                                task_ids="init_dagrun_task",
                                                                key="dagrun_folder")

        if self.create_task_folder:
            self.task_folder = join(self.dagrun_folder, self.attach_provider_prefix(self.task_id))
            makedirs(self.task_folder)
            self.log.info("Created '{}'...".format(self.task_folder))

        return self._execute_with_folder(context)

    def get_provider_id(self):
        return self.provider_id

    def attach_provider_prefix(self, text):
        return "{}.{}".format(self.get_provider_id(), text)

    def get_dag_folder(self):
        if not self.dag_folder:
            raise ValueError("The DAG folder was not initialized.")

        return self.dag_folder

    def get_dagrun_folder(self):
        if not self.dagrun_folder:
            raise ValueError("The DagRun folder was not initialized.")

        return self.dagrun_folder

    def get_task_folder(self):
        if not self.task_folder:
            raise ValueError("The Task folder was not initialized.")

        return self.task_folder

    def _execute_with_folder(self, context):
        raise NotImplementedError()


class DagInitOperator(BaseOperator):

    @apply_defaults
    def __init__(self, base_folder: str, *args, **kwargs):
        super(DagInitOperator, self).__init__(*args, **kwargs)

        self.base_folder = base_folder

    def execute(self, context):
        dag_folder = join(self.base_folder, self.dag_id)

        if not exists(dag_folder):
            makedirs(dag_folder)
            self.log.info("Created '{}'...".format(dag_folder))

        context["task_instance"].xcom_push(key="dag_folder", value=dag_folder)


class DagRunInitOperator(BaseOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(DagRunInitOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        dag_folder = context["task_instance"]\
            .xcom_pull(dag_id="gtfs_pipeline", task_ids="init_dag_task", key="dag_folder")

        if not dag_folder or not exists(dag_folder):
            raise ValueError("The Dag folder {} does not exist...".format(dag_folder))

        dagrun_folder = join(dag_folder, context["ds"])

        if exists(dagrun_folder):
            raise ValueError("The DagRun folder {} does already exist...".format(dagrun_folder))

        makedirs(dagrun_folder)
        self.log.info("Created '{}'...".format(dagrun_folder))

        context["task_instance"].xcom_push(key="dagrun_folder", value=dagrun_folder)

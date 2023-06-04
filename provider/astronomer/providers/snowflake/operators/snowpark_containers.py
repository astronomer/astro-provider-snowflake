from __future__ import annotations

from typing import TYPE_CHECKING, Any
import json

from typing import Any, Callable, Collection, Iterable, Mapping
import aiohttp
import asyncio

from airflow.models import BaseOperator, BaseOperatorLink
from airflow.exceptions import AirflowException
from airflow.operators.python import _BasePythonVirtualenvOperator
from airflow.utils.context import Context, context_copy_partial

from astronomer.providers.snowflake.hooks.snowpark_containers import SnowparkContainersHook
from astronomer.providers.snowflake.operators.snowpark import _BaseSnowparkOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context

class SnowparkContainersOperatorExtraLink(BaseOperatorLink):

    name = "Astronomer Registry"
    
    def get_link(self, operator: BaseOperator, *, ti_key=None):
        return "https://registry.astronomer.io"

class SnowparkContainersPythonOperator(_BaseSnowparkOperator):
    """
    Runs a function in a Snowpark Container runner service.  

    The function must be defined using def, and not be
    part of a class. All imports must happen inside the function
    and no variables outside the scope may be referenced. A global scope
    variable named virtualenv_string_args will be available (populated by
    string_args). In addition, one can pass stuff through op_args and op_kwargs, and one
    can use a return value.
    
    #TODO: Update after runner change to non-airflow
    The runner should use the Snowflake custom XCOM backend (part of this provider) since the Snowpark Container 
    Service state cannot be ensured.  Alternatively the local file xcom backend could be used to a volume mounted
    Snowflake stage.
    
    :param snowflake_conn_id: connection to use when running code within the Snowpark Container runner service.
    :type snowflake_conn_id: str  (default is snowflake_default)
    :param runner_service_name: Name of Airflow runner service in Snowpark Container services.  Must specify 
    runner_service_name or endpoint
    :type runner_service_name: str
    :param endpoint: Endpoint URL of the instantiated Snowpark Container runner.  Must specify endpoint or 
    runner_service_name.
    :type endpoint: str
    :param headers: Optional OAUTH bearer token for Snowpark Container runner.  If runner_service_name is 
    specified SnowparkContainersHook() will be used to pull the token just before running the task.
    :type headers: str
    :param python_callable: Function to decorate
    :type python_callable: Callable 
    :param python_version: Python version (ie. '<maj>.<min>').  Callable will run in a PythonVirtualenvOperator on the runner.  
        If not set will use default python version on runner.
    :type python_version: str:
    :param requirements: Optional list of python dependencies or a path to a requirements.txt file to be installed for the callable.
    :type requirements: list | str
    :param temp_data_output: If set to 'stage' or 'table' Snowpark DataFrame objects returned
        from the operator will be serialized to the stage specified by 'temp_data_stage' or
        a table with prefix 'temp_data_table_prefix'.
    :param temp_data_db: The database to be used in serializing temporary Snowpark DataFrames. If
        not set the operator will use the database set at the operator or hook level.  If None, 
        the operator will assume a default database is set in the Snowflake user preferences.
    :param temp_data_schema: The schema to be used in serializing temporary Snowpark DataFrames. If
        not set the operator will use the schema set at the operator or hook level.  If None, 
        the operator will assume a default schema is set in the Snowflake user preferences.
    :param temp_data_stage: The stage to be used in serializing temporary Snowpark DataFrames. This
        must be set if temp_data_output == 'stage'.  Output location will be named for the task:
        <DATABASE>.<SCHEMA>.<STAGE>/<DAG_ID>/<TASK_ID>/<RUN_ID>
        
        and a uri will be returned to Airflow xcom:
        
        snowflake://<ACCOUNT>.<REGION>?&stage=<FQ_STAGE>&key=<DAG_ID>/<TASK_ID>/<RUN_ID>/0/return_value.parquet'

    :param temp_data_table_prefix: The prefix name to use for serialized Snowpark DataFrames. This
        must be set if temp_data_output == 'table'. Default: "XCOM_"

        Output table will be named for the task:
        <DATABASE>.<SCHEMA>.<PREFIX><DAG_ID>__<TASK_ID>__<TS_NODASH>_INDEX

        and the return value set to a SnowparkTable object with the fully-qualified table name.
        
        SnowparkTable(name=<DATABASE>.<SCHEMA>.<PREFIX><DAG_ID>__<TASK_ID>__<TS_NODASH>_INDEX)

    :param temp_data_overwrite: boolean.  Whether to overwrite existing temp data or error.
    :param op_kwargs: a dictionary of keyword arguments that will get unpacked in your function (templated)
    :type op_kwargs: dict
    :param op_args: a list of positional arguments that will get unpacked when calling your callable (templated)
    :type op_args: list
    :param string_args: Strings that are present in the global var virtualenv_string_args,
        available to python_callable at runtime as a list[str]. Note that args are split
        by newline.
    :type string_args: list
    :param templates_dict: a dictionary where the values are templates that
        will get templated by the Airflow engine sometime between
        ``__init__`` and ``execute`` takes place and are made available
        in your callable's context after the template has been applied
    :type templates_dict: dict
    :param templates_exts: a list of file extensions to resolve while
        processing templated fields, for examples ``['.sql', '.hql']``
    :type templates_exts: list
    :param expect_airflow: expect Airflow to be installed in the target environment. If true, the operator
        will raise warning if Airflow is not installed, and it will attempt to load Airflow
        macros when starting.
    :type expect_airflow: bool
    """

    #template_fields: Sequence[str] = tuple({"python"} | set(PythonOperator.template_fields))

    def __init__(
        self,
        *,
        python_callable: Callable,
        runner_service_name:str | None = None,
        endpoint: str | None = None,
        headers: dict | None = None,
        snowflake_conn_id: str = 'snowflake_default',
        log_level: str = 'ERROR',
        python_version: str | None = None,
        requirements: Iterable[str] | str = [],
        # use_dill: bool = False,
        pip_install_options: list[str] = [],
        op_args: Collection[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        string_args: Iterable[str] | None = None,
        # templates_dict: dict | None = None,
        # templates_exts: list[str] | None = None,
        # expect_airflow: bool = False,
        # expect_pendulum: bool = False,
        **kwargs,
    ):
        assert runner_service_name or endpoint, "Must specify 'runner_service_name' or 'endpoint'"

        if isinstance(requirements, str):
            try: 
                with open(requirements, 'r') as requirements_file:
                    self.requirements = requirements_file.read().splitlines()
            except:
                raise FileNotFoundError(f'Specified requirements file {requirements} does not exist or is not readable.')
        else:
            assert isinstance(requirements, list), "requirements must be a list or filename."
            self.requirements = requirements

        self.log_level = log_level                
        self.endpoint=endpoint
        self.headers=headers
        self.runner_service_name=runner_service_name
        self.pip_install_options = pip_install_options
        # self.use_dill = use_dill
        # self.snowflake_user_conn_params = hook._get_conn_params()
        self.python_version = python_version
        # self.expect_airflow = expect_airflow
        # self.expect_pendulum = expect_pendulum
        self.string_args = string_args
        # self.templates_dict = templates_dict
        # self.templates_exts = templates_exts
        self.system_site_packages = True
        
        super().__init__(
            python_callable=python_callable,
            snowflake_conn_id=snowflake_conn_id,
            # use_dill = use_dill,
            op_args=op_args,
            op_kwargs=op_kwargs,
            string_args=string_args,
            # templates_dict=templates_dict,
            # templates_exts=templates_exts,
            # expect_airflow = expect_airflow,
            **kwargs,
        )

    def _build_payload(self, context):

        payload = dict(
            python_callable_str = self.get_python_source(), 
            python_callable_name = self.python_callable.__name__,
            log_level = self.log_level,
            requirements = self.requirements,
            pip_install_options = self.pip_install_options,
            snowflake_user_conn_params = self.conn_params,
            temp_data_dict = self.temp_data_dict,
            # use_dill = self.use_dill,
            system_site_packages = self.system_site_packages,
            python_version = self.python_version,
            dag_id = self.dag_id,
            task_id = self.task_id,
            run_id = context['run_id'],
            ts_nodash = context['ts_nodash'],
            # task_start_date = context["ts"],
            op_args = self.op_args,
            op_kwargs = self.op_kwargs,
            # templates_dict = self.templates_dict,
            # templates_exts = self.templates_exts,
            # expect_airflow = self.expect_airflow,
            # expect_pendulum = self.expect_pendulum,
            string_args = self.string_args,
        )
        
        return payload

    def _iter_serializable_context_keys(self):
        yield from self.BASE_SERIALIZABLE_CONTEXT_KEYS
        if self.system_site_packages or "apache-airflow" in self.requirements:
            yield from self.AIRFLOW_SERIALIZABLE_CONTEXT_KEYS
            yield from self.PENDULUM_SERIALIZABLE_CONTEXT_KEYS
        elif "pendulum" in self.requirements:
            yield from self.PENDULUM_SERIALIZABLE_CONTEXT_KEYS

    def execute(self, context: Context) -> Any:
        serializable_keys = set(self._iter_serializable_context_keys())
        serializable_context = context_copy_partial(context, serializable_keys)

        self.payload = self._build_payload(context)

        return super().execute(context=serializable_context)

    def execute_callable(self):

        responses = asyncio.run(self._execute_python_callable_in_snowpark_container())

        return responses
        
    async def _execute_python_callable_in_snowpark_container(self):

        if self.runner_service_name:
            # hook=SnowparkContainersHook(*[f'{k}={v}' for k, v in self.conn_params.items()], session_parameters={'PYTHON_CONNECTOR_QUERY_RESULT_FORMAT': 'json'})
            hook=SnowparkContainersHook(*self.conn_params, session_parameters={'PYTHON_CONNECTOR_QUERY_RESULT_FORMAT': 'json'})
            urls, self.headers = hook.get_service_urls(service_name=self.runner_service_name)
            self.endpoint = urls['runner']+'/task'

        print(f"""
        __________________________________
        Running function {self.payload['python_callable_name']} in Snowpark Containers 
        Task: {self.task_id}
        Airflow Task Runner: {self.endpoint}
        __________________________________
        """)

       
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(self.endpoint, headers=self.headers) as ws:
                await ws.send_json(self.payload)

                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        response = json.loads(msg.data)

                        if response['type'] in ["log", "execution_log", "infra"]:
                            [self.log.info(line) for line in response['output'].splitlines()]
                        
                        if response['type'] == "error":
                            [self.log.error(line) for line in response['output'].splitlines()]
                            raise AirflowException("Error occurred in Task run.")

                        if  response['type'] == 'results':
                            return json.loads(response['output'])
                                              
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        break

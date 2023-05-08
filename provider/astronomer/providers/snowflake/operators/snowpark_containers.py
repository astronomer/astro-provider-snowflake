from __future__ import annotations

from typing import TYPE_CHECKING, Any

from typing import Any, Callable, Collection, Iterable, Mapping
import aiohttp
import asyncio

from airflow.models import BaseOperator, BaseOperatorLink
from airflow.exceptions import AirflowException
from airflow.operators.python import _BasePythonVirtualenvOperator
from airflow.utils.context import Context, context_copy_partial

from astronomer.providers.snowflake.hooks.snowpark_containers import SnowparkContainersHook

if TYPE_CHECKING:
    from airflow.utils.context import Context

class SnowparkContainersOperatorExtraLink(BaseOperatorLink):

    name = "Astronomer Registry"
    
    def get_link(self, operator: BaseOperator, *, ti_key=None):
        return "https://registry.astronomer.io"

class SnowparkContainersPythonOperator(_BasePythonVirtualenvOperator):
    """
    Runs a function in a Snowpark Container runner service environment.  

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
    :param runner_endpoint: URL endpoint of the instantiated Snowpark Container runner (ie. ws://<hostnam>:<port>/<endpoint>)
    :type runner_endpoint: str
    :param python_callable: Function to decorate
    :type python_callable: Callable 
    :param python: Python version (ie. '<maj>.<min>').  Callable will run in a PythonVirtualenvOperator on the runner.  
        If not set will use default python version on runner.
    :type python: str:
    :param requirements: Optional list of python dependencies or a path to a requirements.txt file to be installed for the callable.
    :type requirements: list | str
    :param op_kwargs: a dictionary of keyword arguments that will get unpacked in your function (templated)
    :param op_args: a list of positional arguments that will get unpacked when calling your callable (templated)
    :param string_args: Strings that are present in the global var virtualenv_string_args,
        available to python_callable at runtime as a list[str]. Note that args are split
        by newline.
    :param templates_dict: a dictionary where the values are templates that
        will get templated by the Airflow engine sometime between
        ``__init__`` and ``execute`` takes place and are made available
        in your callable's context after the template has been applied
    :param templates_exts: a list of file extensions to resolve while
        processing templated fields, for examples ``['.sql', '.hql']``
    :param expect_airflow: expect Airflow to be installed in the target environment. If true, the operator
        will raise warning if Airflow is not installed, and it will attempt to load Airflow
        macros when starting.
    """

    #template_fields: Sequence[str] = tuple({"python"} | set(PythonOperator.template_fields))

    def __init__(
        self,
        *,
        runner_endpoint: str, 
        python_callable: Callable,
        snowflake_conn_id: str = 'snowflake_default',
        python: str | None = None,
        requirements: None | Iterable[str] | str = None,
        use_dill: bool = False,
        pip_install_options: list[str] | None = None,
        op_args: Collection[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        string_args: Iterable[str] | None = None,
        templates_dict: dict | None = None,
        templates_exts: list[str] | None = None,
        expect_airflow: bool = True,
        expect_pendulum: bool = False,
        **kwargs,
    ):

        if not requirements:
            self.requirements = []
        elif isinstance(requirements, str):
            try: 
                with open(requirements, 'r') as requirements_file:
                    self.requirements = requirements_file.read().splitlines()
            except:
                raise FileNotFoundError(f'Specified requirements file {requirements} does not exist or is not readable.')
        else:
            assert isinstance(requirements, list), "requirements must be a list or filename."
            self.requirements = requirements
        
        hook = SnowparkContainersHook(snowflake_conn_id=snowflake_conn_id)

        self.pip_install_options = pip_install_options
        self.use_dill = use_dill
        self.AIRFLOW_CONN_SNOWFLAKE_USER = hook._get_uri_from_conn_params()
        self.python = python
        self.expect_airflow = expect_airflow
        self.expect_pendulum = expect_pendulum
        self.string_args = string_args
        self.templates_dict = templates_dict
        self.templates_exts = templates_exts
        self.runner_endpoint = runner_endpoint
        self.system_site_packages = True
        
        super().__init__(
            python_callable=python_callable,
            use_dill = use_dill,
            op_args=op_args,
            op_kwargs=op_kwargs,
            string_args=string_args,
            templates_dict=templates_dict,
            templates_exts=templates_exts,
            expect_airflow = expect_airflow,
            **kwargs,
        )

    def _build_payload(self, context):

        payload = dict(
            python_callable_str = self.get_python_source(), 
            python_callable_name = self.python_callable.__name__,
            requirements = self.requirements,
            pip_install_options = self.pip_install_options,
            AIRFLOW_CONN_SNOWFLAKE_USER = self.AIRFLOW_CONN_SNOWFLAKE_USER,
            use_dill = self.use_dill,
            system_site_packages = self.system_site_packages,
            python = self.python,
            dag_id = self.dag_id,
            task_id = self.task_id,
            task_start_date = context["ts"],
            op_args = self.op_args,
            op_kwargs = self.op_kwargs,
            templates_dict = self.templates_dict,
            templates_exts = self.templates_exts,
            expect_airflow = self.expect_airflow,
            expect_pendulum = self.expect_pendulum,
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

        responses =  asyncio.run(self._execute_python_callable_in_snowpark_container(self.payload))

        return responses
        
    async def _execute_python_callable_in_snowpark_container(self, payload):

        responses = []
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(self.runner_endpoint) as websocket_runner:
                await websocket_runner.send_json(payload)

                while True:
                    response = await websocket_runner.receive_json()

                    assert response != None

                    if response['type'] == 'results':
                        self.log.info(response)
                        return response['output']
                    elif response['type'] in ["log", "execution_log", "infra"]:
                        self.log.info(response)
                    elif response['type'] == "error":
                        self.log.error(response)
                        raise AirflowException("Error occurred in Task run.")
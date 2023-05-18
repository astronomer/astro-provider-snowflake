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
    :param endpoint: Endpoint URL of the instantiated Snowpark Container runner.  
    :type endpoint: str
    :param headers: Optional OAUTH bearer token for Snowpark Container runner.  In local_test mode this can be None.
    :type headers: str
    :param python_callable: Function to decorate
    :type python_callable: Callable 
    :param python: Python version (ie. '<maj>.<min>').  Callable will run in a PythonVirtualenvOperator on the runner.  
        If not set will use default python version on runner.
    :type python: str:
    :param requirements: Optional list of python dependencies or a path to a requirements.txt file to be installed for the callable.
    :type requirements: list | str
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
        endpoint: str,         
        python_callable: Callable,
        headers: dict | None = None,
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
                
        self.endpoint = endpoint
        self.headers=headers
        self.pip_install_options = pip_install_options
        self.use_dill = use_dill
        self.AIRFLOW_CONN_SNOWFLAKE_USER = hook._get_uri_from_conn_params()
        self.python = python
        self.expect_airflow = expect_airflow
        self.expect_pendulum = expect_pendulum
        self.string_args = string_args
        self.templates_dict = templates_dict
        self.templates_exts = templates_exts
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
        # import requests
        # print(self.headers)
        # print(self.endpoint)
        # print('##################')
        # print(requests.get(url=self.endpoint.split('/task')[0], headers=self.headers))
        # print('##################')
        # urls, self.headers = SnowparkContainersHook().get_service_urls(service_name='runner')
        # self.endpoint = urls['runner']+'/task'
        # print(self.headers)
        # print(self.endpoint)
        # print('##################')
        # print(requests.get(url=self.endpoint.split('/task')[0], headers=self.headers))
        # print('##################')

        responses = []
        async with aiohttp.ClientSession(headers=self.headers) as session:
            async with session.ws_connect(self.endpoint) as websocket_runner:
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
                    

# def test():
#     from include.myfunc import myfunc1, myfunc3
#     from astronomer.providers.snowflake.hooks.snowpark_containers import SnowparkContainersHook
#     from astronomer.providers.snowflake.operators.snowpark_containers import SnowparkContainersPythonOperator
#     _SNOWFLAKE_CONN_ID='snowflake_default'
#     _LOCAL_MODE = None #'astro_cli'
#     if _LOCAL_MODE != 'astro_cli':
#         urls, runner_headers = SnowparkContainersHook(_SNOWFLAKE_CONN_ID).get_service_urls(service_name='runner')
#         runner_conn = {'endpoint': urls['runner']+'/task', 'headers': runner_headers}

#         urls, weaviate_headers = SnowparkContainersHook(_SNOWFLAKE_CONN_ID).get_service_urls(service_name='weaviate')
#         weaviate_conn = {'endpoint': urls['weaviate'], 'headers': weaviate_headers}
#     else:
#         runner_conn = {'endpoint': 'http://host.docker.internal:8001/task', 'headers': None}
#         weaviate_conn = {'endpoint': 'http://host.docker.internal:8081', 'headers': None}
    
#     self=SnowparkContainersPythonOperator(task_id='test', endpoint=runner_conn['endpoint'], headers=runner_conn['headers'], python_callable=myfunc3)
#     self.payload=self._build_payload(context={'ts':"2022-01-01T00:00:00Z00:00"})
#     self.execute_callable()
    
    
    # import requests
    # requests.get(url=runner_endpoint.split('/task')[0], headers=headers).text
    # session=aiohttp.ClientSession(headers=self.headers)
    # websocket_runner=session.ws_connect(self.runner_endpoint)
    # websocket_runner.send(json.dumps(self.payload))

    # self.execute_callable()

    # #!/usr/bin/env python

    # import asyncio
    # from websockets.sync.client import connect

    # def hello():
    #     with connect("ws://localhost:8001/test") as websocket:
    #         websocket.send("Hello world!")
    #         message = websocket.recv()
    #         print(f"Received: {message}")

    # hello()
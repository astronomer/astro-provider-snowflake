from __future__ import annotations

import inspect
from textwrap import dedent
from typing import Callable, Sequence

from airflow.decorators.base import DecoratedOperator, TaskDecorator, task_decorator_factory

from include.provider.astronomer.providers.snowflake.operators.snowpark_containers import SnowparkContainersPythonOperator

from airflow.utils.decorators import remove_task_decorator

class SnowparkContainersPythonDecoratedOperator(DecoratedOperator, SnowparkContainersPythonOperator):
    """
    Wraps a Python callable and captures args/kwargs when called for execution.

    """

    template_fields: Sequence[str] = ("op_args", "op_kwargs")
    template_fields_renderers = {"op_args": "py", "op_kwargs": "py"}

    # since we won't mutate the arguments, we should just do the shallow copy
    # there are some cases we can't deepcopy the objects (e.g protobuf).
    shallow_copy_attrs: Sequence[str] = ("python_callable",)

    custom_operator_name: str = "@task.snowpark_containers_python"

    def __init__(self, *, runner_service_name, endpoint, headers, python_callable, python_version, op_args, op_kwargs, **kwargs) -> None:
        kwargs_to_upstream = {
            "runner_service_name": runner_service_name,
            "endpoint": endpoint,
            "headers": headers,
            "python_callable": python_callable,
            "python_version": python_version,
            "op_args": op_args,
            "op_kwargs": op_kwargs,
        }
        super().__init__(
            kwargs_to_upstream=kwargs_to_upstream,
            python_callable=python_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
            **kwargs,
        )

    def get_python_source(self):
        raw_source = inspect.getsource(self.python_callable)
        res = dedent(raw_source)
        res = remove_task_decorator(res, self.custom_operator_name)
        return res

def snowpark_containers_python_task(
    runner_service_name: str | None = None,
    endpoint: str | None = None,
    headers: str | None = None,
    python_version: str | None = None,
    python_callable: Callable | None = None,
    snowflake_conn_id:str = 'snowflake_default',
    multiple_outputs: bool | None = None,
    **kwargs,
) -> TaskDecorator:
    """Wraps a python callable into an Airflow operator to run via a Snowpark Container runner service.

    Accepts kwargs for operator kwarg. Can be reused in a single DAG.

    This function is only used during type checking or auto-completion.

    :meta private:
    
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
    :param log_level: Set log level for Snowflake logging.  Default: 'ERROR'
    :type log_level: str
    :param temp_data_output: If set to 'stage' or 'table' Snowpark DataFrame objects returned
        from the operator will be serialized to the stage specified by 'temp_data_stage' or
        a table with prefix 'temp_data_table_prefix'.
    :type temp_data_output: str
    :param temp_data_db: The database to be used in serializing temporary Snowpark DataFrames. If
        not set the operator will use the database set at the operator or hook level.  If None, 
        the operator will assume a default database is set in the Snowflake user preferences.
    :type temp_data_db: str
    :param temp_data_schema: The schema to be used in serializing temporary Snowpark DataFrames. If
        not set the operator will use the schema set at the operator or hook level.  If None, 
        the operator will assume a default schema is set in the Snowflake user preferences.
    :type temp_data_schema: str
    :param temp_data_stage: The stage to be used in serializing temporary Snowpark DataFrames. This
        must be set if temp_data_output == 'stage'.  Output location will be named for the task:
        <DATABASE>.<SCHEMA>.<STAGE>/<DAG_ID>/<TASK_ID>/<RUN_ID>
        
        and a uri will be returned to Airflow xcom:
        
        snowflake://<ACCOUNT>.<REGION>?&stage=<FQ_STAGE>&key=<DAG_ID>/<TASK_ID>/<RUN_ID>/0/return_value.parquet'
    :type temp_data_stage: str
    :param temp_data_table_prefix: The prefix name to use for serialized Snowpark DataFrames. This
        must be set if temp_data_output == 'table'. Default: "XCOM_"

        Output table will be named for the task:
        <DATABASE>.<SCHEMA>.<PREFIX><DAG_ID>__<TASK_ID>__<TS_NODASH>_INDEX

        and the return value set to a SnowparkTable object with the fully-qualified table name.
        
        SnowparkTable(name=<DATABASE>.<SCHEMA>.<PREFIX><DAG_ID>__<TASK_ID>__<TS_NODASH>_INDEX)
    :type temp_data_table_prefix: str
    :param temp_data_overwrite: Whether to overwrite existing temp data or error.
    :type temp_data_overwrite: bool
    :param multiple_outputs: If set to True, the decorated function's return value will be unrolled to
        multiple XCom values. Dict will unroll to XCom values with its keys as XCom keys.
        Defaults to False.
    :type multiple_outputs: bool
    """
    return task_decorator_factory(
        runner_service_name=runner_service_name,
        endpoint=endpoint,
        headers=headers, 
        python_version=python_version,
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=SnowparkContainersPythonDecoratedOperator,
        **kwargs,
    )
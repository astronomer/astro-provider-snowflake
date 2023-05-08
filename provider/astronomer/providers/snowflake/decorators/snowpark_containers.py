from __future__ import annotations

import inspect
from textwrap import dedent
from typing import Callable, Sequence

from airflow.decorators.base import DecoratedOperator, TaskDecorator, task_decorator_factory

from astronomer.providers.snowflake.operators.snowpark_containers import SnowparkContainersPythonOperator

from airflow.utils.decorators import remove_task_decorator

class SnowparkContainerPythonDecoratedOperator(DecoratedOperator, SnowparkContainersPythonOperator):
    """
    Wraps a Python callable and captures args/kwargs when called for execution.

    TODO: Update docs
     :param snowflake_conn_id: connection to use when running code within the Snowpark Container runner service.
    :type snowflake_conn_id: str  (default is snowflake_default)
    :param runner_endpoint: URL endpoint of the instantiated Snowpark Container runner service 
        (ie. ws://<hostnam>:<port>/<endpoint>)
    :type runner_endpoint: str
    :param python_callable: Function to decorate
    :type python_callable: Callable 
    :param python: Python version (ie. '<maj>.<min>').  Callable will run similar to PythonVirtualenvOperator 
        on the runner.  If not set will use default python version on runner.
    :type python: str:
    :param requirements: List of python dependencies to be installed for the callable.
    :type requirements: list
    
    :param op_kwargs: a dictionary of keyword arguments that will get unpacked
        in your function (templated)
    :param op_args: a list of positional arguments that will get unpacked when
        calling your callable (templated)
    :param multiple_outputs: If set to True, the decorated function's return value will be unrolled to
        multiple XCom values. Dict will unroll to XCom values with its keys as XCom keys. Defaults to False.
    """

    template_fields: Sequence[str] = ("op_args", "op_kwargs")
    template_fields_renderers = {"op_args": "py", "op_kwargs": "py"}

    # since we won't mutate the arguments, we should just do the shallow copy
    # there are some cases we can't deepcopy the objects (e.g protobuf).
    shallow_copy_attrs: Sequence[str] = ("python_callable",)

    custom_operator_name: str = "@snowsparkcontainer_python_task"

    def __init__(self, *, runner_endpoint, python_callable, python, op_args, op_kwargs, **kwargs) -> None:
        kwargs_to_upstream = {
            "runner_endpoint": runner_endpoint,
            "python_callable": python_callable,
            "python": python,
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

def snowsparkcontainer_python_task(
    runner_endpoint: str, 
    python: str | None = None,
    python_callable: Callable | None = None,
    multiple_outputs: bool | None = None,
    **kwargs,
) -> TaskDecorator:
    """Wraps a python callable into an Airflow operator to run via a Snowpark Container runner service.

    Accepts kwargs for operator kwarg. Can be reused in a single DAG.

    This function is only used during type checking or auto-completion.

    :meta private:
    
    :param snowflake_conn_id: connection to use when running code within the Snowpark Container runner service.
    :type snowflake_conn_id: str  (default is snowflake_default)
    :param runner_endpoint: URL endpoint of the instantiated Snowpark Container runner service 
        (ie. ws://<hostnam>:<port>/<endpoint>)
    :type runner_endpoint: str
    :param python_callable: Function to decorate
    :type python_callable: Callable 
    :param python: Python version (ie. '<maj>.<min>').  Callable will run in a PythonVirtualenvOperator on the runner.  
        If not set will use default python version on runner.
    :type python: str:
    :param multiple_outputs: If set to True, the decorated function's return value will be unrolled to
        multiple XCom values. Dict will unroll to XCom values with its keys as XCom keys.
        Defaults to False.
    :type multiple_outputs: bool
    """
    return task_decorator_factory(
        runner_endpoint=runner_endpoint, 
        python=python,
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=SnowparkContainerPythonDecoratedOperator,
        **kwargs,
    )
from __future__ import annotations

import inspect
from textwrap import dedent
from typing import Callable, Sequence

from airflow.decorators.base import (
    DecoratedOperator,
    TaskDecorator,
    task_decorator_factory,
)
from airflow.utils.decorators import remove_task_decorator
from astronomer.providers.snowflake.operators.snowpark import (
    SnowparkContainersPythonOperator,
    SnowparkExternalPythonOperator,
    SnowparkPythonOperator,
    SnowparkVirtualenvOperator,
)


class _SnowparkPythonDecoratedOperator(DecoratedOperator, SnowparkPythonOperator):
    """
    Wraps a Python callable and captures args/kwargs when called for execution.

    :param snowflake_conn_id or conn_id: A Snowflake connection name.  Default 'snowflake_default'
    :param log_level: Set log level for Snowflake logging.  Default: 'ERROR'
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
    :param warehouse: name of warehouse (will overwrite any warehouse defined in the connection's extra JSON)
    :param database: name of database (will overwrite database defined in connection)
    :param schema: name of schema (will overwrite schema defined in connection)
    :param role: name of role (will overwrite any role defined in connection's extra JSON)
    :param authenticator: authenticator for Snowflake.
        'snowflake' (default) to use the internal Snowflake authenticator
        'externalbrowser' to authenticate using your web browser and
        Okta, ADFS or any other SAML 2.0-compliant identify provider
        (IdP) that has been defined for your account
        'https://<your_okta_account_name>.okta.com' to authenticate
        through native Okta.
    :param session_parameters: You can set session-level parameters at the time you connect to Snowflake
    :param python_callable: A reference to an object that is callable
    :param multiple_outputs: If set to True, the decorated function's return value will be unrolled to
        multiple XCom values. Dict will unroll to XCom values with its keys as XCom keys. Defaults to False.
    """

    template_fields: Sequence[str] = ("templates_dict", "op_args", "op_kwargs")
    template_fields_renderers = {
        "templates_dict": "json",
        "op_args": "py",
        "op_kwargs": "py",
    }

    custom_operator_name: str = "@task.snowpark_python"

    def __init__(
        self,
        *,
        snowflake_conn_id: str | None = None,
        conn_id: str | None = None,
        python_callable,
        op_args,
        op_kwargs,
        **kwargs,
    ) -> None:
        kwargs_to_upstream = {
            "python_callable": python_callable,
            "op_args": op_args,
            "op_kwargs": op_kwargs,
        }
        super().__init__(
            kwargs_to_upstream=kwargs_to_upstream,
            snowflake_conn_id=snowflake_conn_id or conn_id or "snowflake_default",
            python_callable=python_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
            **kwargs,
        )


def snowpark_python_task(
    python_callable: Callable | None = None,
    multiple_outputs: bool | None = None,
    snowflake_conn_id: str | None = None,
    **kwargs,
) -> TaskDecorator:
    """Wraps a function into an Airflow operator.

    Accepts kwargs for operator kwarg. Can be reused in a single DAG.

    This decorator assumes that Snowpark libraries are installed on the Apache Airflow instance and,
    by definition, that the Airflow instance is running a version of python which is supported with
    Snowpark.  If not consider using a virtualenv and the snowpark_virtualenv_task or
    snowpark_ext_python_task decorator instead.

    :param python_callable: Function to decorate
    :param multiple_outputs: If set to True, the decorated function's return value will be unrolled to
        multiple XCom values. Dict will unroll to XCom values with its keys as XCom keys. Defaults to False.
    :param snowflake_conn_id or conn_id: A Snowflake connection name.  Default 'snowflake_default'
    :param warehouse: name of warehouse (will overwrite any warehouse defined in the connection's extra JSON)
    :param database: name of database (will overwrite database defined in connection)
    :param schema: name of schema (will overwrite schema defined in connection)
    :param role: name of role (will overwrite any role defined in connection's extra JSON)
    :param authenticator: authenticator for Snowflake.
        'snowflake' (default) to use the internal Snowflake authenticator
        'externalbrowser' to authenticate using your web browser and
        Okta, ADFS or any other SAML 2.0-compliant identify provider
        (IdP) that has been defined for your account
        'https://<your_okta_account_name>.okta.com' to authenticate
        through native Okta.
    :param session_parameters: You can set session-level parameters at the time you connect to Snowflake
    :param python_callable: A reference to an object that is callable
    :param log_level: Set log level for Snowflake logging.  Default: 'ERROR'
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
    """
    return task_decorator_factory(
        python_callable=python_callable,
        snowflake_conn_id=snowflake_conn_id,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=_SnowparkPythonDecoratedOperator,
        **kwargs,
    )


class _SnowparkVirtualenvDecoratedOperator(
    DecoratedOperator, SnowparkVirtualenvOperator
):
    """
    Wraps a Python callable and captures args/kwargs when called for execution.
    :param python_callable: A reference to an object that is callable
    :param snowflake_conn_id or conn_id: A Snowflake connection name.  Default 'snowflake_default'
    :param requirements: Either a list of requirement strings, or a (templated)
        "requirements file" as specified by pip.
    :param python_version: The Python version to run the virtualenv with. Note that
        both 2 and 2.7 are acceptable forms.
    :param system_site_packages: Whether to include
        system_site_packages in your virtualenv.
        See virtualenv documentation for more information.
    :param pip_install_options: a list of pip install options when installing requirements
        See 'pip install -h' for available options
    :param warehouse: name of warehouse (will overwrite any warehouse defined in the connection's extra JSON)
    :param database: name of database (will overwrite database defined in connection)
    :param schema: name of schema (will overwrite schema defined in connection)
    :param role: name of role (will overwrite any role defined in connection's extra JSON)
    :param authenticator: authenticator for Snowflake.
        'snowflake' (default) to use the internal Snowflake authenticator
        'externalbrowser' to authenticate using your web browser and
        Okta, ADFS or any other SAML 2.0-compliant identify provider
        (IdP) that has been defined for your account
        'https://<your_okta_account_name>.okta.com' to authenticate
        through native Okta.
    :param session_parameters: You can set session-level parameters at the time you connect to Snowflake
    :param python_callable: A reference to an object that is callable
    :param log_level: Set log level for Snowflake logging.  Default: 'ERROR'
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
    :param multiple_outputs: If set to True, the decorated function's return value will be unrolled to
        multiple XCom values. Dict will unroll to XCom values with its keys as XCom keys. Defaults to False.
    """

    custom_operator_name: str = "@task.snowpark_virtualenv"

    def __init__(
        self,
        *,
        snowflake_conn_id: str | None = None,
        conn_id: str | None = None,
        python_callable,
        op_args,
        op_kwargs,
        **kwargs,
    ) -> None:
        kwargs_to_upstream = {
            "python_callable": python_callable,
            "op_args": op_args,
            "op_kwargs": op_kwargs,
        }
        super().__init__(
            kwargs_to_upstream=kwargs_to_upstream,
            snowflake_conn_id=snowflake_conn_id or conn_id or "snowflake_default",
            python_callable=python_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
            **kwargs,
        )


def snowpark_virtualenv_task(
    python_callable: Callable | None = None,
    multiple_outputs: bool | None = None,
    snowflake_conn_id: str | None = None,
    **kwargs,
) -> TaskDecorator:
    """Wraps a callable into an Airflow operator to run via a Python virtual environment.
    Accepts kwargs for operator kwarg. Can be reused in a single DAG.
    This function is only used only used during type checking or auto-completion.
    :meta private:
    :param python_callable: Function to decorate
    :param requirements: Either a list of requirement strings, or a (templated)
        "requirements file" as specified by pip.
    :param python_version: The Python version to run the virtualenv with. Note that
        both 2 and 2.7 are acceptable forms.
    :param system_site_packages: Whether to include
        system_site_packages in your virtualenv.
        See virtualenv documentation for more information.
    :param pip_install_options: a list of pip install options when installing requirements
        See 'pip install -h' for available options
    :param multiple_outputs: If set to True, the decorated function's return value will be unrolled to
        multiple XCom values. Dict will unroll to XCom values with its keys as XCom keys.
        Defaults to False.
    :param snowflake_conn_id or conn_id: A Snowflake connection name.  Default 'snowflake_default'
    :param warehouse: name of warehouse (will overwrite any warehouse defined in the connection's extra JSON)
    :param database: name of database (will overwrite database defined in connection)
    :param schema: name of schema (will overwrite schema defined in connection)
    :param role: name of role (will overwrite any role defined in connection's extra JSON)
    :param authenticator: authenticator for Snowflake.
        'snowflake' (default) to use the internal Snowflake authenticator
        'externalbrowser' to authenticate using your web browser and
        Okta, ADFS or any other SAML 2.0-compliant identify provider
        (IdP) that has been defined for your account
        'https://<your_okta_account_name>.okta.com' to authenticate
        through native Okta.
    :param session_parameters: You can set session-level parameters at the time you connect to Snowflake
    :param python_callable: A reference to an object that is callable
    :param log_level: Set log level for Snowflake logging.  Default: 'ERROR'
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
    """
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        snowflake_conn_id=snowflake_conn_id,
        decorated_operator_class=_SnowparkVirtualenvDecoratedOperator,
        **kwargs,
    )


class _SnowparkExternalPythonDecoratedOperator(
    DecoratedOperator, SnowparkExternalPythonOperator
):
    """
    Wraps a Python callable and captures args/kwargs when called for execution.

    :param python: Full path string (file-system specific) that points to a Python binary inside
        a virtualenv that should be used (in ``VENV/bin`` folder). Should be absolute path
        (so usually start with "/" or "X:/" depending on the filesystem/os used).
    :param python_callable: A reference to an object that is callable
    :param snowflake_conn_id or conn_id: A Snowflake connection name.  Default 'snowflake_default'
    :param warehouse: name of warehouse (will overwrite any warehouse defined in the connection's extra JSON)
    :param database: name of database (will overwrite database defined in connection)
    :param schema: name of schema (will overwrite schema defined in connection)
    :param role: name of role (will overwrite any role defined in connection's extra JSON)
    :param authenticator: authenticator for Snowflake.
        'snowflake' (default) to use the internal Snowflake authenticator
        'externalbrowser' to authenticate using your web browser and
        Okta, ADFS or any other SAML 2.0-compliant identify provider
        (IdP) that has been defined for your account
        'https://<your_okta_account_name>.okta.com' to authenticate
        through native Okta.
    :param session_parameters: You can set session-level parameters at the time you connect to Snowflake
    :param python_callable: A reference to an object that is callable
    :param log_level: Set log level for Snowflake logging.  Default: 'ERROR'
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
    """

    custom_operator_name: str = "@task.snowpark_ext_python"

    def __init__(
        self,
        *,
        snowflake_conn_id: str | None = None,
        conn_id: str | None = None,
        python_callable,
        op_args,
        op_kwargs,
        **kwargs,
    ) -> None:
        kwargs_to_upstream = {
            "python_callable": python_callable,
            "op_args": op_args,
            "op_kwargs": op_kwargs,
        }
        super().__init__(
            kwargs_to_upstream=kwargs_to_upstream,
            snowflake_conn_id=snowflake_conn_id or conn_id or "snowflake_default",
            python_callable=python_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
            **kwargs,
        )


def snowpark_ext_python_task(
    python_callable: Callable | None = None,
    multiple_outputs: bool | None = None,
    python: str | None = None,
    snowflake_conn_id: str | None = None,
    **kwargs,
) -> TaskDecorator:
    """Wraps a callable into an Airflow operator to run via a Python virtual environment.
    Accepts kwargs for operator kwarg. Can be reused in a single DAG.
    This function is only used during type checking or auto-completion.
    :meta private:
    :param python: Full path string (file-system specific) that points to a Python binary inside
        a virtualenv that should be used (in ``VENV/bin`` folder). Should be absolute path
        (so usually start with "/" or "X:/" depending on the filesystem/os used).
    :param python_callable: Function to decorate
    :param multiple_outputs: If set to True, the decorated function's return value will be unrolled to
        multiple XCom values. Dict will unroll to XCom values with its keys as XCom keys.
        Defaults to False.
    :param snowflake_conn_id or conn_id: A Snowflake connection name.  Default 'snowflake_default'
    :param warehouse: name of warehouse (will overwrite any warehouse defined in the connection's extra JSON)
    :param database: name of database (will overwrite database defined in connection)
    :param schema: name of schema (will overwrite schema defined in connection)
    :param role: name of role (will overwrite any role defined in connection's extra JSON)
    :param authenticator: authenticator for Snowflake.
        'snowflake' (default) to use the internal Snowflake authenticator
        'externalbrowser' to authenticate using your web browser and
        Okta, ADFS or any other SAML 2.0-compliant identify provider
        (IdP) that has been defined for your account
        'https://<your_okta_account_name>.okta.com' to authenticate
        through native Okta.
    :param session_parameters: You can set session-level parameters at the time you connect to Snowflake
    :param python_callable: A reference to an object that is callable
    :param log_level: Set log level for Snowflake logging.  Default: 'ERROR'
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
    """
    return task_decorator_factory(
        python=python,
        snowflake_conn_id=snowflake_conn_id,
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=_SnowparkExternalPythonDecoratedOperator,
        **kwargs,
    )


class SnowparkContainersPythonDecoratedOperator(
    DecoratedOperator, SnowparkContainersPythonOperator
):
    """
    Wraps a Python callable and captures args/kwargs when called for execution.

    """

    template_fields: Sequence[str] = ("op_args", "op_kwargs")
    template_fields_renderers = {"op_args": "py", "op_kwargs": "py"}

    shallow_copy_attrs: Sequence[str] = ("python_callable",)

    custom_operator_name: str = "@task.snowpark_containers_python"

    def __init__(self, *, python_callable, op_args, op_kwargs, **kwargs) -> None:
        kwargs_to_upstream = {
            "python_callable": python_callable,
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
    multiple_outputs: bool = False,
    **kwargs,
) -> TaskDecorator:
    """Wraps a python callable into an Airflow operator to run via a Snowpark Container runner service.

    Accepts kwargs for operator kwarg. Can be reused in a single DAG.

    This function is only used during type checking or auto-completion.

    :meta private:

    :param snowflake_conn_id: connection to use when running code within the Snowpark Container runner service.
    :type snowflake_conn_id: str  (default is snowflake_default)
    :param runner_service_name: Name of Airflow runner service in Snowpark Container services.  Must specify
    runner_service_name or runner_endpoint
    :type runner_service_name: str
    :param runner_endpoint: Endpoint URL of the instantiated Snowpark Container runner.  Must specify
    runner_endpoint or runner_service_name.
    :type runner_endpoint: str
    :param runner_headers: Optional OAUTH bearer token for Snowpark Container runner.  If runner_service_name is
    specified SnowparkContainersHook() will be used to pull the token just before running the task.
    :type runner_headers: str
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
        multiple_outputs=multiple_outputs,
        decorated_operator_class=SnowparkContainersPythonDecoratedOperator,
        **kwargs,
    )

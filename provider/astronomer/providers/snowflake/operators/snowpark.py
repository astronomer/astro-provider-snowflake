from __future__ import annotations
import jinja2
import os
from typing_extensions import Any
import sys
from textwrap import dedent
import inspect
from typing import Any, Callable, Collection, Iterable, Mapping, Sequence, Container
import subprocess
from pathlib import Path

from airflow.utils.process_utils import execute_in_subprocess
from airflow.utils.decorators import remove_task_decorator
from airflow.operators.python import (
    _BasePythonVirtualenvOperator, 
    PythonVirtualenvOperator, 
    ExternalPythonOperator,
    PythonOperator
)
from airflow.exceptions import (
    AirflowException,
    AirflowSkipException
)

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from astronomer.providers.snowflake import SnowparkTable

try:
    from astro.sql.table import Table, TempTable
except:
    Table = None    # type: ignore
    TempTable = None

try:
    from snowflake.snowpark import Session as SnowparkSession
except ImportError:
    SnowparkSession = None  # type: ignore

def get_snowflake_conn_params(operator:Any) -> dict:
    """
    Resolves snowflake connection parameters.

    Conn params may come from the hook or set in the operator/decorator.
    Some params (ie. warehouse, database, schema, etc) can be None as these are set in Snowflake defaults.
    Some regions (ie. us-west-2) do not except a region param.  Account must be fully qualified instead.
    Table or SnowparkTable class can also override this in get_fq_table_name()

    """

    #Start with params that come with the Snowflake hook
    conn_params = SnowflakeHook(snowflake_conn_id=operator.snowflake_conn_id)._get_conn_params()

    if conn_params['region'] in [None, '']:
        assert len(conn_params['account'].split('.')) > 3, "Snowflake Region not set and account is not fully-qualified."

    #replace with any that come from the operator at runtime
    if operator.warehouse:
        conn_params['warehouse'] = operator.warehouse
    if operator.database:
        conn_params['database'] = operator.database
    if operator.schema:
        conn_params['schema'] = operator.schema
    if operator.role:
        conn_params['role'] = operator.role
    if operator.authenticator:
        conn_params['authenticator'] = operator.authenticator
    if operator.session_parameters:
        conn_params['session_parameters'] = operator.session_parameters

    return conn_params

class _BaseSnowparkOperator(_BasePythonVirtualenvOperator):
    """
    Provides a base class overloading the get_python_source() to prepend the python_callable with
    Snowpark specific bits.

    This is required when calling the function from a subprocess since we can't pass the non-serializable
    snowpark session or table objects as args.

    In addition to specifying the snowflake_conn_id the user can override warehouse, database, role, schema
    authenticator and session_parameters in the operator args.

    :param snowflake_conn_id: A Snowflake connection name.  Default 'snowflake_default'
    :param python_callable: A python function with no references to outside variables,
        defined with def, which will be run in a virtualenv.
    :param use_dill: Whether to use dill to serialize
        the args and result (pickle is default). This allow more complex types
        but requires you to include dill in your requirements.
    :param op_args: A list of positional arguments to pass to python_callable.
    :param op_kwargs: A dict of keyword arguments to pass to python_callable.
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

    def __init__(
        self,
        *,
        python_callable: Callable,
        snowflake_conn_id: str = "snowflake_default",
        use_dill: bool = False,
        op_args: Collection[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        string_args: Iterable[str] | None = None,
        templates_dict: dict | None = None,
        templates_exts: list[str] | None = None,
        expect_airflow: bool = True,
        show_return_value_in_logs: bool = True,
        skip_on_exit_code: int | Container[int] | None = None,
        **kwargs,
    ):
        self.skip_on_exit_code = skip_on_exit_code
        self.snowflake_conn_id = snowflake_conn_id
        self.warehouse = kwargs.pop('warehouse', None)
        self.database = kwargs.pop('database', None)
        self.role = kwargs.pop('role', None)
        self.schema = kwargs.pop('schema', None)
        self.authenticator = kwargs.pop('authenticator', None)
        self.session_parameters = kwargs.pop('session_parameters', None)

        super().__init__(
            python_callable=python_callable,
            use_dill=use_dill,
            op_args=op_args,
            op_kwargs=op_kwargs,
            string_args=string_args,
            templates_dict=templates_dict,
            templates_exts=templates_exts,
            expect_airflow=expect_airflow,
            show_return_value_in_logs=show_return_value_in_logs,
            # skip_on_exit_code=skip_on_exit_code,
            **kwargs,
        )
    
    def get_python_source(self):
        raw_source = inspect.getsource(self.python_callable)
        res = dedent(raw_source)
        if hasattr(self, 'custom_operator_name'):
            res = remove_task_decorator(res, self.custom_operator_name)
        return res

    @staticmethod
    def write_python_script(
        jinja_context: dict,
        filename: str,
        render_template_as_native_obj: bool = False,
    ):
        """
        Renders the python script to a file to execute in the virtual environment.
        :param jinja_context: The jinja context variables to unpack and replace with its placeholders in the
            template file.
        :param filename: The name of the file to dump the rendered script to.
        :param render_template_as_native_obj: If ``True``, rendered Jinja template would be converted
            to a native Python object
        """
        
        template_loader = jinja2.FileSystemLoader(searchpath=os.path.dirname(__file__))
        template_env: jinja2.Environment
        if render_template_as_native_obj:
            template_env = jinja2.nativetypes.NativeEnvironment(
                loader=template_loader, undefined=jinja2.StrictUndefined
            )
        else:
            template_env = jinja2.Environment(loader=template_loader, undefined=jinja2.StrictUndefined)
        template = template_env.get_template("snowpark_virtualenv_script.jinja2")
        template.stream(**jinja_context).dump(filename)

    def _execute_python_callable_in_subprocess(self, python_path: Path, tmp_dir: Path):
        op_kwargs: dict[str, Any] = {k: v for k, v in self.op_kwargs.items()}
        if self.templates_dict:
            op_kwargs["templates_dict"] = self.templates_dict
        input_path = tmp_dir / "script.in"
        output_path = tmp_dir / "script.out"
        string_args_path = tmp_dir / "string_args.txt"
        script_path = tmp_dir / "script.py"
        self._write_args(input_path)
        self._write_string_args(string_args_path)
        self.write_python_script(
            jinja_context=dict(
                conn_params=get_snowflake_conn_params(self),
                snowflake_conn_id=self.snowflake_conn_id,
                op_args=self.op_args,
                op_kwargs=op_kwargs,
                expect_airflow=self.expect_airflow,
                pickling_library=self.pickling_library.__name__,
                python_callable=self.python_callable.__name__,
                python_callable_source=self.get_python_source(),
            ),
            filename=os.fspath(script_path),
            render_template_as_native_obj=self.dag.render_template_as_native_obj,
        )

        try:
            execute_in_subprocess(
                cmd=[
                    os.fspath(python_path),
                    os.fspath(script_path),
                    os.fspath(input_path),
                    os.fspath(output_path),
                    os.fspath(string_args_path),
                ]
            )
        except subprocess.CalledProcessError as e:
            if e.returncode in self.skip_on_exit_code:
                raise AirflowSkipException(f"Process exited with code {e.returncode}. Skipping.")
            else:
                raise

        return self._read_result(output_path)


class SnowparkVirtualenvOperator(PythonVirtualenvOperator, _BaseSnowparkOperator):
    """
    Runs a Snowflake Snowpark Python function in a virtualenv that is created and destroyed automatically.

    Instantiates a Snowpark Session named 'snowpark_session' and attempts to create Snowpark Dataframes 
    from any SnowparTable or Astro Python SDK Table type annotated arguments.

    The virtualenv must have this package or Astro SDK installed in order pass table args.

    If an existing virtualenv has all necessary packages consider using the SnowparkExternalPythonOperator.

    The function must be defined using def, and not be
    part of a class. All imports must happen inside the function
    and no variables outside the scope may be referenced. A global scope
    variable named virtualenv_string_args will be available (populated by
    string_args). In addition, one can pass stuff through op_args and op_kwargs, and one
    can use a return value.

    Note that if your virtualenv runs in a different Python major version than Airflow,
    you cannot use return values, op_args, op_kwargs, or use any macros that are being provided to
    Airflow through plugins. You can use string_args though.

    In addition to specifying the snowflake_conn_id the user can override warehouse, database, role, schema
    authenticator and session_parameters in the operator args.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:PythonVirtualenvOperator`
    :param snowflake_conn_id: A Snowflake connection name.  Default 'snowflake_default'
    :param python_callable: A python function with no references to outside variables,
        defined with def, which will be run in a virtualenv
    :param requirements: Either a list of requirement strings, or a (templated)
        "requirements file" as specified by pip.
    :param python_version: The Python version to run the virtualenv with. Note that
        both 2 and 2.7 are acceptable forms.
    :param use_dill: Whether to use dill to serialize
        the args and result (pickle is default). This allow more complex types
        but requires you to include dill in your requirements.
    :param system_site_packages: Whether to include
        system_site_packages in your virtualenv.
        See virtualenv documentation for more information.
    :param pip_install_options: a list of pip install options when installing requirements
        See 'pip install -h' for available options
    :param op_args: A list of positional arguments to pass to python_callable.
    :param op_kwargs: A dict of keyword arguments to pass to python_callable.
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
    template_fields: Sequence[str] = tuple({"requirements"} | set(_BaseSnowparkOperator.template_fields))
    template_ext: Sequence[str] = (".txt",)

    def __init__(
        self,
        *,
        python_callable: Callable,
        snowflake_conn_id: str = 'snowflake_default',
        requirements: None | Iterable[str] | str = None,
        python_version: str | int | float | None = None,
        use_dill: bool = False,
        system_site_packages: bool = True,
        pip_install_options: list[str] | None = None,
        op_args: Collection[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        string_args: Iterable[str] | None = None,
        templates_dict: dict | None = None,
        templates_exts: list[str] | None = None,
        expect_airflow: bool = True,
        skip_on_exit_code: int | Container[int] | None = None,
        **kwargs,
    ):        

        super().__init__(
            python_callable=python_callable,
            snowflake_conn_id=snowflake_conn_id,
            use_dill=use_dill,
            op_args=op_args,
            op_kwargs=op_kwargs,
            string_args=string_args,
            templates_dict=templates_dict,
            templates_exts=templates_exts,
            expect_airflow=expect_airflow,
            skip_on_exit_code=skip_on_exit_code,
            requirements=requirements,
            python_version=python_version,
            system_site_packages=system_site_packages,
            pip_install_options=pip_install_options,
            **kwargs,
        )


class SnowparkExternalPythonOperator(ExternalPythonOperator, _BaseSnowparkOperator):
    """
    Runs a Snowflake Snowpark Python function with a preexisting python environment.

    Instantiates a Snowpark Session named 'snowpark_session' and attempts to create Snowpark Dataframes 
    from any SnowparTable or Astro Python SDK Table type annotated arguments.
    
    The virtualenv must have this package or Astro SDK installed in order pass table args.

    If an existing virtualenv is not available consider using the SnowparkVirtualenvPythonOperator.

    The function must be defined using def, and not be
    part of a class. All imports must happen inside the function
    and no variables outside the scope may be referenced. A global scope
    variable named virtualenv_string_args will be available (populated by
    string_args). In addition, one can pass stuff through op_args and op_kwargs, and one
    can use a return value.

    Note that if your virtualenv runs in a different Python major version than Airflow,
    you cannot use return values, op_args, op_kwargs, or use any macros that are being provided to
    Airflow through plugins. You can use string_args though.

    If Airflow is installed in the external environment in different version than the version
    used by the operator, the operator will fail.

    In addition to specifying the snowflake_conn_id the user can override warehouse, database, role, schema
    authenticator and session_parameters in the operator args.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:ExternalPythonOperator`
    :param snowflake_conn_id: A Snowflake connection name.  Default 'snowflake_default'
    :param python_callable: A python function with no references to outside variables,
        defined with def, which will be run in a virtualenv
    :param python: Full path string (file-system specific) that points to a Python binary inside
        a virtualenv that should be used (in ``VENV/bin`` folder). Should be absolute path
        (so usually start with "/" or "X:/" depending on the filesystem/os used).
    :param use_dill: Whether to use dill to serialize
        the args and result (pickle is default). This allow more complex types
        but requires you to include dill in your requirements.
    :param op_args: A list of positional arguments to pass to python_callable.
    :param op_kwargs: A dict of keyword arguments to pass to python_callable.
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
    template_fields: Sequence[str] = tuple(set(_BaseSnowparkOperator.template_fields))
    template_ext: Sequence[str] = (".txt",)

    def __init__(
        self,
        *,
        python: str,
        python_callable: Callable,
        snowflake_conn_id: str = 'snowflake_default',
        use_dill: bool = False,
        op_args: Collection[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        string_args: Iterable[str] | None = None,
        templates_dict: dict | None = None,
        templates_exts: list[str] | None = None,
        expect_airflow: bool = True,
        expect_pendulum: bool = False,
        skip_on_exit_code: int | Container[int] | None = None,
        **kwargs,
    ):
        
        super().__init__(
            python_callable=python_callable,
            python=python,
            snowflake_conn_id=snowflake_conn_id,
            use_dill=use_dill,
            op_args=op_args,
            op_kwargs=op_kwargs,
            string_args=string_args,
            templates_dict=templates_dict,
            templates_exts=templates_exts,
            expect_airflow=expect_airflow,
            expect_pendulum=expect_pendulum,
            skip_on_exit_code=skip_on_exit_code,
            **kwargs,
        )
        
    # # ###DEBUG###
    # def execute_callable(self):
    #     python_path = Path(self.python)
    #     tmp_path = Path('/usr/local/airflow/include/tmp')
    #     return self._execute_python_callable_in_subprocess(python_path, tmp_path)


class SnowparkPythonOperator(SnowparkExternalPythonOperator):
    """
    Runs a Snowflake Snowpark Python function in a local Airflow task.

    This operator assumes that Snowpark libraries are installed on the Apache Airflow instance and, 
    by definition, that the Airflow instance is running a version of python which is supported with 
    Snowpark.  If not consider using a virtualenv and the SnowparkVirtualenvOperator or 
    SnowparkExternalPythonOperator instead.

    :param snowflake_conn_id: Reference to
        :ref:`Snowflake connection id<howto/connection:snowflake>`
    :param warehouse: name of warehouse (will override any warehouse defined in the connection's extra JSON)
    :param database: name of database (will override database defined in connection)
    :param schema: name of schema (will override schema defined in connection)
    :param role: name of role (will override any role defined in connection's extra JSON)
    :param authenticator: authenticator for Snowflake.
        'snowflake' (default) to use the internal Snowflake authenticator
        'externalbrowser' to authenticate using your web browser and
        Okta, ADFS or any other SAML 2.0-compliant identify provider
        (IdP) that has been defined for your account
        'https://<your_okta_account_name>.okta.com' to authenticate
        through native Okta.
    :param session_parameters: You can set session-level parameters at the time you connect to Snowflake
    :param python_callable: A reference to an object that is callable
    :param op_kwargs: a dictionary of keyword arguments that will get unpacked in your function (templated)
    :param op_args: a list of positional arguments that will get unpacked when
        calling your callable (templated)
    :param multiple_outputs: if set, function return value will be
        unrolled to multiple XCom values. Dict will unroll to xcom values with keys as keys.
        Defaults to False.
    """
    
    def __init__(
        self,
        *,
        python: str = sys.executable,
        python_callable: Callable,
        snowflake_conn_id: str = 'snowflake_default',
        use_dill: bool = False,
        op_args: Collection[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        string_args: Iterable[str] | None = None,
        templates_dict: dict | None = None,
        templates_exts: list[str] | None = None,
        expect_airflow: bool = True,
        expect_pendulum: bool = False,
        skip_on_exit_code: int | Container[int] | None = None,
        show_return_value_in_logs: bool = True,
        **kwargs,
    ) -> None:

        if SnowparkSession is None:
            raise AirflowException("The snowflake-snowpark-python package is not installed.")

        super().__init__(
            python=python,
            python_callable=python_callable,
            snowflake_conn_id=snowflake_conn_id,
            use_dill=use_dill,
            op_args=op_args,
            op_kwargs=op_kwargs,
            string_args=string_args,
            templates_dict=templates_dict,
            templates_exts=templates_exts,
            expect_airflow=expect_airflow,
            expect_pendulum=expect_pendulum,
            skip_on_exit_code=skip_on_exit_code,
            show_return_value_in_logs=show_return_value_in_logs,
            **kwargs,
        )


class SnowparkPythonUDFOperator(PythonVirtualenvOperator, _BaseSnowparkOperator):
    """
    ######WORK IN PROGRESS#####


    Runs a Python Funciton as a temporary User Defined Function (UDF) in Snowpark.  
    
    Will use the base Airflow python to register the UDF or create a virtual environment if python_version is set.

    Instantiates a Snowpark Session named 'snowpark_session' and attempts to create Snowpark Dataframes 
    from any SnowparTable or Astro python SDK Table type annotated arguments.

    Any requirements specified will be passed to the Snowpark backend.

    The virtualenv must have this package or Astro SDK installed in order pass table args.

    If an existing virtualenv has all necessary packages consider using the SnowparkExternalPythonOperator.

    The function must be defined using def, and not be
    part of a class. All imports must happen inside the function
    and no variables outside the scope may be referenced. A global scope
    variable named virtualenv_string_args will be available (populated by
    string_args). In addition, one can pass stuff through op_args and op_kwargs, and one
    can use a return value.

    Note that if your virtualenv runs in a different Python major version than Airflow,
    you cannot use return values, op_args, op_kwargs, or use any macros that are being provided to
    Airflow through plugins. You can use string_args though.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:PythonVirtualenvOperator`
    :param snowflake_conn_id: A Snowflake connection name.  Default 'snowflake_default'
    :param python_callable: A python function with no references to outside variables,
        defined with def, which will be run in a virtualenv
    :param requirements: Either a list of requirement strings, or a (templated)
        "requirements file" as specified by pip.
    :param python_version: The Python version to run the virtualenv with. Note that
        both 2 and 2.7 are acceptable forms.
    :param use_dill: Whether to use dill to serialize
        the args and result (pickle is default). This allow more complex types
        but requires you to include dill in your requirements.
    :param system_site_packages: Whether to include
        system_site_packages in your virtualenv.
        See virtualenv documentation for more information.
    :param pip_install_options: a list of pip install options when installing requirements
        See 'pip install -h' for available options
    :param op_args: A list of positional arguments to pass to python_callable.
    :param op_kwargs: A dict of keyword arguments to pass to python_callable.
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
    template_fields: Sequence[str] = tuple({"requirements"} | set(_BaseSnowparkOperator.template_fields))
    template_ext: Sequence[str] = (".txt",)

    def __init__(
        self,
        *,
        python_callable: Callable,
        snowflake_conn_id: str = None,
        conn_id: str = None,
        requirements: None | Iterable[str] | str = None,
        python_version: str | int | float | None = None,
        use_dill: bool = False,
        system_site_packages: bool = True,
        pip_install_options: list[str] | None = None,
        op_args: Collection[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        string_args: Iterable[str] | None = None,
        templates_dict: dict | None = None,
        templates_exts: list[str] | None = None,
        expect_airflow: bool = True,
        **kwargs,
    ):
        if python_version and python_version not in _SUPPORTED_SNOWPARK_PYTHON_VERSIONS:
            raise AirflowException(
                f"Requested python version {python_version} "
                f"not in supported versions: {_SUPPORTED_SNOWPARK_PYTHON_VERSIONS} "
                )
        
        self.snowflake_conn_id = snowflake_conn_id or conn_id or "snowflake_default"

        super().__init__(
            python_callable=python_callable,
            requirements=requirements,
            python_version=python_version,
            use_dill=use_dill,
            system_site_packages=system_site_packages,
            pip_install_options=pip_install_options,
            op_args=op_args,
            op_kwargs=op_kwargs,
            string_args=string_args,
            templates_dict=templates_dict,
            templates_exts=templates_exts,
            expect_airflow=expect_airflow,
            **kwargs,
        )
    def get_python_source(self):
        """Return the source of self.python_callable prepended with the Snowpark UDF session creation."""

        python_callable:str = self._get_python_source()

        #TODO: add @udf decorator

        return 
    

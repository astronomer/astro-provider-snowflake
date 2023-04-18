from __future__ import annotations

import sys
from textwrap import dedent
import inspect
from typing import Any, Callable, Collection, Iterable, Mapping, Sequence

from airflow.operators.python import PythonVirtualenvOperator, PythonOperator
from airflow.exceptions import AirflowException

#TODO: investigate merging SnowparkTable and SDK Table
#     from astro.sql.table import Table 

from astronomer.providers.snowflake import SnowparkTable

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

try:
    from snowflake.snowpark import Session as SnowparkSession
except ImportError:
    SnowparkSession = None  # type: ignore

_SUPPORTED_SNOWPARK_PYTHON_VERSIONS = ['3.8']

class SnowparkPythonOperator(PythonOperator):
    """
    Runs a Snowflake Snowpark Python function in an local Airflow task.

    This operator assumes that Snowpark libraries are installed on the Apache Airflow instance and, 
    by definition, that the Airflow instance is running a version of python which is supported with 
    Snowpark.  If not consider using a virtualenv and the SnowparkVirtualenvOperator or 
    SnowparkExternalPythonOperator instead.

    :param snowflake_conn_id: Reference to
        :ref:`Snowflake connection id<howto/connection:snowflake>`
    :param parameters: (optional) the parameters to render the SQL query with.
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
        snowflake_conn_id: str = "snowflake_default",
        parameters: dict | None = None,
        warehouse: str | None = None,
        database: str | None = None,
        role: str | None = None,
        schema: str | None = None,
        authenticator: str | None = None,
        session_parameters: dict | None = None,
        python_callable,
        op_args,
        op_kwargs: dict,
        **kwargs,
    ) -> None:
        
        sys_ver = f'{sys.version_info.major}.{sys.version_info.minor}'
        
        if sys_ver not in _SUPPORTED_SNOWPARK_PYTHON_VERSIONS:
            raise AirflowException(f'Airflow python version {sys_ver} not supported by Snowpark. ',
                                   'Try using SnowparkVirtualenvOperator.')

        if SnowparkSession is None:
            raise AirflowException("The snowflake-snowpark-python package is not installed.")

        self.snowflake_conn_id = snowflake_conn_id
        self.parameters = parameters
        self.warehouse = warehouse
        self.database = database
        self.role = role
        self.schema = schema
        self.authenticator = authenticator
        self.session_parameters = session_parameters

        kwargs_to_upstream = {
            "python_callable": python_callable,
            "op_args": op_args,
            "op_kwargs": op_kwargs,
        }
        super().__init__(
            kwargs_to_upstream=kwargs_to_upstream,
            python_callable=python_callable,
            op_args=op_args,
            # airflow.decorators.base.DecoratedOperator checks if the functions are bindable, so we have to
            # add an artificial value to pass the validation. The real value is determined at runtime.
            op_kwargs={**op_kwargs, "snowpark_session": None},
            **kwargs,
        )
    
    def execute_callable(self):
        hook = SnowflakeHook(
            snowflake_conn_id=self.snowflake_conn_id,
            parameters=self.parameters,
            warehouse=self.warehouse,
            database=self.database,
            role=self.role,
            schema=self.schema,
            authenticator=self.authenticator,
            session_parameters=self.session_parameters,
        )
        snowpark_session = SnowparkSession(hook._get_conn_params())
        try:
            op_kwargs = dict(self.op_kwargs)
            # Set real sessions as an argument to the function.
            op_kwargs["snowpark_session"] = snowpark_session
            return self.python_callable(*self.op_args, **self.op_kwargs)
        finally:
            snowpark_session.close()


class BaseSnowparkOperator(PythonVirtualenvOperator):
    """
    Runs a Snowflake Snowpark Python function in a virtualenv that is created and destroyed automatically.

    Instantiates a Snowpark Session named 'session' and attempts to create Snowpark Dataframes 
    from any SnowparkDataFrame type annotated arguments.

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
    :param conn_id: A Snowflake connection name.  Default 'snowflake_default'
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

    template_fields: Sequence[str] = tuple({"requirements"} | set(PythonVirtualenvOperator.template_fields))
    template_ext: Sequence[str] = (".txt",)

    def __init__(
        self,
        *,
        python_callable: Callable,
        conn_id: str | None = 'snowflake_default',
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
        
        self.conn_id = conn_id

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

    @staticmethod
    def get_fq_table_name(table:SnowparkTable, database:str = None, schema:str = None) -> str:

        name:list = table.name.split('.')

        if len(name) == 3:
            fq_table_name:str = table.name
        elif len(name) == 1:
            database:str = table.metadata.database or database
            schema:str = table.metadata.schema or schema
            
            if database and schema:
                fq_table_name = f'{database}.{schema}.{table.name}'
            else:
                #assume user has default db and schema set in their account settings
                fq_table_name = table.name
        else:
            AirflowException(f'Incorrect table name format {name}')

        return fq_table_name

    def get_python_source(self):
        """Return the source of self.python_callable prepended with the Snowpark session creation."""

        hook:SnowflakeHook = SnowflakeHook(conn_id=self.conn_id)
        conn_params:dict = hook._get_conn_params()
        database:str | None = hook.database or conn_params['database']
        schema:str | None = hook.schema or conn_params['schema']

        python_callable:list = dedent(inspect.getsource(self.python_callable)).split('\n')

        match_indent:str = ' ' * int(len(python_callable[1]) - len(python_callable[1].lstrip(' ')))

        prepended_callable:list = ['from astronomer.providers.snowflake import SnowparkTable\n']

        #add the function def
        prepended_callable.append(f'{python_callable.pop(0)}\n')

        #create a snowpark session called 'snowpark_session'
        prepended_callable.append(f'{match_indent}from snowflake.snowpark import Session as SnowparkSession\n')
        prepended_callable.append(f'{match_indent}snowpark_session = SnowparkSession.builder.configs({hook._get_conn_params()}).create()\n')

        #create a dict of SnowparkTable type args in order to auto instantiate Snowpark Dataframes for the user
        full_spec = inspect.getfullargspec(self.python_callable)
        op_args = list(self.op_args)
        op_kwargs = self.op_kwargs

        snowpark_table_args = {}

        #first pop the positional args
        for op_arg in op_args:
            try:
                current_arg = full_spec.args.pop(0)
            except IndexError:
                AirflowException('op_arg count does not match function arg count.')

            if full_spec.annotations.get(current_arg) == SnowparkTable:
                fq_table_name = self.get_fq_table_name(table=op_arg, database=database, schema=schema)
                snowpark_table_args[current_arg]=fq_table_name

        #If there are any function args not yet consumed check for kwargs
        if op_kwargs:
            if not full_spec.args:
                AirflowException('op_kwargs specified but no args in funciton signature remaining after op_args.')
            else:
                param_types = inspect.signature(self.python_callable).parameters
                for k, v in op_kwargs.items():
                    if param_types.get(k).annotation == SnowparkTable:
                        full_spec.args.remove(k)
                        
                        fq_table_name = self.get_fq_table_name(table=v, database=database, schema=schema)

                        #If args are specified both in position and keyword, the keyword takes precedence.
                        snowpark_table_args[k]=fq_table_name

        #prepend instantiation to the python_callable
        for k, v in snowpark_table_args.items():
            prepended_callable.append(f'{match_indent}{k} = snowpark_session.table("{v}")\n')

        #add the remaining lines of the python_callable
        for line in python_callable:
            prepended_callable.append(f'{line}\n')

        return ''.join(prepended_callable)
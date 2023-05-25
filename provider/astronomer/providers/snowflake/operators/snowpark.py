from __future__ import annotations
import jinja2
import os
import sys
from textwrap import dedent
import inspect
from typing import Any, Callable, Collection, Iterable, Mapping, Sequence, Container, TYPE_CHECKING
import subprocess
from pathlib import Path

from airflow.utils.context import Context, context_copy_partial
if TYPE_CHECKING:
    from airflow.utils.context import Context

from airflow.utils.process_utils import execute_in_subprocess
from airflow.utils.decorators import remove_task_decorator
from airflow.operators.python import (
    _BasePythonVirtualenvOperator, 
    PythonVirtualenvOperator, 
    ExternalPythonOperator
)
from airflow.exceptions import (
    AirflowException,
    AirflowSkipException
)

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from astronomer.providers.snowflake import SnowparkTable
from astronomer.providers.snowflake.xcom_backends.snowflake import _try_parse_snowflake_xcom_uri

try:
    from astro.sql.table import Table, TempTable
except:
    Table = None    # type: ignore
    TempTable = None # type: ignore

try:
    from snowflake.snowpark import Session as SnowparkSession
except ImportError:
    SnowparkSession = None  # type: ignore

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
        temp_data_output: str = None,
        temp_data_db: str = None,
        temp_data_schema: str = None,
        temp_data_stage: str = None,
        temp_data_table_prefix: str = 'XCOM_',
        temp_data_overwrite: bool = False,
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
        self.conn_params = self.get_snowflake_conn_params()

        self.temp_data_dict = {
            'temp_data_output': temp_data_output,
            'temp_data_db': temp_data_db,
            'temp_data_schema': temp_data_schema,
            'temp_data_stage': temp_data_stage,
            'temp_data_table_prefix': temp_data_table_prefix,
            'temp_data_overwrite': temp_data_overwrite
        }
    
        if temp_data_output == 'stage':
            assert temp_data_stage, "temp_data_stage must be specified if temp_data_output='stage'"

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
    
    def get_snowflake_conn_params(self) -> dict:
        """
        Resolves snowflake connection parameters.

        Conn params may come from the hook or set in the operator/decorator.
        Some params (ie. warehouse, database, schema, etc) can be None as these are set in Snowflake defaults.
        Some regions (ie. us-west-2) do not except a region param.  Account must be fully qualified instead.
        Table or SnowparkTable class can also override this in get_fq_table_name()

        """

        #Start with params that come with the Snowflake hook
        conn_params = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)._get_conn_params()

        if conn_params['region'] in [None, '']:
            parse_logic = len(conn_params['account'].split('.')) > 3 or len(conn_params['account'].split('-')) >= 2
            assert parse_logic, "Snowflake Region not set and account name is not fully-qualified."

        #replace with any that come from the operator at runtime
        if self.warehouse:
            conn_params['warehouse'] = self.warehouse
        if self.database:
            conn_params['database'] = self.database
        if self.schema:
            conn_params['schema'] = self.schema
        if self.role:
            conn_params['role'] = self.role
        if self.authenticator:
            conn_params['authenticator'] = self.authenticator
        if self.session_parameters:
            conn_params['session_parameters'] = self.session_parameters

        return conn_params
    
    #This function is part of the snowpark jinja template and will be inserted into the callable script.
    def _is_table_arg(arg):
        if ((Table or TempTable) and isinstance(arg, (Table, TempTable))) \
                or (SnowparkTable and isinstance(arg, SnowparkTable)):
            arg=arg.to_json()

        if isinstance(arg, dict) and arg.get("class", "") in ["SnowparkTable", "Table", "TempTable"]:
            # assert arg['conn_id'] and SnowflakeHook(conn_id=arg['conn_id']).conn_type == 'snowflake', f"No connection specified for {arg.get('class')} {arg['name']}"
            if len(arg['name'].split('.')) == 3:
                return arg['name']
            elif len(arg['name'].split('.')) == 1:
                database = arg['metadata'].get('database') \
                                or conn_params['database'] \
                                or snowpark_session.get_current_database().replace("\"","")
                assert database, "SnowparkTable object provided without database or user default."

                schema = arg['metadata'].get('schema') \
                            or conn_params['schema'] \
                            or snowpark_session.get_current_schema().replace("\"","")
                assert schema, "SnowparkTable object provided without schema or user default."

                return f"{database}.{schema}.{arg['name']}"
            else:
                raise Exception("SnowparkTable name must be fully-qualified or tablename only.")
        else:
            return False
    
    #This function is part of the snowpark jinja template and will be inserted into the callable script.
    def _deserialize_snowpark_args(arg:Any):
        table_name = _is_table_arg(arg)
        uri = _try_parse_snowflake_xcom_uri(arg)

        if table_name:
            return snowpark_session.table(table_name)
        
        elif uri and uri['xcom_stage'] and uri['xcom_key'].split('.')[-1] == 'parquet': 
            return snowpark_session.read.parquet(f"@{uri['xcom_stage']}/{uri['xcom_key']}")
        
        elif isinstance(arg, dict):
            for k, v in arg.items():
                table_name = _is_table_arg(v)
                uri = _try_parse_snowflake_xcom_uri(v)
                if table_name:
                    arg[k] = snowpark_session.table(table_name)
                elif uri and uri['xcom_stage'] and uri['xcom_key'].split('.')[-1] == 'parquet': 
                    arg[k] = snowpark_session.read.parquet(f"@{uri['xcom_stage']}/{uri['xcom_key']}")
                elif isinstance(v, (dict)):
                    _deserialize_snowpark_args(arg.get(k, {}))
                elif isinstance(v, (list)):
                    arg[k] = _deserialize_snowpark_args(arg.get(k, []))
        elif isinstance(arg, list):
            return [_deserialize_snowpark_args(item) for item in arg]
        else:
            return arg
        
    #This function is part of the snowpark jinja template and will be inserted into the callable script.
    # def _deserialize_snowpark_args(arg_dict:dict) -> dict:

        #If using the Snowflake XCOM backend snowpark dataframes serialized to stages would have already been parsed and deserialized.  
        #If not using the XCOM backend we need to deserialize the uri to a Snowpark Dataframe.

        # new_arg_dict={'args':[], 'kwargs':{}}

        #instantiate Table and SnowparkTable args as Snowpark dataframes
        # for arg in arg_dict['args']:
        #     uri = _try_parse_snowflake_xcom_uri(arg)
        #     table_name = _is_table_arg(arg)

        #     if uri and uri['xcom_stage'] and uri['xcom_key'].split('.')[-1] == 'parquet': 
        #         new_arg_dict['args'].append(snowpark_session.read.parquet(f"@{uri['xcom_stage']}/{uri['xcom_key']}"))
        #     elif table_name:
        #         new_arg_dict['args'].append(snowpark_session.table(table_name))
        #     else:
        #         new_arg_dict['args'].append(arg)

        # for arg_name, arg in arg_dict['kwargs'].items():
        #     uri = _try_parse_snowflake_xcom_uri(arg)
        #     table_name = _is_table_arg(arg)

        #     if uri and uri['xcom_stage'] and uri['xcom_key'].split('.')[-1] == 'parquet': 
        #         new_arg_dict['kwargs'][arg_name] = snowpark_session.read.parquet(f"@{uri['xcom_stage']}/{uri['xcom_key']}")
        #     elif table_name:
        #         new_arg_dict['kwargs'][arg_name] = snowpark_session.table(table_name)
        #     else:
        #         new_arg_dict['kwargs'][arg_name] = arg

        # return new_arg_dict
    
    #This function is part of the snowpark jinja template and will be inserted into the callable script.
    def _write_snowpark_dataframe(spdf:Snowpark_DataFrame, multi_index:int):
        try:
            database = temp_data_dict.get('temp_data_db') or snowpark_session.get_current_database().replace("\"","")
            schema = temp_data_dict.get('temp_data_schema') or snowpark_session.get_current_schema().replace("\"","")
        except: 
            assert database and schema, "To serialize Snowpark dataframes the database and schema must be set in temp_data params, operator/decorator, hook or Snowflake user session defaults."
        
        if conn_params['region']:
            base_uri = f"snowflake://{conn_params['account']}.{conn_params['region']}?"
        else:
            base_uri = f"snowflake://{conn_params['account']}?"


        if temp_data_dict['temp_data_output'] == 'stage':
            """
            Save to stage <DATABASE>.<SCHEMA>.<STAGE>/<DAG_ID>/<TASK_ID>/<RUN_ID> 
            and return uri
            snowflake://<ACCOUNT>.<REGION>?&stage=<FQ_STAGE>&key=<DAG_ID>/<TASK_ID>/<RUN_ID>/0/return_value.parquet'
            """

            stage_name = f"{temp_data_dict['temp_data_stage']}".upper()
            fq_stage_name = f"{database}.{schema}.{stage_name}".upper()
            assert len(fq_stage_name.split('.')) == 3, "stage for snowpark dataframe serialization is not fully-qualified"
            
            uri = f"{base_uri}&stage={fq_stage_name}&key={dag_id}/{task_id}/{run_id}/{multi_index}/return_value.parquet"

            spdf.write.copy_into_location(file_format_type="parquet",
                                        overwrite=temp_data_dict['temp_data_overwrite'],
                                        header=True, 
                                        single=True,
                                        location=f"{fq_stage_name}/{dag_id}/{task_id}/{run_id}/{multi_index}/return_value.parquet")

            return uri

        elif temp_data_dict['temp_data_output'] == 'table':
            """
            Save to table <DATABASE>.<SCHEMA>.<PREFIX><DAG_ID>__<TASK_ID>__<TS_NODASH>_INDEX
            and return SnowparkTable object
            SnowparkTable(name=<DATABASE>.<SCHEMA>.<PREFIX><DAG_ID>__<TASK_ID>__<TS_NODASH>_INDEX)
            """
            table_name = f"{temp_data_dict['temp_data_table_prefix'] or ''}{dag_id}__{task_id.replace('.','_')}__{ts_nodash}__{multi_index}".upper()
            fq_table_name = f"{database}.{schema}.{table_name}".upper()
            assert len(fq_table_name.split('.')) == 3, "table for snowpark dataframe serialization is not fully-qualified"

            if temp_data_dict['temp_data_overwrite']:
                mode = 'overwrite'
            else:
                mode = 'errorifexists'

            spdf.write.save_as_table(fq_table_name, mode=mode)

            return SnowparkTable(name=table_name, 
                                 conn_id=snowflake_conn_id, 
                                 metadata={'schema': schema, 'database': database}) #.to_json()
        else:
            raise Exception("temp_data_output must be one of 'stage' | 'table' | None")

    #This function is part of the snowpark jinja template and will be inserted into the callable script.
    def _serialize_snowpark_results(res:Any, multi_index:int):
        if temp_data_dict.get('temp_data_output') in ['stage', 'table']:
            if isinstance(res, Snowpark_DataFrame): 
                return _write_snowpark_dataframe(res, multi_index)
            if isinstance(res, dict):
                for k, v in res.items():
                    if isinstance(v, Snowpark_DataFrame):
                        res[k] = _write_snowpark_dataframe(v, multi_index)
                    elif isinstance(v, (dict)):
                        _serialize_snowpark_results(res.get(k, {}), multi_index)
                    elif isinstance(v, (list)):
                        res[k] = _serialize_snowpark_results(res.get(k, []), multi_index)
                    multi_index+=1
            elif isinstance(res, (list, tuple)):
                tmp = []
                for item in res:
                    tmp.append(_serialize_snowpark_results(item, multi_index))
                    multi_index+=1
                return tmp
                # return [_serialize_snowpark_results(item) for item in res]
            else:
                return res
    
    # def _write_snowpark_dataframe(spdf:Snowpark_DataFrame, multi_index:int):
    #     try:
    #         conn_params['database'] = temp_data_dict.get('temp_data_db') or snowpark_session.get_current_database().replace("\"","")
    #         conn_params['schema'] = temp_data_dict.get('temp_data_schema') or snowpark_session.get_current_schema().replace("\"","")
    #     except: 
    #         assert conn_params['database'] and conn_params['schema'], "To serialize Snowpark dataframes the database and schema must be set in temp_data params, operator/decorator, hook or Snowflake user session defaults."
        
    #     if conn_params['region']:
    #         base_uri = f"snowflake://{conn_params['account']}.{conn_params['region']}?"
    #     else:
    #         base_uri = f"snowflake://{conn_params['account']}?"


    #     if temp_data_dict['temp_data_output'] == 'stage':
    #         """
    #         Save to stage <DATABASE>.<SCHEMA>.<STAGE>/<DAG_ID>/<TASK_ID>/<RUN_ID> 
    #         and return uri
    #         snowflake://<ACCOUNT>.<REGION>?&stage=<FQ_STAGE>&key=<DAG_ID>/<TASK_ID>/<RUN_ID>/0/return_value.parquet'
    #         """

    #         stage_name = f"{temp_data_dict['temp_data_stage']}".upper()
    #         fq_stage_name = f"{conn_params['database']}.{conn_params['schema']}.{stage_name}".upper()
    #         assert len(fq_stage_name.split('.')) == 3, "stage for snowpark dataframe serialization is not fully-qualified"
            
    #         uri = f"{base_uri}&stage={fq_stage_name}&key={dag_id}/{task_id}/{run_id}/{multi_index}/return_value.parquet"

    #         spdf.write.copy_into_location(file_format_type="parquet",
    #                                     overwrite=temp_data_dict['temp_data_overwrite'],
    #                                     header=True, 
    #                                     single=True,
    #                                     location=f"{fq_stage_name}/{dag_id}/{task_id}/{run_id}/{multi_index}/return_value.parquet")

    #         return uri

    #     elif temp_data_dict['temp_data_output'] == 'table':
    #         """
    #         Save to table <DATABASE>.<SCHEMA>.<PREFIX><DAG_ID>__<TASK_ID>__<TS_NODASH>_INDEX
    #         and return SnowparkTable object
    #         SnowparkTable(name=<DATABASE>.<SCHEMA>.<PREFIX><DAG_ID>__<TASK_ID>__<TS_NODASH>_INDEX)
    #         """
    #         table_name = f"{temp_data_dict['temp_data_table_prefix'] or ''}{dag_id}__{task_id.replace('.','_')}__{ts_nodash}__{multi_index}".upper()
    #         fq_table_name = f"{conn_params['database']}.{conn_params['schema']}.{table_name}".upper()
    #         assert len(fq_table_name.split('.')) == 3, "table for snowpark dataframe serialization is not fully-qualified"

    #         if temp_data_dict['temp_data_overwrite']:
    #             mode = 'overwrite'
    #         else:
    #             mode = 'errorifexists'

    #         spdf.write.save_as_table(fq_table_name, mode=mode)

    #         return SnowparkTable(name=table_name, 
    #                              conn_id=snowflake_conn_id, 
    #                              metadata={'schema': conn_params['schema'], 'database': conn_params['database']}) #.to_json()
    #     else:
    #         raise Exception("temp_data_output must be one of 'stage' | 'table' | None")

    # #This function is part of the snowpark jinja template and will be inserted into the callable script.
    # def _serialize_snowpark_results(res):
    #     if temp_data_dict.get('temp_data_output') in ['stage','table']:
    #         multi_index = 0
    #         if isinstance(res, dict):
    #             for return_key, return_value in res.items():
    #                 if isinstance(return_value, Snowpark_DataFrame):
    #                     res[return_key] = _write_snowpark_dataframe(return_value, multi_index)
    #                     multi_index+=1
    #         elif isinstance(res, (list, tuple, set)):
    #             new_res = []
    #             for return_value in res:
    #                 if isinstance(return_value, Snowpark_DataFrame):
    #                     new_res.append(_write_snowpark_dataframe(return_value, multi_index))
    #                     multi_index+=1
    #                 else:
    #                     new_res.append(return_value)
    #             res = type(res)(new_res)
    #         elif isinstance(res, Snowpark_DataFrame):
    #                 res = _write_snowpark_dataframe(res, multi_index)

    #     return res

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

    def execute(self, context: Context) -> Any:
        # serializable_keys = set(super()._iter_serializable_context_keys())
        # serializable_context = context_copy_partial(context, serializable_keys)

        self.run_id = context['run_id']
        self.ts_nodash = context['ts_nodash']

        return super().execute(context=context) #serializable_context)

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

        print('###################')
        print(self.op_args)
        print(self.op_kwargs)
        print('###################')

        self.write_python_script(
            jinja_context=dict(
                conn_params=self.get_snowflake_conn_params(),
                snowflake_conn_id=self.snowflake_conn_id,
                temp_data_dict=self.temp_data_dict,
                dag_id=self.dag_id,
                task_id=self.task_id,
                run_id=self.run_id,
                ts_nodash=self.ts_nodash,
                try_parse_snowflake_xcom_uri_func = dedent(inspect.getsource(_try_parse_snowflake_xcom_uri)),
                is_table_arg_func = dedent(inspect.getsource(self._is_table_arg)),
                deserialize_snowpark_args_func = dedent(inspect.getsource(self._deserialize_snowpark_args)),
                write_snowpark_dataframe_func = dedent(inspect.getsource(self._write_snowpark_dataframe)),
                serialize_snowpark_results_func = dedent(inspect.getsource(self._serialize_snowpark_results)),
                op_args=self.op_args,
                op_kwargs=op_kwargs,
                expect_airflow=self.expect_airflow,
                pickling_library=self.pickling_library.__name__,
                python_callable=self.python_callable.__name__,
                python_callable_source=self.get_python_source(),
            ),
            filename=os.fspath(script_path),
            render_template_as_native_obj=False,
        )

        print(script_path.read_text())

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
    
     


def test():
    import pandas as pd
    from astronomer.providers.snowflake.xcom_backends.snowflake import SnowflakeXComBackend
    
    from snowflake.snowpark import DataFrame as Snowpark_DataFrame
    from snowflake.snowpark import Session as SnowparkSession
    from snowflake.snowpark import functions as F
    from snowflake.snowpark import types as T

    import pickle
    import sys
    try:
        from snowflake.snowpark import Session as SnowparkSession
        from snowflake.snowpark import functions as F
        from snowflake.snowpark import types as T
    except: 
        raise Exception('Snowpark libraries are not installed in the environment.')

    try:
        from astro.sql.table import Table, TempTable
    except: 
        Table = TempTable = None

    try: 
        from astronomer.providers.snowflake import SnowparkTable
    except: 
        SnowparkTable = None

    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    from astronomer.providers.snowflake.operators.snowpark import SnowparkVirtualenvOperator

    conn_params = SnowflakeHook()._get_conn_params()
    # conn_params['database']=None
    # conn_params['schema']=None

    snowpark_session = SnowparkSession.builder.configs(conn_params).create()

    temp_data_dict = {
            'temp_data_output': 'stage',
            'temp_data_db': 'sissyg',
            'temp_data_schema': 'demo',
            'temp_data_stage': 'xcom_stage',
            'temp_data_table_prefix': 'XCOM__',
            'temp_data_overwrite': True
        }
    dag_id='customer_analytics'
    task_id='score_sentiment.call_sentiment'
    run_id='manual__2023-05-23T07:40:44.577105+00:00'
    ts_nodash = '20230523T074044'

    spdf = snowpark_session.table('SISSYG.DEMO.STG_AD_SPEND')
    
    temp_data_dict['temp_data_output']=None
    spdf = snowpark_session.table('SISSYG.DEMO.STG_AD_SPEND')
    assert spdf == process_snowpark_results(spdf)

    temp_data_dict['temp_data_output']='table'
    spdf = snowpark_session.table('SISSYG.DEMO.STG_AD_SPEND')
    res = process_snowpark_results(spdf)
    assert (spdf.to_pandas() == snowpark_session.table(f'{res.metadata.database}.{res.metadata.schema}.{res.name}').to_pandas()).values.argmax() == 0

    temp_data_dict['temp_data_output']='stage'
    spdf = snowpark_session.table('SISSYG.DEMO.STG_AD_SPEND')
    res = process_snowpark_results(spdf)
    assert (spdf.to_pandas() == SnowflakeXComBackend()._deserialize(uri=res)).values.argmax() == 0

    temp_data_dict['temp_data_output']='table'
    spdf = (snowpark_session.table('SISSYG.DEMO.STG_AD_SPEND'), snowpark_session.table('SISSYG.DEMO.STG_CUSTOMERS'))
    process_snowpark_results(spdf)
    
    spdf = [snowpark_session.table('SISSYG.DEMO.STG_AD_SPEND'), snowpark_session.table('SISSYG.DEMO.STG_CUSTOMERS')]
    process_snowpark_results(spdf)

    spdf = {'df1': snowpark_session.table('SISSYG.DEMO.STG_AD_SPEND'), 'df2': snowpark_session.table('SISSYG.DEMO.STG_CUSTOMERS')}
    process_snowpark_results(spdf)

    temp_data_dict['temp_data_output']='stage'
    spdf = (snowpark_session.table('SISSYG.DEMO.STG_AD_SPEND'), snowpark_session.table('SISSYG.DEMO.STG_CUSTOMERS'))
    res = process_snowpark_results(spdf)
    for i in zip(spdf, res):
        assert (i[0].to_pandas() == SnowflakeXComBackend()._deserialize(uri=i[1])).values.argmax() == 0
    assert type(res) == type(spdf)
        
    spdf = [snowpark_session.table('SISSYG.DEMO.STG_AD_SPEND'), snowpark_session.table('SISSYG.DEMO.STG_CUSTOMERS')]
    res = process_snowpark_results(spdf)
    for i in zip(spdf, res):
        assert (i[0].to_pandas() == SnowflakeXComBackend()._deserialize(uri=i[1])).values.argmax() == 0
    assert type(res) == type(spdf)
    {'df': df, 'df1': df1, 'mystr': 'success', 'dfs': {'d2': df2, 'df8': df8, 'df9': df9} }
    spdf = {'df1': snowpark_session.table('SISSYG.DEMO.STG_AD_SPEND'), 'df2': snowpark_session.table('SISSYG.DEMO.STG_CUSTOMERS')}
    res = process_snowpark_results(spdf.copy())
    for i in zip(spdf.values(), res.values()):
        assert (i[0].to_pandas() == SnowflakeXComBackend()._deserialize(uri=i[1])).values.argmax() == 0
    assert type(res) == type(spdf)

    
    test_arg1='snowflake://FY02423-SNOWSERVICES_PREVIEW?&stage=SISSYG.DEMO.XCOM_STAGE&key=snowpark_test_dag/SPtask/manual__2023-05-23T16:42:51.112381+00:00/0/return_value.parquet'
    test_arg2=SnowparkTable(name='XCOM__CUSTOMER_ANALYTICS__SCORE_SENTIMENT_CALL_SENTIMENT__20230523T074044', conn_id='snowflake_default', metadata={'schema': 'demo', 'database': 'sissyg'})
    test_arg4=SnowparkTable(name='sissyg.demo.XCOM__CUSTOMER_ANALYTICS__SCORE_SENTIMENT_CALL_SENTIMENT__20230523T074044', conn_id='snowflake_default').to_json()
    test_arg3={'class': 'SnowparkTable', 'name': 'XCOM_SNOWPARK_TEST_DAG__SPTASK__20230524T104305__0', 'metadata': {'schema': 'DEMO', 'database': 'SISSYG'}, 'conn_id': 'snowflake_default'}

    def _write_snowpark_dataframe(spdf:Snowpark_DataFrame, multi_index:int):
        try:
            database = temp_data_dict.get('temp_data_db') or snowpark_session.get_current_database().replace("\"","")
            schema = temp_data_dict.get('temp_data_schema') or snowpark_session.get_current_schema().replace("\"","")
        except: 
            assert database and schema, "To serialize Snowpark dataframes the database and schema must be set in temp_data params, operator/decorator, hook or Snowflake user session defaults."
        
        if conn_params['region']:
            base_uri = f"snowflake://{conn_params['account']}.{conn_params['region']}?"
        else:
            base_uri = f"snowflake://{conn_params['account']}?"


        if temp_data_dict['temp_data_output'] == 'stage':
            """
            Save to stage <DATABASE>.<SCHEMA>.<STAGE>/<DAG_ID>/<TASK_ID>/<RUN_ID> 
            and return uri
            snowflake://<ACCOUNT>.<REGION>?&stage=<FQ_STAGE>&key=<DAG_ID>/<TASK_ID>/<RUN_ID>/0/return_value.parquet'
            """

            stage_name = f"{temp_data_dict['temp_data_stage']}".upper()
            fq_stage_name = f"{database}.{schema}.{stage_name}".upper()
            assert len(fq_stage_name.split('.')) == 3, "stage for snowpark dataframe serialization is not fully-qualified"
            
            uri = f"{base_uri}&stage={fq_stage_name}&key={dag_id}/{task_id}/{run_id}/{multi_index}/return_value.parquet"

            # spdf.write.copy_into_location(file_format_type="parquet",
            #                             overwrite=temp_data_dict['temp_data_overwrite'],
            #                             header=True, 
            #                             single=True,
            #                             location=f"{fq_stage_name}/{dag_id}/{task_id}/{run_id}/{multi_index}/return_value.parquet")

            return uri

        elif temp_data_dict['temp_data_output'] == 'table':
            """
            Save to table <DATABASE>.<SCHEMA>.<PREFIX><DAG_ID>__<TASK_ID>__<TS_NODASH>_INDEX
            and return SnowparkTable object
            SnowparkTable(name=<DATABASE>.<SCHEMA>.<PREFIX><DAG_ID>__<TASK_ID>__<TS_NODASH>_INDEX)
            """
            table_name = f"{temp_data_dict['temp_data_table_prefix'] or ''}{dag_id}__{task_id.replace('.','_')}__{ts_nodash}__{multi_index}".upper()
            fq_table_name = f"{database}.{schema}.{table_name}".upper()
            assert len(fq_table_name.split('.')) == 3, "table for snowpark dataframe serialization is not fully-qualified"

            if temp_data_dict['temp_data_overwrite']:
                mode = 'overwrite'
            else:
                mode = 'errorifexists'

            # spdf.write.save_as_table(fq_table_name, mode=mode)

            return SnowparkTable(name=table_name, 
                                 conn_id=snowflake_conn_id, 
                                 metadata={'schema': schema, 'database': database}) #.to_json()
        else:
            raise Exception("temp_data_output must be one of 'stage' | 'table' | None")

    def _serialize_snowpark_results(res:Any, multi_index:int):
        if temp_data_dict.get('temp_data_output') in ['stage', 'table']:
            if isinstance(res, Snowpark_DataFrame): 
                return _write_snowpark_dataframe(res, multi_index)
            if isinstance(res, dict):
                for k, v in res.items():
                    if isinstance(v, Snowpark_DataFrame):
                        res[k] = _write_snowpark_dataframe(v, multi_index)
                    elif isinstance(v, (dict)):
                        _serialize_snowpark_results(res.get(k, {}), multi_index)
                    elif isinstance(v, (list)):
                        res[k] = _serialize_snowpark_results(res.get(k, []), multi_index)
                    multi_index+=1
            elif isinstance(res, list):
                tmp = []
                for item in res:
                    tmp.append(_serialize_snowpark_results(item, multi_index))
                    multi_index+=1
                return tmp
                # return [_serialize_snowpark_results(item) for item in res]
            else:
                return res

    temp_data_dict ['temp_data_output']='stage'
    # res = {'df2': snowpark_session.table(test_arg2.name), 'df3': snowpark_session.table(test_arg3['name']), 'mystr': 'success', 'dfs': {'d2': snowpark_session.table(test_arg2.name), 'df8': snowpark_session.table(test_arg4['name'])} }
    # res = {'df2': snowpark_session.table(test_arg2.name), 'df4': snowpark_session.table(test_arg4['name'])}
    res = [snowpark_session.table(test_arg2.name), snowpark_session.table(test_arg4['name']), 'string1']
    res = ['string1']
    res = 'string1'
    res = snowpark_session.table(test_arg2.name)

    if isinstance(res, dict):
        _serialize_snowpark_results(res, 0)
    else:
        res = _serialize_snowpark_results(res, 0)
    res

    
    ###WORKS####
    # arg_dict={'args': [test_arg1, test_arg2, 'string', dict(), list(), pd.DataFrame()], 'kwargs': {'df1': test_arg2, 'df2': test_arg1}, 'df3': pd.DataFrame()}
    # def _deserialize_snowpark_args(arg:Any):
    #     table_name = _is_table_arg(arg)
    #     uri = _try_parse_snowflake_xcom_uri(arg)

    #     if table_name:
    #         return snowpark_session.table(table_name)
        
    #     elif uri and uri['xcom_stage'] and uri['xcom_key'].split('.')[-1] == 'parquet': 
    #         return snowpark_session.read.parquet(f"@{uri['xcom_stage']}/{uri['xcom_key']}")
        
    #     elif isinstance(arg, dict):
    #         for k, v in arg.items():
    #             table_name = _is_table_arg(v)
    #             uri = _try_parse_snowflake_xcom_uri(v)
    #             if table_name:
    #                 arg[k] = snowpark_session.table(table_name)
    #             elif uri and uri['xcom_stage'] and uri['xcom_key'].split('.')[-1] == 'parquet': 
    #                 arg[k] = snowpark_session.read.parquet(f"@{uri['xcom_stage']}/{uri['xcom_key']}")
    #             elif isinstance(v, (dict)):
    #                 _deserialize_snowpark_args(arg.get(k, {}))
    #             elif isinstance(v, (list)):
    #                 arg[k] = _deserialize_snowpark_args(arg.get(k, []))
    #     elif isinstance(arg, list):
    #         return [_deserialize_snowpark_args(item) for item in arg]
    #     else:
    #         return arg
        
    # arg_dict={'args': [test_arg1, test_arg2, 'string1', {'x','y'}, [[['a','b']]], pd.DataFrame()], 'kwargs': {'df1': test_arg1, 'df2': test_arg2, 'df3': test_arg3, 'df': pd.DataFrame(['x'])}}
    # _deserialize_snowpark_args(arg_dict)
    # arg_dict


    # ######WORKS#######
    # def deserialize(obj):
    #     # print(f"{obj=}")
    #     if table_name := _is_table_arg(obj):
    #         return 'TABLE'
    #     elif uri := _try_parse_snowflake_xcom_uri(obj):
    #         return 'URI'
    #     elif isinstance(obj, dict):
    #         for k, v in obj.items():
    #             if table_name := _is_table_arg(v):
    #                 obj[k] = 'TABLE'
    #             elif uri := _try_parse_snowflake_xcom_uri(v):
    #                 obj[k] = 'URI'
    #             elif isinstance(v, (dict)):
    #                 deserialize(obj.get(k, {}))
    #                 # deserialize(v)
    #             elif isinstance(v, (list)):
    #                 obj[k] = deserialize(obj.get(k, []))
    #                 # obj[k] = deserialize(v)
    #     elif isinstance(obj, list):
    #         return [deserialize(item) for item in obj]
    #     else:
    #         return obj
    
    # arg_dict={'args': [test_arg1, test_arg2, 'string1', {'x','y'}, [[['a','b']]], pd.DataFrame()], 'kwargs': {'df1': test_arg1, 'df2': test_arg2, 'df3': test_arg3, 'df': pd.DataFrame(['x'])}}
    # deserialize(arg_dict)
    # arg_dict
    
    # import collections

    # def update(orig_dict, new_dict):
    #     for key, val in new_dict.iteritems():
    #         if isinstance(val, collections.abc.Mapping):
    #             tmp = update(orig_dict.get(key, { }), val)
    #             orig_dict[key] = tmp
    #         elif isinstance(val, list):
    #             orig_dict[key] = (orig_dict.get(key, []) + val)
    #         else:
    #             orig_dict[key] = new_dict[key]
    #     return orig_dict

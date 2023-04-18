 from __future__ import annotations

try:
    from airflow.decorators.base import TaskDecorator, task_decorator_factory
except ImportError:
    from airflow.decorators.base import task_decorator_factory
    from airflow.decorators import _TaskDecorator as TaskDecorator

from astronomer.providers.snowflake.utils.table import find_first_table, Table
from astronomer.providers.snowflake.operators.snowpark import (
    BaseSnowparkOperator,
    load_op_arg_table_into_dataframe,
    load_op_kwarg_table_into_dataframe,
)
from astronomer.providers.snowflake.hooks.snowpark import SnowparkHook

try:
    import snowflake.snowpark  # noqa
except ImportError:
    raise AirflowException(
        "The snowflake-snowpark-python package is not installed. Make sure you are using Python 3.8."
    )


class SnowparkDataframeOperator(BaseSnowparkOperator):
    """
    Run a task with a `snowpark_session` and return a Snowpark dataframe.
    """

    def __init__(
        self,
        *,
        conn_id: str = "snowflake_default",
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
        self.task_id = kwargs.get("task_id")
        self.conn_id = conn_id
        self.parameters = parameters
        self.warehouse = warehouse
        self.database = database
        self.role = role
        self.schema = schema
        self.authenticator = authenticator
        self.session_parameters = session_parameters

        super().__init__(
            task_id=self.task_id,
            conn_id=self.conn_id,
            parameters=self.parameters,
            warehouse=self.warehouse,
            database=self.database,
            role=self.role,
            schema=self.schema,
            authenticator=self.authenticator,
            session_parameters=self.session_parameters,
            python_callable=python_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
        )

    def execute(self, context: Context) -> Table | list:
        first_table = find_first_table(
            op_args=self.op_args,  # type: ignore
            op_kwargs=self.op_kwargs,
            python_callable=self.python_callable,
            parameters=self.parameters or {},  # type: ignore
            context=context,
        )
        if first_table:
            self.conn_id = self.conn_id or first_table.conn_id  # type: ignore
        else:
            if not self.conn_id:
                raise ValueError("You need to provide a table or a connection id")

        # Set real sessions as an argument to the function.
        hook = SnowparkHook(snowflake_conn_id=self.conn_id)
        conn = hook.get_conn()
        snowpark_session = hook.get_snowpark_session()
        op_kwargs = dict(self.op_kwargs)

        # Load dataframes from arguments tagged with `Table`.
        self.op_args = load_op_arg_table_into_dataframe(
            self.op_args,
            self.python_callable,
            self.columns_names_capitalization,
            self.log,
        )
        self.op_kwargs = load_op_kwarg_table_into_dataframe(
            self.op_kwargs,
            self.python_callable,
            self.columns_names_capitalization,
            self.log,
        )
        self.op_kwargs["snowpark_session"] = snowpark_session

        function_output = self.python_callable(*self.op_args, **self.op_kwargs)

        if function_output:
            if isinstance(
                function_output, snowflake.snowpark.dataframe.DataFrame
            ) or isinstance(function_output, snowflake.snowpark.table.Table):
                if isinstance(function_output, snowflake.snowpark.dataframe.DataFrame):
                    # Snowpark DataFrames stored in temp tables, in temp schema
                    _table_name = "__".join(["TMP", self.dag_id, self.task_id])
                    table_name = ".".join([conn.database, self.TMP_SCHEMA, _table_name])
                elif isinstance(function_output, snowflake.snowpark.table.Table):
                    table_name = function_output.table_name
                function_output.write.mode("overwrite").save_as_table(table_name)
                output = Table(name=table_name, conn_id=self.conn_id)

            elif isinstance(function_output, pd.DataFrame):
                raise ValueError(
                    "Snowpark decorator only handles Snowpark dataframes, not Pandas dataframes. "
                    "Please change your function output."
                )
                pass
            else:
                output = function_output

            try:
                return output
            finally:
                snowpark_session.close()


def dataframe_decorator(
    python_callable: Callable | None = None,
    multiple_outputs: bool | None = None,
    conn_id: str = "",
    database: str | None = None,
    schema: str | None = None,
    **kwargs,
) -> TaskDecorator:
    """Enables Snowpark tasks.

    Returned Snowpark dataframes are stored either as Snowflake tables or
    serialized Pandas dataframes - depending on the need of downstream tasks.

    By identifying the parameters as `Table` objects, astro knows to automatically convert those
    objects into tables (if they are, for example, a Snowpark dataframe). Any type besides
    table will lead to assume you do not want the parameter converted.

    :param python_callable: Function to decorate
    :param multiple_outputs: If set to True, the decorated function's return value will be unrolled to
        multiple XCom values. Dict will unroll to XCom values with its keys as XCom keys. Defaults to False.
    """
    kwargs.update(
        {
            "conn_id": conn_id,
            "database": database,
            "schema": schema,
        }
    )
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=SnowparkDataframeOperator,
        **kwargs,
    )

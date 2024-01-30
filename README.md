# Apache Airflow Provider for Snowpark

This provider from Astronomer extends the capabilities of the base [Snowflake Airflow Provider](https://github.com/apache/airflow/tree/main/airflow/providers/snowflake) with additional components to interact with [Snowpark](https://docs.snowflake.com/en/developer-guide/snowpark/index).

> [!WARNING]
> This provider is currently in **beta**, and is not yet recommended for production use. Please report any issues you encounter. 

## Installation

### Astro CLI / Astronomer customers

Make sure you are running [Astro Runtime](https://docs.astronomer.io/astro/runtime-release-notes) version 10.0.0+.

The Snowpark provider depends on the following packages:

- Base [Snowflake Airflow Provider](https://github.com/apache/airflow/tree/main/airflow/providers/snowflake)
- [Snowflake Snowpark Python](https://docs.snowflake.com/en/developer-guide/snowpark/python/index.html)

When using the Astro CLI you can install these packages by adding them to your `requirements.txt` file as shown below togehter with the Snowpark provider as shown below.

```text
apache-airflow-providers-snowflake==5.2.0
snowflake-snowpark-python[pandas]==1.11.1
git+https://github.com/astronomer/astro-provider-snowflake.git
```

You will also need to add `git` to your `packages.txt` file to be able to install directly from a GH repository.

```text
git
```

### OSS Airflow

Make sure you are running [Airflow 2.8.0+](https://airflow.apache.org/docs/apache-airflow/stable/index.html).

Install the following prerequisites:

```bash
pip install apache-airflow-providers-snowflake==5.2.0
pip install snowflake-snowpark-python[pandas]==1.11.1
```

Install the Astronomer Snowflake Provider by running the following command:

```bash
pip install git+https://github.com/astronomer/astro-provider-snowflake.git
```

## Provider Contents

### Operators

All operators instantiate a Snowpark Python session named `snowpark_session` which can be referenced in the python callable. 

- `SnowparkPythonOperator`: This is the simplest operator which runs as a PythonOperator in the Airflow instance. This requires that the Airflow instance is running a version of python supported by Snowpark and has Snowpark Python package installed. NOTE: Currently Snowpark supports python 3.8, 3.9 and 3.10.
- `SnowparkVirtualenvOperator`: This operator creates a python virtualenv to run the python callable in a subprocess. Users can specify python package requirements (ie. snowflake-snowpark-python).  
- `SnowparkExternalPythonOperator`: This operator runs the Snowpark python callable in a pre-existing virtualenv. It is assumed that Snowpark is already installed in that environment. Using the [Astronomer buildkit](https://github.com/astronomer/astro-provider-venv) will simplify building this environment.

Example use:

```python
def my_callable(table_name):
    df = snowpark_session.table(table_name)
    df.show()
    # your Snowpark code here


my_task = SnowparkPythonOperator(
    task_id="my_task",
    snowflake_conn_id=YOUR_SNOWFLAKE_CONN_ID,
    database=YOUR_SNOWFLAKE_DB,
    schema=YOUR_SNOWFLAKE_SCHEMA,
    python_callable=my_callable,
    op_kwargs={"table_name": YOUR_SNOWFLAKE_TABLE},
)
```

### Decorators

All decorators instantiate a Snowpark Python session named `snowpark_session` which can be referenced in the python callable. 

- `snowpark_python`: Corresponding [TaskFlow API](https://docs.astronomer.io/learn/airflow-decorators) decorator for `SnowparkPythonOperator` (i.e @task.snowpark_python()).
- `snowpark_virtualenv`: Corresponding TaskFlow API decorator for `SnowparkVirtualenvOperator` (i.e @task.snowpark_virtualenv()).
- `snowpark_ext_python`: Corresponding TaskFlow API decorator for `SnowparkExternalPythonOperator` (i.e @task.snowpark_ext_python()).

Example use:

```python
@task.snowpark_python(
    task_id="my_task",
    snowflake_conn_id=YOUR_SNOWFLAKE_CONN_ID,
    database=YOUR_SNOWFLAKE_DB,
    schema=YOUR_SNOWFLAKE_SCHEMA,
)
def my_callable(table_name):
    df = snowpark_session.table(table_name)
    df.show()
    # your Snowpark code here

my_callable(table_name=YOUR_SNOWFLAKE_TABLE)
```

### Custom Xcom Backend

The `SnowflakeXcomBackend` is a [custom XCom backend](https://docs.astronomer.io/learn/xcom-backend-tutorial) designed to provide additional security and data governance this feature allows storing task input and output via [XCom](https://docs.astronomer.io/learn/airflow-passing-data-between-tasks#xcom) in Snowflake. Rather than storing potentially-sensitive data in the Airflow XCom tables Snowflake users can now ensure that all their data stays in Snowflake. JSON-serializable data is stored in an XCom table and non-JSON serializable data is stored as objects in a Snowflake stage.

To enable the Snowflake XCom backend, you first need to create a suitable Snowflake Table and Stage. 

The table must have the following schema:

```sql
dag_id varchar NOT NULL, 
task_id varchar NOT NULL, 
run_id varchar NOT NULL,
multi_index integer NOT NULL,
key varchar NOT NULL,
value_type varchar NOT NULL,
value varchar NOT NULL
```

For the stage, create an internal Snowflake Stage, see [Choosing an Internal Stage for Local Files](https://docs.snowflake.com/en/user-guide/data-load-local-file-system-create-stage).

Once the table and stage are created, you can enable the Snowflake XCom backend by setting the following environment variables in your Airflow environment. Make sure to reference the table and stage you created above and your Snowflake connection name.

```text
AIRFLOW__CORE__XCOM_BACKEND=snowpark_provider.xcom_backends.snowflake.SnowflakeXComBackend
AIRFLOW__CORE__XCOM_SNOWFLAKE_TABLE='AIRFLOW_XCOM_DB.AIRFLOW_XCOM_SCHEMA.XCOM_TABLE'
AIRFLOW__CORE__XCOM_SNOWFLAKE_STAGE='AIRFLOW_XCOM_DB.AIRFLOW_XCOM_SCHEMA.XCOM_STAGE'
AIRFLOW__CORE__XCOM_SNOWFLAKE_CONN_NAME='snowflake_default'
```

## Learn More

Find more information about the Snowpark provider in the [Snowpark Provider tutorial](https://docs.astronomer.io/learn/airflow-snowpark).

## Future Work

[Snowpark Containers](https://www.snowflake.com/snowpark-container-services/): Snowflake's containerized compute offering (currently in Private Preview) provides managed, container-based compute "next" to the data in Snowflake.  Containerized compute simplifies dependency management and allows applications (in any language) to run along with the data and any specialized compute requirements (ie. GPUs) in Snowflake.

- Hook: The Snowpark Containers hook simplifies connection to Snowpark Containers and provides a python interface for CRUD operations on Snowpark Containers services. Additionally the hook has integrations with the [Astro CLI](https://github.com/astronomer/astro-cli) to support local development. Snowpark Containers can be created in local docker containers for integration and testing before being pushed to Snowpark Containers service. 
- Python Operator: The Snowpark Containers python operator is used in conjunction with a Airflow Task Runner [(see example)](https://github.com/astronomer/airflow-snowpark-containers-demo/tree/main/include/airflow-runner) container to bring code to the data. This allows execution of Python callable functions where the data sits in Snowflake. 
- Decorator: The Taskflow decorator provides cleaner Airflow DAG code while simplifying task input and output, and lineage tracking.
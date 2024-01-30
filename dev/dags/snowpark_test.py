from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import os
import requests
from astro import sql as aql
from astro.sql.table import Table
from astro.files import File
from snowpark_provider import SnowparkTable
from snowpark_provider.hooks.snowpark import SnowparkContainersHook

from snowpark_provider.operators.snowpark import (
    SnowparkPythonOperator,
    SnowparkVirtualenvOperator,
    SnowparkExternalPythonOperator,
    SnowparkContainersPythonOperator,
)

_SNOWFLAKE_CONN_ID = "snowflake_default"
_SNOWPARK_BIN = "/home/astro/.venv/snowpark/bin/python"
_TEST_SCHEMA = os.environ["TEST_SCHEMA"]
_TEST_DATABASE = os.environ["TEST_DATABASE"]
_LOCAL_MODE = True
_SERVICE_NAME = "TEST"
_TABLE_NAME = "TAXI_RAW"

_PACKAGES = ["pandas"]

default_args = {
    "owner": "Airflow",
    "temp_data_output": "stage",
    "temp_data_stage": "xcom_stage",
    "temp_data_overwrite": True,
    "runner_endpoint": "http://airflow-task-runner:8001/task",
}


@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["example"],
)
def snowpark_test_dag():
    raw_table = aql.load_file(
        input_file=File("include/data/yellow_tripdata_sample_2019_01.csv"),
        output_table=Table(name=_TABLE_NAME, conn_id=_SNOWFLAKE_CONN_ID),
        if_exists="replace",
    )

    @task.snowpark_python(task_id="SPdec", temp_data_output="table")
    def spdec(table_name):
        from snowflake.snowpark.row import Row

        df = snowpark_session.table(table_name)

        test_row = Row(
            VENDOR_ID=2,
            PICKUP_DATETIME="2019-01-05 06:36:51",
            DROPOFF_DATETIME="2019-01-05 06:50:42",
            PASSENGER_COUNT=1,
            TRIP_DISTANCE=3.72,
            RATE_CODE_ID=1,
            STORE_AND_FWD_FLAG="N",
            PICKUP_LOCATION_ID=68,
            DROPOFF_LOCATION_ID=236,
            PAYMENT_TYPE=1,
            FARE_AMOUNT=14.0,
            EXTRA=0.0,
            MTA_TAX=0.5,
            TIP_AMOUNT=1.0,
            TOLLS_AMOUNT=0.0,
            IMPROVEMENT_SURCHARGE=0.3,
            TOTAL_AMOUNT=15.8,
            CONGESTION_SURCHARGE=None,
        )

        assert test_row == df.collect()[0]

        return df

    SPdec = spdec(table_name=_TABLE_NAME)

    @task.snowpark_ext_python(
        task_id="EPdec", python=_SNOWPARK_BIN, temp_data_schema=_TEST_SCHEMA
    )
    def epdec(rawdf):
        newdf = snowpark_session.table(rawdf.table_name)

        assert newdf.collect() == rawdf.collect()

        return newdf.to_pandas()

    EPdec = epdec(rawdf=SPdec)

    @task.snowpark_virtualenv(
        task_id="VEdec",
        python_version="3.9",
        requirements=_PACKAGES,
        database=_TEST_DATABASE,
        temp_data_output="table",
    )
    def vedec(newdf, rawdf):
        import pandas as pd

        assert pd.DataFrame.equals(newdf, rawdf.to_pandas())

        return newdf

    VEdec = vedec(newdf=EPdec, rawdf=SPdec)

    def spop(newdf):
        return newdf

    SPop = SnowparkPythonOperator(
        task_id="SPop",
        python_callable=spop,
        database=_TEST_DATABASE,
        schema=_TEST_SCHEMA,
        temp_data_output="stage",
        temp_data_stage="xcom_stage",
        snowflake_conn_id="snowflake_default",
        op_kwargs={"newdf": "{{ ti.xcom_pull(task_ids='VEdec') }}"},
    )

    def veop(df):
        print("########")
        print(type(df))
        print(df[0])
        return df

    VEop = SnowparkVirtualenvOperator(
        task_id="VEop",
        python_callable=veop,
        python_version="3.9",
        requirements=_PACKAGES,
        op_kwargs={"df": "{{ ti.xcom_pull(task_ids='SPop') }}"},
    )

    def epop(df):
        return df

    EPop = SnowparkExternalPythonOperator(
        task_id="EPop",
        python="/usr/local/bin/python3.8",
        python_callable=epop,
        op_kwargs={"df": "{{ ti.xcom_pull(task_ids='VEop') }}"},
    )
    VEdec >> SPop >> VEop >> EPop

    @task
    def check_runner_status(service_name: str):
        if _LOCAL_MODE:
            runner_conn = {
                "dns_name": default_args["runner_endpoint"],
                "url": default_args["runner_endpoint"],
                "headers": {},
            }

        else:
            hook = SnowparkContainersHook(
                snowflake_conn_id=_SNOWFLAKE_CONN_ID,
                session_parameters={"PYTHON_CONNECTOR_QUERY_RESULT_FORMAT": "json"},
            )

            service = hook.list_services(service_name=service_name)[
                service_name.upper()
            ]
            assert service.get(
                "dns_name", {}
            ), f"{service_name} service does not appear to be instantiated."

            for container, container_status in service["container_status"].items():
                assert (
                    container_status["status"] == "READY"
                ), f"In service  container {container} is not running."

            service_urls, service_public_headers = hook.get_service_urls(
                service_name=service_name
            )

            runner_conn = {
                "dns_name": f"http://{service.get('dns_name', {})}:8001",
                "url": service_urls["airflow-task-runner"],
                "headers": service_public_headers,
            }

        response = requests.get(
            url=runner_conn["url"].split("/task")[0], headers=runner_conn["headers"]
        )
        assert (
            response.text == '"pong"' and response.status_code == 200
        ), "Airflow Task Runner does not appear to be running"

    def scsop(rawdf: SnowparkTable):
        from snowflake.snowpark.row import Row

        test_row = Row(
            VENDOR_ID=1,
            PICKUP_DATETIME="2019-01-23 15:22:13",
            DROPOFF_DATETIME="2019-01-23 15:32:50",
            PASSENGER_COUNT=1,
            TRIP_DISTANCE=3.3,
            RATE_CODE_ID=1,
            STORE_AND_FWD_FLAG="N",
            PICKUP_LOCATION_ID=12,
            DROPOFF_LOCATION_ID=232,
            PAYMENT_TYPE=2,
            FARE_AMOUNT=12.5,
            EXTRA=0.0,
            MTA_TAX=0.5,
            TIP_AMOUNT=0.0,
            TOLLS_AMOUNT=0.0,
            IMPROVEMENT_SURCHARGE=0.3,
            TOTAL_AMOUNT=13.3,
            CONGESTION_SURCHARGE=0.0,
        )

        assert test_row == rawdf.collect()[1]

        return rawdf

    SCSop = SnowparkContainersPythonOperator(
        task_id="SCSop", python_callable=scsop, op_args=[raw_table]
    )

    @task.snowpark_containers_python()
    def scsdec(rawdf: SnowparkTable):
        import pandas as pd

        test_values = [
            2,
            "2019-01-04 10:54:47",
            "2019-01-04 11:18:31",
            2,
            3.09,
            1,
            "N",
            234,
            236,
            1,
            17.0,
            0.0,
            0.5,
            3.56,
            0.0,
            0.3,
            21.36,
        ]

        df = rawdf.to_pandas()

        assert df.iloc[2].dropna().values.tolist() == test_values
        return snowpark_session.create_dataframe(df)

    SCSdec = scsdec(raw_table)

    check_runner_status(_SERVICE_NAME) >> SCSop >> SCSdec


snowpark_test_dag()


def test():
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    from snowflake.snowpark import Session as SnowparkSession
    from snowflake.snowpark import functions as F, types as T

    conn_params = SnowflakeHook()._get_conn_params()
    snowpark_session = SnowparkSession.builder.configs(conn_params).create()

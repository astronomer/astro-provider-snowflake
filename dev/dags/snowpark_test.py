import pandas as pd
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from astro import sql as aql
from astro.sql.table import Table
from astro.files import File
from astronomer.providers.snowflake.utils.snowpark_helpers import SnowparkTable

#test import
from astronomer.providers.snowflake.operators.snowpark import (
    SnowparkPythonOperator, 
    SnowparkVirtualenvOperator, 
    SnowparkExternalPythonOperator
)

from astronomer.providers.snowflake.decorators.snowpark import (
    snowpark_python_task,
    snowpark_virtualenv_task,
    snowpark_ext_python_task
)

_SNOWFLAKE_CONN_ID = 'snowflake_default'
_SNOWPARK_BIN = '/home/astro/.venv/snowpark/bin/python'

PACKAGES = [
    # "snowflake-snowpark-python",
    # "scikit-learn",
    # "dill",
    "pandas",
    # "numpy",
    # "joblib",
    # "cachetools",
]


@dag(
    default_args={"owner": "Airflow", 
                  "temp_data_output": 'stage',
                  "temp_data_stage": 'xcom_stage',
                  "temp_data_overwrite": True},
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["example"],
)
def snowpark_test_dag():

    raw_table = aql.load_file(
        input_file = File('include/data/yellow_tripdata_sample_2019_01.csv'), 
        output_table = Table(name='TAXI_RAW', conn_id=_SNOWFLAKE_CONN_ID),
        if_exists='replace'
    )

    @task.snowpark_python(task_id='SPdec', temp_data_output='table')
    def spdec(rawdf:SnowparkTable):
        from snowflake.snowpark.row import Row

        test_row = Row(VENDOR_ID=2, 
                       PICKUP_DATETIME='2019-01-05 06:36:51', 
                       DROPOFF_DATETIME='2019-01-05 06:50:42', 
                       PASSENGER_COUNT=1, 
                       TRIP_DISTANCE=3.72,
                       RATE_CODE_ID=1, 
                       STORE_AND_FWD_FLAG='N', 
                       PICKUP_LOCATION_ID=68, 
                       DROPOFF_LOCATION_ID=236, 
                       PAYMENT_TYPE=1, 
                       FARE_AMOUNT=14.0, 
                       EXTRA=0.0, MTA_TAX=0.5, 
                       TIP_AMOUNT=1.0, 
                       TOLLS_AMOUNT=0.0, 
                       IMPROVEMENT_SURCHARGE=0.3, 
                       TOTAL_AMOUNT=15.8, 
                       CONGESTION_SURCHARGE=None)
        
        assert test_row == rawdf.collect()[0]

        return rawdf
    SPdec = spdec(raw_table)


    @task.snowpark_ext_python(task_id='EPdec', python=_SNOWPARK_BIN, temp_data_schema='XCOM')
    def epdec(rawdf):

        newdf = snowpark_session.table(rawdf.table_name)
        
        assert newdf.collect() == rawdf.collect()

        return newdf
    
    EPdec = epdec(rawdf=SPdec)

    @task.snowpark_virtualenv(task_id='VEdec', python_version='3.9', requirements=PACKAGES, database='sissyg', temp_data_output='table')
    def vedec(newdf, rawdf):
        import pandas as pd

        assert pd.DataFrame.equals(newdf.to_pandas(), rawdf.to_pandas())
        
    VEdec = vedec(newdf=EPdec, rawdf=SPdec)
    
    # from include.tests import test_task, test_task1, test_task2
    # SPop = SnowparkPythonOperator(task_id='SPtask', 
    #                               python_callable=test_task2, 
    #                             #   database='sandbox',
    #                             #   schema='michaelgregory',
    #                               temp_data_output = 'table',
    #                             #   temp_data_stage = 'xcom_stage',
    #                               snowflake_conn_id='snowflake_default',
    #                             #   op_args = [df1, df2, 'testbad'], 
    #                               op_kwargs = {'df': SnowparkTable(name='XCOM_SNOWPARK_TEST_DAG__SPDEC__20230525T110601__0', conn_id='snowflake_default', metadata={'schema': 'DEMO', 'database': 'SISSYG'})}
    #                             )
    # SPop

    # VEop = SnowparkPythonOperator(task_id='VEtask', 
    #                                   python_callable=test_task1, 
    #                                 #   python_version='3.9',
    #                                 #   database='sandbox',
    #                                 # schema='michaelgregory',
    #                                 #   temp_data_output = 'stage',
    #                                 #   temp_data_stage = 'xcom_stage',
    #                                   snowflake_conn_id='snowflake_default',
    #                                 #   requirements=PACKAGES,
    #                                   role='michaelgregory',
    #                                   provide_context=True,
    #                                   op_args = ["{{ ti.xcom_pull(task_ids='SPop') }}"], 
    #                                   op_kwargs = {'string1': 'df3', 'list1': ['df4', 'df6'], 'df2': pd.DataFrame(['df4', 'df6'])})
    # VEop

    # EPop = SnowparkExternalPythonOperator(task_id='EPtask',
    #                                       schema='michaelgregory',
    #                                     #   python='/home/astro/.venv/snowpark/bin/python',
    #                                       python='/usr/local/bin/python3.8',
    #                                     #   python='/usr/local/bin/python3.9',
    #                                       python_callable=test_task2, 
    #                                       op_args = [df1, df2, 'testbad'], 
    #                                       op_kwargs = {'df3': df3, 'df4': df4, 'df6': df6, 'mydict': {}})
    # EPop

    

    



snowpark_test_dag()
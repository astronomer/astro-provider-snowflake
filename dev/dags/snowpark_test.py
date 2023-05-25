import pandas as pd
from airflow.decorators import dag
from airflow.utils.dates import days_ago

from astronomer.providers.snowflake.operators.snowpark import (
    SnowparkVirtualenvOperator, 
    SnowparkExternalPythonOperator,
    SnowparkPythonOperator
)
from astronomer.providers.snowflake.decorators.snowpark import (
    snowpark_python_task,
    snowpark_virtualenv_task,
    snowpark_ext_python_task
)
# from astro.sql.table import Table
from astronomer.providers.snowflake import SnowparkTable

df1 = SnowparkTable('STG_ORDERS', metadata={'database':'sissyg', 'schema':'demo'}, conn_id='snowflake_default')
df2 = SnowparkTable('sissyg.demo.stg_ad_spend', conn_id='snowflake_default')
df3 = SnowparkTable('STG_payments')
df4 = SnowparkTable('stg_customers', metadata={'database':'sissyg'})
df6 = SnowparkTable('stg_sessions', conn_id='snowflake_user')

PACKAGES = [
    # "snowflake-snowpark-python",
    # "scikit-learn",
    # "dill",
    "pandas",
    # "numpy",
    # "joblib",
    # "cachetools",
]

_SNOWPARK_BIN = '/home/astro/.venv/snowpark/bin/python'

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

    @snowpark_python_task(task_id='SPdec', temp_data_output='table')
    def test_task3(df1:SnowparkTable, df2:SnowparkTable, str1:str, df6:SnowparkTable, mydict, df3:SnowparkTable, df4:SnowparkTable):
        
        df1.show()
        df2.show()
        df3.show()
        df4.show()
        df6.show()
        mydict['mystr'] = str1

        return df1
    SPdec = test_task3(df1=df1, df2=df2, str1='testbad', df6=df6, df3=df3, mydict={}, df4=df4)


    @snowpark_ext_python_task(task_id='EPdec', python=_SNOWPARK_BIN, role='michaelgregory')
    def test_task(df):
        df.show()
        df7 = snowpark_session.table('STG_payments')
        df7.show()

        return df, df7
        # return df
    
    EPdec = test_task(df=SPdec)

    @snowpark_virtualenv_task(task_id='VEdec', python_version='3.9', requirements=PACKAGES, database='sissyg', temp_data_output='table')
    def test_task1(dfs, str1:str, mylist:list, df:pd.DataFrame):

        print(dfs)
        df1 = dfs[0]
        df1.show()
        df2 = dfs[0]
        df2.show()

        df8 = pd.concat([df, pd.DataFrame(mylist, columns=['name'])]).reset_index()
        print(df8)

        df9 = snowpark_session.create_dataframe(df8)
        
        return {'df': df.to_json(), 'df1': df1, 'mystr': 'success', 'dfs': {'d2': df2, 'df8': df8.to_json(), 'df9': df9} }
        # return {'df9': df9, 'df1': df1, 'mystr': 'success'}

    VEdec = test_task1(dfs=EPdec, str1='testbad', mylist=['x','y'], df=pd.DataFrame(['b','a'], columns=['name']))
    
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
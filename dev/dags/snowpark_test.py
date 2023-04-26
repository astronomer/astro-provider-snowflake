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

df1 = SnowparkTable('STG_ORDERS', metadata={'database':'SANDBOX', 'schema':'michaelgregory'})
df2 = SnowparkTable('sandbox.michaelgregory.stg_ad_spend')
df3 = SnowparkTable('STG_payments', metadata={'database':'SANDBOX'})
df4 = SnowparkTable('stg_customers')
df6 = SnowparkTable('stg_sessions')

PACKAGES = [
    "snowflake-snowpark-python",
    # "scikit-learn",
    # "pandas",
    # "numpy",
    # "joblib",
    # "cachetools",
]

_SNOWPARK_BIN = '/home/astro/.venv/snowpark/bin/python'

@dag(
    default_args={"owner": "Airflow"},
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["example"],
)
def snowpark_test_dag():

    @snowpark_ext_python_task(task_id='EPdec', python=_SNOWPARK_BIN, role='michaelgregory')
    def test_task(df1:SnowparkTable, df2:SnowparkTable, str1:str, df6:SnowparkTable, mydict, df3:SnowparkTable, df4:SnowparkTable):
        from snowflake.snowpark import version as v
        from snowflake.snowpark.functions import col, sproc, udf
        
        snowpark_session.get_fully_qualified_current_schema()
        
        df1.show()
        df2.show()
        df3.show()
        df4.show()
        df6.show()
        mydict['mystr'] = str1

        return SnowparkTable()
    
    EPdec = test_task(df1=df1, df2=df2, str1='testbad', df6=df6, df3=df3, mydict={}, df4=df4)

    @snowpark_virtualenv_task(task_id='VEdec', python_version='3.8', requirements=PACKAGES, database='sandbox')
    def test_task1(df1:SnowparkTable, df2:SnowparkTable, str1:str, df6:SnowparkTable, mydict, df3:SnowparkTable, df4:SnowparkTable):
        from snowflake.snowpark import version as v
        from snowflake.snowpark.functions import col, sproc, udf
        
        snowpark_session.get_fully_qualified_current_schema()
        
        df1.show()
        df2.show()
        df3.show()
        df4.show()
        df6.show()
        mydict['mystr'] = str1

        return SnowparkTable('STG_ORDERS', metadata={'database':'SANDBOX', 'schema':'michaelgregory'}, conn_id=snowflake_conn_id)
    
    VEdec = test_task1(df1=df1, df2=df2, str1='testbad', df6=df6, df3=df3, mydict={}, df4=df4)
    
    from include.tests import test_task as test_task2
    VEop = SnowparkVirtualenvOperator(task_id='VEtask', 
                                      python_callable=test_task2, 
                                      python_version='3.8',
                                      database='sandbox',
                                      snowflake_conn_id='snowflake_default',
                                      requirements=PACKAGES,
                                      role='michaelgregory',
                                      op_args = [df1, df2, 'testbad'], 
                                      op_kwargs = {'df3': df3, 'df4': df4, 'df6': df6, 'mydict': {}})
    VEop
    
    EPop = SnowparkExternalPythonOperator(task_id='EPtask',
                                          schema='michaelgregory',
                                        #   python='/home/astro/.venv/snowpark/bin/python',
                                          python='/usr/local/bin/python3.8',
                                        #   python='/usr/local/bin/python3.9',
                                          python_callable=test_task2, 
                                          op_args = [df1, df2, 'testbad'], 
                                          op_kwargs = {'df3': df3, 'df4': df4, 'df6': df6, 'mydict': {}})
    EPop

    SPop = SnowparkPythonOperator(task_id='SPtask', 
                                  python_callable=test_task2, 
                                  snowflake_conn_id='snowflake_default',
                                  op_args = [df1, df2, 'testbad'], 
                                  op_kwargs = {'df3': df3, 'df4': df4, 'df6': df6, 'mydict': {}}
                                )
    SPop

    # @snowpark_python_task(task_id='SPdec')
    # def test_task3(df1:SnowparkTable, df2:SnowparkTable, str1:str, df6:SnowparkTable, mydict, df3:SnowparkTable, df4:SnowparkTable):

    #     snowpark_session.get_fully_qualified_current_schema()
        
    #     df1.show()
    #     df2.show()
    #     df3.show()
    #     df4.show()
    #     df6.show()
    #     mydict['mystr'] = str1

    #     return mydict['mystr']
    # SPdec = test_task3(df1=df1, df2=df2, str1='testbad', df6=df6, df3=df3, mydict={}, df4=df4)



snowpark_test_dag()
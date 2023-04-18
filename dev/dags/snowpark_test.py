import datetime
import io
import json

import pandas as pd
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from astronomer.providers.snowflake.operators.snowpark import BaseSnowparkOperator

# try:
#      from astro.sql.table import Table 
# except: 
from astronomer.providers.snowflake import SnowparkTable


# from snowpark_provider.decorators import snowpark_df_decorator
# from snowpark_provider.utils.table import Table

# to-do: declare global snowpark_session.add_packages(...)
PACKAGES = [
    "snowflake-snowpark-python",
    # "scikit-learn",
    # "pandas",
    # "numpy",
    # "joblib",
    # "cachetools",
]

@dag(
    default_args={"owner": "Airflow"},
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["example"],
)
def test_dag():

    def test_task(df1:SnowparkTable, df2:SnowparkTable, str1:str, df6:SnowparkTable, mydict, df3:SnowparkTable, df4:SnowparkTable):
        import snowflake.snowpark
        from snowflake.snowpark import functions as F
        from snowflake.snowpark import version as v
        from snowflake.snowpark.functions import col, sproc, udf
        
        snowpark_session.get_fully_qualified_current_schema()
        
        df1.show()
        df2.show()
        df3.show()
        df4.show()
        df6.show()
        mydict['mystr'] = str1

        return mydict['mystr']
    
    myop = BaseSnowparkOperator(task_id='mytask', 
                                python_callable=test_task, 
                                python_version='3.8',
                                requirements=PACKAGES,
                                op_args = tuple([SnowparkTable('STG_ORDERS', metadata={'database':'SANDBOX', 'schema':'michaelgregory'}), 
                                                SnowparkTable('sandbox.michaelgregory.stg_ad_spend'),
                                                'teststr'
                                                ]), 
                                op_kwargs = {'df3': SnowparkTable('STG_payments', metadata={'database':'SANDBOX'}), 
                                            'df4': SnowparkTable('stg_customers'),
                                            'df6': SnowparkTable('stg_sessions'),
                                            'mydict': {},
                                            })

    myop


test_dag()
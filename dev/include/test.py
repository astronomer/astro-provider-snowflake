
from astronomer.providers.snowflake import SnowparkTable
from astronomer.providers.snowflake.operators.snowpark import SnowparkVirtualenvOperator, SnowparkExternalPythonOperator
from astronomer.providers.snowflake.operators.snowpark import _BaseSnowparkOperator
from astronomer.providers.snowflake.decorators.snowpark import (
    snowpark_python_task,
    snowpark_virtualenv_task,
    snowpark_ext_python_task
)
from airflow.utils.dates import days_ago
from airflow.models import DAG
from include.tests import test_task
from pathlib import Path
import pickle
from airflow.utils.python_virtualenv import write_python_script
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from textwrap import dedent
import inspect

df1 = SnowparkTable('STG_ORDERS', metadata={'database':'SANDBOX', 'schema':'michaelgregory'})
df2 = SnowparkTable('sandbox.michaelgregory.stg_ad_spend')
df3 = SnowparkTable('STG_payments', metadata={'database':'SANDBOX'})
df4 = SnowparkTable('stg_customers')
df6 = SnowparkTable('stg_sessions')

_SNOWPARK_BIN = '/home/astro/.venv/snowpark/bin/python'


@snowpark_ext_python_task(python=_SNOWPARK_BIN)
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
self = test_task(df1=df1, df2=df2, str1='testbad', df6=df6, df3=df3, mydict={}, df4=df4)

self = SnowparkVirtualenvOperator(task_id='VEtask',
                                  conn_id = 'snowflake_mpg',
                                      python_callable=test_task,
                                      op_args = tuple([df1, df2, 'testbad']), 
                                      op_kwargs = {'df3': df3, 'df4': df4, 'df6': df6, 'mydict': {},})


self = SnowparkExternalPythonOperator(task_id='myeptask',
                                    #   python='/home/astro/.venv/snowpark/bin/python',
                                        python='/usr/local/bin/python3.8',
                                    #   python='/usr/local/bin/python3.9',
                                        python_callable=test_task,
                                        op_args = tuple([df1, df2, 'testbad']), 
                                        op_kwargs = {'df3': df3, 'df4': df4, 'df6': df6, 'mydict': {},})


print(self.get_python_source())
        
mydag=DAG(dag_id='testdag', start_date=days_ago(2))
mydag.add_task(self)

self._write_args(Path('junk'))
with open('junk', 'rb') as file:
    arg_dict = pickle.load(file)
type(arg_dict['args'][0])


write_python_script(
        jinja_context=dict(
            op_args=self.op_args,
            op_kwargs=self.op_kwargs,
            expect_airflow=self.expect_airflow,
            pickling_library=self.pickling_library.__name__,
            python_callable=self.python_callable.__name__,
            python_callable_source=self.get_python_source(),
        ),
        filename='junk',
        render_template_as_native_obj=self.dag.render_template_as_native_obj,
    )

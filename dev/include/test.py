from __future__ import annotations

try:
    from astro.sql.table import Table 
except:
    from astronomer.providers.snowflake import Table
    
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


# from attr import field

# class Metadata:
#     schema: str | None = None
#     database: str | None = None

# class Table():

#     name: str | None = None
#     __module__ = 'astronomer.providers.snowflake'
#     metadata: Metadata = field(
#         factory=Metadata,
#         converter=lambda val: Metadata(**val) if isinstance(val, dict) else val,
#     )
#     def __new__(cls, *args, **kwargs):
#         name = kwargs.get("name") or args and args[0] or ""
#         return cls


df1 = Table('STG_ORDERS', metadata={'database':'SANDBOX', 'schema':'michaelgregory'})
df2 = Table('sandbox.michaelgregory.stg_ad_spend')
df3 = Table('STG_payments', metadata={'database':'SANDBOX'})
df4 = Table('stg_customers')
df6 = Table('stg_sessions')

_SNOWPARK_BIN = '/home/astro/.venv/snowpark/bin/python'


# @snowpark_ext_python_task(python=_SNOWPARK_BIN)
# def test_task(df1:Table, df2:Table, str1:str, df6:Table, mydict, df3:Table, df4:Table):
#     import snowflake.snowpark
#     from snowflake.snowpark import functions as F
#     from snowflake.snowpark import version as v
#     from snowflake.snowpark.functions import col, sproc, udf
    
#     snowpark_session.get_fully_qualified_current_schema()
    
#     df1.show()
#     df2.show()
#     df3.show()
#     df4.show()
#     df6.show()
#     mydict['mystr'] = str1

#     return mydict['mystr']
# test_task(df1=df1, df2=df2, str1='testbad', df6=df6, df3=df3, mydict={}, df4=df4)

self = SnowparkVirtualenvOperator(task_id='VEtask',
                                  conn_id = 'snowflake_mpg',
                                      python_callable=test_task,
                                      op_args = tuple([df1, df2, 'testbad']), 
                                      op_kwargs = {'df3': df3, 'df4': df4, 'df6': df6, 'mydict': {},})
print(self.get_python_source())

# self = SnowparkExternalPythonOperator(task_id='myeptask',
#                                     #   python='/home/astro/.venv/snowpark/bin/python',
#                                         python='/usr/local/bin/python3.8',
#                                     #   python='/usr/local/bin/python3.9',
#                                         python_callable=test_task,
#                                         op_args = tuple([df1, df2, 'testbad']), 
#                                         op_kwargs = {'df3': df3, 'df4': df4, 'df6': df6, 'mydict': {},})


# print(self.get_python_source())
        
# mydag=DAG(dag_id='testdag', start_date=days_ago(2))
# mydag.add_task(self)

# self._write_args(Path('junk'))
# with open('junk', 'rb') as file:
#     arg_dict = pickle.load(file)
# type(arg_dict['args'][0])


# write_python_script(
#         jinja_context=dict(
#             op_args=self.op_args,
#             op_kwargs=self.op_kwargs,
#             expect_airflow=self.expect_airflow,
#             pickling_library=self.pickling_library.__name__,
#             python_callable=self.python_callable.__name__,
#             python_callable_source=self.get_python_source(),
#         ),
#         filename='junk',
#         render_template_as_native_obj=self.dag.render_template_as_native_obj,
#     )



from astro.sql.table import Table 
Table()
Table().metadata.database
Table
Table.__name__
Table.__module__

from astronomer.providers.snowflake import Table as SPTable
SPTable('x')
SPTable()
SPTable.__module__
SPTable.__name__




# def test():
#     # end first line with \ to avoid the empty line!
#     s = '''\
#     hello
#       world
#     '''
#     print(repr(s))          # prints '    hello\n      world\n    '
#     print(repr(dedent(s)))  # prints 'hello\n  world\n'



# my_callable_str="""@snowpark_ext_python_task(task_id="check_registry", python=_SNOWPARK_BIN )
# 	def check_registry(model_registry_database:str) -> str:
# 		from snowflake.ml.registry import model_registry

# 		registry = model_registry.ModelRegistry(session=snowpark_session, name=model_registry_database)
		
# 		return registry._name"""
# from textwrap import dedent
# dedent(my_callable_str).replace('\t', '    ')
from astronomer.providers.snowflake.hooks.snowpark_containers import SnowparkContainersHook
from astronomer.providers.snowflake.operators.snowpark_containers import SnowparkContainersPythonOperator
from astronomer.providers.snowflake.decorators.snowpark_containers import snowsparkcontainer_python_task

_SNOWFLAKE_CONN_ID='snowflake_default'

# hook = SnowparkContainersHook(_SNOWFLAKE_CONN_ID, session_parameters={'PYTHON_CONNECTOR_QUERY_RESULT_FORMAT': 'json'})

#import requests
# urls, headers = hook.get_service_urls(service_name='runner')
# assert '"pong"' == requests.get(url=urls['runner'], headers=headers).text
# assert '{"detail":"Not Found"}' == requests.get(url=rurls['runner']+'/task', headers=headers).text

#import weaviate, os
# weaviate_headers={"X-OpenAI-Api-Key": os.environ['OPENAI_APIKEY']}
# dns_name = hook.list_services('weaviate')['WEAVIATE']['dns_name']
# weaviate_conn = {'endpoint': f'http://{dns_name}:8081', 'headers': weaviate_headers}
# from include.myfunc import test_weaviate
# tw = SnowparkContainersPythonOperator(task_id='test_weaviate', 
#                                         endpoint=runner_conn['endpoint'], 
#                                         headers=runner_conn['headers'],
#                                         python_callable=test_weaviate, 
#                                         database='sissyg',
#                                         schema='demo',
#                                         # temp_data_output = 'table',
#                                         # temp_data_stage = 'xcom_stage',
#                                         snowflake_conn_id=_SNOWFLAKE_CONN_ID,
#                                         # op_args = [weaviate_conn], 
#                                         op_kwargs = {'weaviate_conn': weaviate_conn}
#                                         )
# tw.payload=tw._build_payload(context={'ts_nodash':"20180101T000000", 'run_id': 'testrun'})
# tw.execute_callable()

runner_service_name = 'runner'
runner_conn = {'endpoint': None, 'headers': None}
# runner_service_name = None
# runner_conn = {'endpoint': 'http://host.docker.internal:8001/task', 'headers': {}}

from astronomer.providers.snowflake import SnowparkTable
df1 = SnowparkTable('STG_ORDERS', metadata={'database':'sissyg', 'schema':'demo'}, conn_id='snowflake_default')
df2 = SnowparkTable('sissyg.demo.stg_ad_spend', conn_id='snowflake_default')
df3 = SnowparkTable('STG_payments')
df4 = SnowparkTable('stg_customers', metadata={'database':'sissyg'})
df6 = SnowparkTable('stg_sessions', conn_id='snowflake_user')

from include.myfunc import test_task3
SPop = SnowparkContainersPythonOperator(task_id='SPtask', 
                                        runner_service_name=runner_service_name,
                                        endpoint=runner_conn['endpoint'], 
                                        headers=runner_conn['headers'],
                                        python_callable=test_task3, 
                                        # database='sandbox',
                                        # schema='michaelgregory',
                                        # temp_data_output = 'table',
                                        # temp_data_stage = 'xcom_stage',
                                        snowflake_conn_id='snowflake_default',
                                        op_args = [df1, df2, 'testbad'], 
                                        op_kwargs = {'df6': df6, 'mydict': {'x':1, 'y':2}, 'df3': df3, 'df4': df4}
                                        )
SPop.payload=SPop._build_payload(context={'ts_nodash':"20180101T000000", 'run_id': 'testrun'})
SPop.endpoint
SPop.headers


SPop.execute_callable()
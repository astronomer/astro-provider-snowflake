from pendulum import datetime
import os
from airflow.decorators import dag, task

from astronomer.providers.snowflake.operators.snowpark_containers import SnowparkContainersPythonOperator
from astronomer.providers.snowflake.decorators.snowpark_containers import snowsparkcontainer_python_task
from astronomer.providers.snowflake.hooks.snowpark_containers import SnowparkContainersHook


@dag(
    start_date=datetime(2023, 1, 1),
    schedule=None,
    default_args={"retries": 2, "snowflake_conn_id": "snowflake_default"},
    tags=["example"],
)
def sample_workflow():
    """
    ### Sample DAG

    Showcases the snowpark container services provider package's operator and decorator.

    To run this example, create a Snowflake connection with:
    - id: snowflake_default
    - type: snowflake
    - username: <USER>
    - password: <PW>
    - schema: <SCHEMA>
    - account: <ACCOUNT> 
    - region: <REGION> 
    - database: <DB> 
    - warehouse: <WH> 
    - role: <ROLE>
    
    or via environment variable
"""
    os.environ['AIRFLOW_CONN_SNOWFLAKE_DEFAULT']='{"conn_type": "snowflake", "login": "USER_NAME", "password": "PASSWORD", "schema": "SCHEMA_NAME", "extra": {"account": "ACCOUNT_NAME", "warehouse": "WAREHOUSE_NAME", "database": "DATABASE_NAME", "region": "REGION_NAME", "role": "MICHAELGREGORY", "authenticator": "snowflake", "session_parameters": null, "application": "AIRFLOW"}}'

    pool_name = 'mypool'
    service_name = 'mysvc'
    runner_image = 'snowpark-container-runner:latest'
    service_url = 'host.docker.internal:8001'
    runner_endpoint = f'ws://{service_url}/task'
    _SNOWFLAKE_CONN_ID='snowflake_default'

    @task()
    def setup_snowpark_container_service(pool_name:str, service_name:str, runner_image:str) -> str:

        hook = SnowparkContainersHook(conn_id = _SNOWFLAKE_CONN_ID, local_test='astro_cli')

        pool_name = hook.create_pool(pool_name=pool_name, instance_family='standard_1', min_nodes=1, max_nodes=2, replace_existing=True)

        _ = hook.create_service(service_name=service_name, pool_name=pool_name, runner_image=runner_image, runner_endpoint='runner')

        _ = hook.suspend_service(service_name=service_name)
        
        return service_name
        
    @task()
    def start_snowpark_container_service(service_name) -> str:

        hook = SnowparkContainersHook(conn_id = _SNOWFLAKE_CONN_ID)

        response = hook.resume_service(service_name=service_name)

        assert response, f'Could not resume service {service_name}'
        

    def myfunc1():
        import pandas as pd
        df = pd.DataFrame([{"a": 1, "b": 1}, {"a": 1, "b": 1}, {"a": 1, "b": 1}])
        a=1
        b=1
        print('stuff')
        return df.to_json()

    def myfunc2(json_input: dict):
        import pandas as pd
        df = pd.DataFrame(json_input)
        a=2
        b=2
        print('more stuff from func2')
        return df #.to_json()
    
    #works
    sspo1 = SnowparkContainersPythonOperator(task_id='sspo1', runner_endpoint=runner_endpoint, python_callable=myfunc1)
    sspo2 = SnowparkContainersPythonOperator(task_id='sspo2', runner_endpoint=runner_endpoint, python_callable=myfunc2, op_kwargs={'json_input': {'a': {0: 4, 1: 4, 2: 4}, 'b': {0: 4, 1: 4, 2: 4}}})
    # sspo1 >> sspo2

    #works
    @snowsparkcontainer_python_task(runner_endpoint=runner_endpoint, snowflake_conn_id=_SNOWFLAKE_CONN_ID)
    def myfunc5():
        import pandas as pd
        from snowflake.snowpark import Session
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        snowflake_conn_params = SnowflakeHook(conn_id=_SNOWFLAKE_CONN_ID)._get_conn_params()
        session = Session.builder.configs(snowflake_conn_params).create()

        snowdf = session.create_dataframe([{"a": 4, "b": 4}, {"a": 4, "b": 4}, {"a": 4, "b": 4}])
        return snowdf.to_pandas()
    
    myfunc5()

    #works
    @task.virtualenv()
    def myfunc3():
        import pandas as pd
        df = pd.DataFrame([{"a": 3, "b": 3}, {"a": 3, "b": 3}, {"a": 3, "b": 3}])
        a=3
        b=3
        print('stuff from func3')
        return df.to_json()

    @task.virtualenv()
    def myfunc4():
        import pandas as pd
        df = pd.DataFrame([{"a": 4, "b": 4}, {"a": 4, "b": 4}, {"a": 4, "b": 4}])
        a=4
        b=4
        print('more stuff from func4')
        return df.to_json()

    # myfunc3()
    # json_output = myfunc4()

    @snowsparkcontainer_python_task(runner_endpoint=runner_endpoint, python='3.9')
    def myfunc6(json_input: dict):
        import pandas as pd
        df = pd.read_json(json_input)
        a=7
        b=5
        print('more stuff from func2')
        return df.to_json()

    # myfunc6(json_input=json_output)

    @snowsparkcontainer_python_task(runner_endpoint=runner_endpoint, requirements=['openai', 'weaviate-client'], snowflake_conn_id=_SNOWFLAKE_CONN_ID)
    def myfunc7():
        import pandas as pd
        import openai
        df = pd.DataFrame([{"a": 3, "b": 3}, {"a": 3, "b": 3}, {"a": 3, "b": 3}])
        a=3
        b=3
        print('stuff from func3')
        return df.to_json()
    
    myfunc7()


sample_workflow()

from __future__ import annotations

from urllib import parse as parser
import os
from typing import TYPE_CHECKING, Any

from airflow.exceptions import AirflowException
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

def _try_parse_snowflake_xcom_uri(value:str) -> Any:
    try:
        parsed_uri = parser.urlparse(value)
        if parsed_uri.scheme != 'snowflake':
            return False

        netloc = parsed_uri.netloc

        if len(netloc.split('.')) == 2:
            account, region = netloc.split('.')
        else:
            account = netloc
            region = None           
    
        uri_query = parsed_uri.query.split('&')

        if uri_query[1].split('=')[0] == 'table':
            xcom_table = uri_query[1].split('=')[1]
            xcom_stage = None
        elif uri_query[1].split('=')[0] == 'stage':
            xcom_stage = uri_query[1].split('=')[1]
            xcom_table = None
        else:
            return False
        
        xcom_key = uri_query[2].split('=')[1]

        return {
            'account': account,
            'region': region,
            'xcom_table': xcom_table, 
            'xcom_stage': xcom_stage,
            'xcom_key': xcom_key,
        }

    except:
        return False 

def get_snowflake_xcom_objects() -> dict:
    try: 
        snowflake_conn_id = os.environ['AIRFLOW__CORE__XCOM_SNOWFLAKE_CONN_NAME']
    except:
        raise AirflowException("AIRFLOW__CORE__XCOM_SNOWFLAKE_CONN_NAME environment variable not set.")
    
    try: 
        snowflake_xcom_table = os.environ['AIRFLOW__CORE__XCOM_SNOWFLAKE_TABLE']
    except: 
        raise AirflowException('AIRFLOW__CORE__XCOM_SNOWFLAKE_TABLE environment variable not set')
    
    assert len(snowflake_xcom_table.split('.')) == 3, "AIRFLOW__CORE__XCOM_SNOWFLAKE_TABLE is not a fully-qualified Snowflake table objet"
    
    try: 
        snowflake_xcom_stage = os.environ['AIRFLOW__CORE__XCOM_SNOWFLAKE_STAGE']
    except: 
        raise AirflowException('AIRFLOW__CORE__XCOM_SNOWFLAKE_STAGE environment variable not set')
    
    assert len(snowflake_xcom_stage.split('.')) == 3, "AIRFLOW__CORE__XCOM_SNOWFLAKE_STAGE is not a fully-qualified Snowflake stage objet"

    return {'conn_id': snowflake_conn_id, 'table': snowflake_xcom_table, 'stage': snowflake_xcom_stage}

def check_xcom_conn(snowflake_conn_id:str) -> str:
    
    response = SnowflakeHook(snowflake_conn_id=snowflake_conn_id).test_connection()
    assert response[0] == True, f"Snowflake XCOM connection {snowflake_conn_id} error. {response[1]}"
    
    return True

def check_xcom_table(snowflake_conn_id:str, snowflake_xcom_table:str) -> str:
    expected_schema = [
        ('DAG_ID', 'VARCHAR(16777216)', 'COLUMN', 'N', None, 'N', 'N', None, None, None, None), 
        ('TASK_ID', 'VARCHAR(16777216)', 'COLUMN', 'N', None, 'N', 'N', None, None, None, None), 
        ('RUN_ID', 'VARCHAR(16777216)', 'COLUMN', 'N', None, 'N', 'N', None, None, None, None), 
        ('MULTI_INDEX', 'NUMBER(38,0)', 'COLUMN', 'N', None, 'N', 'N', None, None, None, None), 
        ('KEY', 'VARCHAR(16777216)', 'COLUMN', 'N', None, 'N', 'N', None, None, None, None), 
        ('VALUE_TYPE', 'VARCHAR(16777216)', 'COLUMN', 'N', None, 'N', 'N', None, None, None, None), 
        ('VALUE', 'VARCHAR(16777216)', 'COLUMN', 'N', None, 'N', 'N', None, None, None, None)
    ]

    xcom_table_schema = SnowflakeHook(snowflake_conn_id=snowflake_conn_id).get_records(f'DESCRIBE TABLE {snowflake_xcom_table}')

    assert xcom_table_schema == expected_schema, \
        f"""
                XCOM table {snowflake_xcom_table} does not have the correct schema. 
                Please create it with:

                SnowflakeHook().run(\'\'\'CREATE TABLE {snowflake_xcom_table} 
                    ( 
                        dag_id varchar NOT NULL, 
                        task_id varchar NOT NULL, 
                        run_id varchar NOT NULL,
                        multi_index integer NOT NULL,
                        key varchar NOT NULL,
                        value_type varchar NOT NULL,
                        value varchar NOT NULL
                    )\'\'\')
                """
    return True

def check_xcom_stage(snowflake_conn_id:str, snowflake_xcom_stage:str) -> str:
    
    try:
        SnowflakeHook(snowflake_conn_id=snowflake_conn_id).get_records(f'DESCRIBE STAGE {snowflake_xcom_stage}')
    except Exception as e:
        if 'does not exist or not authorized' in e.msg:
            raise AirflowException(
                f'''
                XCOM stage {snowflake_xcom_stage} does not exist or not authorized. 
                Please create it with:

                SnowflakeHook().run('CREATE STAGE {snowflake_xcom_stage}')
                '''
            )
        else:
            raise e
        
    return True

def check_xcom_backend():

    snowflake_xcom_objects = get_snowflake_xcom_objects()
    
    assert check_xcom_conn(snowflake_conn_id=snowflake_xcom_objects['conn_id']) 

    assert check_xcom_table(
        snowflake_conn_id=snowflake_xcom_objects['conn_id'], 
        snowflake_xcom_table=snowflake_xcom_objects['table']
        )

    assert check_xcom_stage(
        snowflake_conn_id=snowflake_xcom_objects['conn_id'], 
        snowflake_xcom_stage=snowflake_xcom_objects['stage']
        )

    return True

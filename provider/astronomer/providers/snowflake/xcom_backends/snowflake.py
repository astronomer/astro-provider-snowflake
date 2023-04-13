from __future__ import annotations

import tempfile
from pathlib import Path
from urllib import parse as parser
import os
import json
from typing import TYPE_CHECKING, Any
import pandas as pd
import numpy as np
from ast import literal_eval

from airflow.models.xcom import BaseXCom
if TYPE_CHECKING:
    from airflow.models.xcom import XCom

from airflow.exceptions import AirflowException
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from sqlalchemy.orm import Session
from airflow.utils.session import NEW_SESSION
from astro.files import File
from astro.table import Table, TempTable, Metadata
from airflow.models.dataset import Dataset

    
_ENCODING = "utf-8"
_SNOWFLAKE_VARIANT_SIZE_LIMIT = 16777216
_SUPPORTED_FILE_TYPES = ['json', 'parquet', 'np', 'bin', 'txt']

def _try_parse_snowflake_xcom_uri(value:str) -> Any:
    try:
        parsed_uri = parser.urlparse(value)
        assert parsed_uri.scheme == 'snowflake'

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

class SnowflakeXComBackend(BaseXCom):
    """
    Custom XCom backend that stores XComs in the Snowflake. JSON serializable objects 
    are stored as tables.  Dataframes (pandas and Snowpark) are stored as parquet files in a stage.
    Requires a specified xcom directory.
    Enable with these env variables:
        AIRFLOW__CORE__XCOM_BACKEND=astronomer.providers.snowflake.xcom_backends.snowflake.SnowflakeXComBackend
        AIRFLOW__CORE__XCOM_SNOWFLAKE_TABLE=<DB.SCHEMA.TABLE>
        AIRFLOW__CORE__XCOM_SNOWFLAKE_STAGE=<DB.SCHEMA.STAGE>
        AIRFLOW__CORE__XCOM_SNOWFLAKE_CONN_NAME=<CONN_NAME> ie. 'snowflake_default'
    
    The XCOM table can be created with the following:

        SnowflakeHook().run(\'\'\'CREATE TABLE <TABLE_NAME>
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

    @staticmethod
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
    
    @staticmethod
    def check_xcom_conn(snowflake_conn_id:str) -> str:
        
        response = SnowflakeHook(snowflake_conn_id=snowflake_conn_id).test_connection()
        if response[0] == False:
            raise AirflowException(f"Snowflake XCOM connection {snowflake_conn_id} error. {response[1]}")
        
        return 'success'
    
    @staticmethod
    def check_xcom_table(snowflake_conn_id:str, snowflake_xcom_table:str) -> str:
                
        try:
            SnowflakeHook(snowflake_conn_id=snowflake_conn_id).run(f'DESCRIBE TABLE {snowflake_xcom_table}')
        except Exception as e:
            if 'does not exist or not authorized' in e.msg:
                raise AirflowException(
                    f'''
                    XCOM table {snowflake_xcom_table} does not exist or not authorized. 
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
                    '''
                )
            else:
                raise e
                
        return 'success'

    @staticmethod
    def check_xcom_stage(snowflake_conn_id:str, snowflake_xcom_stage:str) -> str:
       
        try:
            SnowflakeHook(snowflake_conn_id=snowflake_conn_id).run(f'DESCRIBE STAGE {snowflake_xcom_stage}')
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
            
        return 'success'

    @staticmethod
    def check_xcom_backend(self):

        snowflake_xcom_objects = SnowflakeXComBackend.get_snowflake_xcom_objects()
        
        response = SnowflakeXComBackend.check_xcom_conn(snowflake_conn_id=snowflake_xcom_objects['conn_id']) 
        assert response == 'success'

        response = SnowflakeXComBackend.check_xcom_table(
            snowflake_conn_id=snowflake_xcom_objects['conn_id'], 
            snowflake_xcom_table=snowflake_xcom_objects['table']
            )
        assert response == 'success'

        response = SnowflakeXComBackend.check_xcom_stage(
            snowflake_conn_id=snowflake_xcom_objects['conn_id'], 
            snowflake_xcom_stage=snowflake_xcom_objects['stage']
            )
        assert response == 'success'

        return 'success'
    
    @staticmethod
    def _serialize(
            value: Any, 
            key: str, 
            dag_id: str, 
            task_id: str, 
            run_id: str,
            multi_index: int,
        ) -> str:
        
        #Other downstream systems such as Snowservice runners may have serialized to Snowflake already.
        #Check for a valid xcom URI in return value and pass it through.
        if _try_parse_snowflake_xcom_uri(value):
            return value
        
        snowflake_xcom_objects = SnowflakeXComBackend.get_snowflake_xcom_objects()
        snowflake_conn_id = snowflake_xcom_objects['conn_id']
        snowflake_xcom_table = snowflake_xcom_objects['table']
        snowflake_xcom_stage = snowflake_xcom_objects['stage']

        hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

        conn_params = hook._get_conn_params()

        if conn_params['region']:
            base_uri = f"snowflake://{conn_params['account']}.{conn_params['region']}?"
        else:
            base_uri = f"snowflake://{conn_params['account']}?"

        if isinstance(value, str):
            json_str = value
            json_serializable = True
            value_type = 'str'
        elif isinstance(value, File):
            json_str = json.dumps(value.to_json())
            json_serializable = True
            value_type = 'aql_File'
        elif isinstance(value, (Table, TempTable)):
            json_str = json.dumps(value.to_json())
            json_serializable = True
            value_type = 'aql_Table'
        elif isinstance(value, Dataset):
            json_str = json.dumps({'uri': value.uri, 'extra': value.extra})
            json_serializable = True
            value_type = 'airflow_Dataset'
        else:
            try:
                #check serializability
                json_str = json.dumps(value)
                json_serializable = True
                value_type = 'json'
                
                if isinstance(value, dict) and set(map(type, value)) != {str}:
                        print('Non-string-type keys found in dict. Resorting to file serialization to stage.')
                        json_serializable = False

            except Exception as e:
                if isinstance(e, TypeError) and 'not JSON serializable' in e.args[0]:
                    print('Attempting to serialize XCOM value but it is not JSON serializable.  Resorting to file serialization to stage.')
                    json_serializable = False
                else:
                    raise e()

        if 'json_str' in locals() and len(json_str.encode(_ENCODING)) > _SNOWFLAKE_VARIANT_SIZE_LIMIT:
            print(f'XCOM value size exceeds Snowflake cell size limit {_SNOWFLAKE_VARIANT_SIZE_LIMIT}. Resorting to file serialization to stage.')
            json_serializable = False
                    
        if json_serializable:
             
            #upcert
            hook.run(f"""
                MERGE INTO {snowflake_xcom_table} tab1
                USING (SELECT
                            '{dag_id}' AS dag_id, 
                            '{task_id}' AS task_id, 
                            '{run_id}' AS run_id, 
                            '{multi_index}' AS multi_index,
                            '{key}' AS key,  
                            '{value_type}' AS value_type,
                            '{json_str}' AS value) tab2
                ON tab1.dag_id = tab2.dag_id 
                    AND tab1.task_id = tab2.task_id
                    AND tab1.run_id = tab2.run_id
                    AND tab1.multi_index = tab2.multi_index
                    AND tab1.key = tab2.key
                WHEN MATCHED THEN UPDATE SET tab1.value = tab2.value, tab1.value_type = tab2.value_type
                WHEN NOT MATCHED THEN INSERT (dag_id, task_id, run_id, multi_index, key, value_type, value)
                        VALUES (tab2.dag_id, tab2.task_id, tab2.run_id, tab2.multi_index, tab2.key, tab2.value_type, tab2.value);
            """)

            uri = base_uri + f"&table={snowflake_xcom_table}&key={dag_id}/{task_id}/{run_id}/{multi_index}/{key}"

            return uri
            
        else:
            with tempfile.TemporaryDirectory() as td:
                if isinstance(value, str):
                    temp_file = Path(f'{td}/{key}.txt')
                    _ = temp_file.write_text(value, encoding=_ENCODING)

                elif 'json_str' in locals():
                    temp_file = Path(f'{td}/{key}.json')
                    _ = temp_file.write_text(str(value), encoding=_ENCODING)
                
                elif isinstance(value, (pd.DataFrame, pd.core.series.Series)):
                    temp_file = Path(f'{td}/{key}.parquet')
                    pd.DataFrame(value).to_parquet(temp_file)

                elif isinstance(value, np.ndarray):
                    temp_file = Path(f'{td}/{key}.np')
                    temp_file.write_bytes(value.dumps())

                elif isinstance(value, bytes):
                        temp_file = Path(f'{td}/{key}.bin')
                        _ = temp_file.write_bytes(value)
                else:
                        raise AttributeError(f'Could not serialize object of type {type(value)}')
                        
                hook.run(f"""
                    PUT file://{temp_file} @{snowflake_xcom_stage}/{dag_id}/{task_id}/{run_id}/{multi_index}/ 
                    AUTO_COMPRESS = FALSE 
                    SOURCE_COMPRESSION = NONE 
                    OVERWRITE = TRUE
                """)

                uri = f"{base_uri}&stage={snowflake_xcom_stage}&key={dag_id}/{task_id}/{run_id}/{multi_index}/{temp_file.name}"

                return uri

    @staticmethod
    def _deserialize(uri: str) -> Any:

        snowflake_xcom_objects = SnowflakeXComBackend.get_snowflake_xcom_objects()
        snowflake_conn_id = snowflake_xcom_objects['conn_id']
        snowflake_xcom_table = snowflake_xcom_objects['table']
        snowflake_xcom_stage = snowflake_xcom_objects['stage']

        parsed_uri = _try_parse_snowflake_xcom_uri(uri)

        if parsed_uri:

            hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
            hook.account = parsed_uri['account']
            hook.region = parsed_uri['region']

            if parsed_uri['xcom_stage']:
                assert parsed_uri['xcom_stage'][0] == snowflake_xcom_stage, f"Provided stage {parsed_uri['xcom_stage'][0]} is different from system XCOM stage {snowflake_xcom_stage}."

                with tempfile.TemporaryDirectory() as td:
                    hook.run(f"GET @{snowflake_xcom_stage}/{parsed_uri['xcom_key']} file://{td}")

                    temp_file = Path(td).joinpath(Path(parsed_uri['xcom_key']).name)
                    temp_file_type = temp_file.as_posix().split('.')[-1]

                    assert temp_file_type in _SUPPORTED_FILE_TYPES, f'XCOM file type {temp_file_type} is not supported.'

                    if temp_file_type == 'parquet':
                        return pd.read_parquet(temp_file)
                    elif temp_file_type == 'np':
                        return np.load(temp_file, allow_pickle=True)
                    elif temp_file_type == 'json':
                        return literal_eval(temp_file.read_text())
                    elif temp_file_type == 'bin':
                        return temp_file.read_bytes()
                    elif temp_file_type == 'txt':
                        return temp_file.read_text()
                
            elif parsed_uri['xcom_table']:

                assert parsed_uri['xcom_table'] == snowflake_xcom_table, f"Provided table {parsed_uri['xcom_table']} is different from system XCOM table {snowflake_xcom_table}."

                xcom_cols = parsed_uri['xcom_key'].split('/')

                ret_value = hook.get_records(f""" 
                                                SELECT VALUE_TYPE, VALUE FROM {parsed_uri['xcom_table']} 
                                                WHERE dag_id = '{xcom_cols[0]}'
                                                AND task_id = '{xcom_cols[1]}'
                                                AND run_id = '{xcom_cols[2]}'
                                                AND multi_index = '{xcom_cols[3]}'
                                                AND key = '{xcom_cols[4]}'
                                            ;""")[0]
                
                if ret_value[0] == 'str':
                    return ret_value[1]
                elif ret_value[0] == 'json':
                    return json.loads(ret_value[1])
                elif ret_value[0] == 'aql_File':
                    return File.from_json(ret_value[1])
                elif ret_value[0] == 'aql_Table':
                    return Table.from_json(ret_value[1])
                elif ret_value[0] == 'airflow_Dataset':
                    dataset_dict = json.loads(ret_value[1])
                    return Dataset(uri=dataset_dict['uri'], extra=dataset_dict['extra'])
                else:
                    raise AttributeError(f'Unsupported return value type {ret_value[0]}.')
        else: 
            raise AttributeError('Failed to parse XCOM URI.')

    @staticmethod
    def serialize_value(
        key: str, 
        value: Any, 
        task_id: str,
        dag_id: str, 
        run_id: str, 
        map_index: int = -1,
        **kwargs
    ) -> list:
        """
        Custom Xcom Wrapper to serialize to Snowflake stage/table and returns the URI to Xcom.

        Writes JSON serializable content as a column in AIRFLOW__CORE__XCOM_SNOWFLAKE_TABLE and 
        writes non-serializable content as files in AIRFLOW__CORE__XCOM_SNOWFLAKE_STAGE. 

        returns a URI for the table or file to be serialized in the Airflow XCOM DB. 

        The URI format is: 
            snowflake://<ACCOUNT>.<REGION>/<DATABASE>/<SCHEMA>?table=<TABLE>&key=<KEY>
            or
            snowflake://<ACCOUNT>.<REGION>/<DATABASE>/<SCHEMA>?stage=<STAGE>&key=<FILE_PATH>
        
        :param value: The value to serialize.
        :type value: Any
        :param key: The key to use for the xcom output (ie. filename)
        :type: key: str
        :param dag_id: DAG id
        :type dag_id: str
        :param task_id: Task id
        :type task_id: str
        :param run_id: DAG run id
        :type run_id: str
        :param map_index: IGNORED
        :type map_index: int
        :return: The byte encoded uri string.
        :rtype: bytes

        """

        #Some tasks may return multiple values in a tuple.  We will keep track of the number of return values via an index.
        # len(return_uris) should equal multi_index

        multi_index = -1
        return_uris = []

        if isinstance(value, (list, tuple)):
            for item in value:
                multi_index+=1

                if item is None:
                    return_uris.append(None)
                else:
                    uri: str = SnowflakeXComBackend._serialize(
                        value=item, 
                        key=key, 
                        dag_id=dag_id, 
                        task_id=task_id, 
                        run_id=run_id, 
                        multi_index=multi_index
                    )
                    return_uris.append(uri)
        else:
            multi_index+=1
            if value is None:
                return_uris.append(None)
            else:                
                uri: str = SnowflakeXComBackend._serialize(
                    value=value, 
                    key=key, 
                    dag_id=dag_id, 
                    task_id=task_id, 
                    run_id=run_id, 
                    multi_index=multi_index
                )
                return_uris.append(uri)
        
        assert len(return_uris) == multi_index+1

        return BaseXCom.serialize_value(return_uris)

    @staticmethod
    def deserialize_value(result: "XCom") -> Any:
        """
        Deserialize the result of xcom_pull before passing the result to the next task.

        Reads the value from Snowflake XCOM backend given a list of URIs.
        
        The URI format is: 
            snowflake://<ACCOUNT>.<REGION>/<DATABASE>/<SCHEMA>?table=<TABLE>&key=<KEY>
            snowflake://<ACCOUNT>/<DATABASE>/<SCHEMA>?table=<TABLE>&key=<KEY>
            or
            snowflake://<ACCOUNT>.<REGION>/<DATABASE>/<SCHEMA>?stage=<STAGE>&key=<FILE_PATH>
            snowflake://<ACCOUNT>/<DATABASE>/<SCHEMA>?stage=<STAGE>&key=<FILE_PATH>
        
        :param uri: The XCOM uri.
        :type uri: str
        :return: The deserialized value.
        :rtype: Any
        """

        uris = BaseXCom.deserialize_value(result)

        return_results = []
        for uri in uris:
            if uri is None:
                return_results.append(None)
            else:
                return_results.append(SnowflakeXComBackend._deserialize(uri))

        if len(return_results) == 1:
            return return_results[0]
        else:
            return tuple(return_results)
    
from __future__ import annotations

from typing import Any
from urllib import parse as parser

from snowpark_provider import SnowparkTable
from snowflake.snowpark.session import Session as SnowparkSession
from snowflake.snowpark.dataframe import DataFrame as SnowparkDataFrame

def _is_table_arg(arg:Any):
    if isinstance(arg, SnowparkTable):
        arg=arg.to_json()

    if isinstance(arg, dict) and arg.get("class", "") in ["SnowparkTable"]:

        if _try_parse_snowflake_xcom_uri(arg.get('uri', '')):
            return arg['uri']
        elif len(arg['name'].split('.')) == 3:
            return arg['name']
        elif len(arg['name'].split('.')) == 1:
            database = arg['metadata'].get('database') 
            schema = arg['metadata'].get('schema') 

            if database and schema:
                return f"{database}.{schema}.{arg['name']}"
            else:
                return arg['name']
        else:
            raise Exception("SnowparkTable name must be fully-qualified or tablename only.")
    else:
        return False

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

def _deserialize_snowpark_args(arg:Any, snowpark_session:SnowparkSession, conn_params:dict):

    table_name = _is_table_arg(arg)
    uri = _try_parse_snowflake_xcom_uri(arg)

    #if its a table arg table_name can be FQ table name or a URI to staged file or table
    if table_name:
        uri = _try_parse_snowflake_xcom_uri(table_name)
        if not uri:
            return snowpark_session.table(table_name)
    
    if uri: 
        if uri['xcom_stage']:
            file_extension = uri['xcom_key'].split('.')[-1]
            if file_extension == 'parquet': 
                return snowpark_session.read.parquet(f"@{uri['xcom_stage']}/{uri['xcom_key']}")
            else: 
                raise Exception(f"Cannot parse SnowparkTable URI with extension {file_extension}.  Serialized data should be in parquet format with '.parquet' extension.")
        elif uri['xcom_table'] and uri['xcom_key'] == '*':
            return snowpark_session.table(uri['xcom_table'])
        else:
            raise Exception(f"Failed to parse SnowparkTable URI.")
    
    elif isinstance(arg, dict):
        return {k: _deserialize_snowpark_args(v, snowpark_session, conn_params) for k, v in arg.items()}
    elif isinstance(arg, (list, tuple)):
        return arg.__class__(_deserialize_snowpark_args(item, snowpark_session, conn_params) for item in arg)
    else:
        return arg

def _write_snowpark_dataframe(spdf:SnowparkDataFrame, 
                              snowpark_session:SnowparkSession, 
                              temp_data_dict:dict, 
                              conn_params:dict, 
                              dag_id:str, 
                              task_id:str, 
                              run_id:str, 
                              ts_nodash:str, 
                              multi_index:int):
    try:
        database = temp_data_dict.get('temp_data_db') or snowpark_session.get_current_database().replace("\"","")
        schema = temp_data_dict.get('temp_data_schema') or snowpark_session.get_current_schema().replace("\"","")
    except: 
        assert database and schema, "To serialize Snowpark dataframes the database and schema must be set in temp_data params, operator/decorator, hook or Snowflake user session defaults."
    
    if conn_params['region']:
        base_uri = f"snowflake://{conn_params['account']}.{conn_params['region']}?"
    else:
        base_uri = f"snowflake://{conn_params['account']}?"


    if temp_data_dict['temp_data_output'] == 'stage':
        """
        Save to stage <DATABASE>.<SCHEMA>.<STAGE>/<DAG_ID>/<TASK_ID>/<RUN_ID> 
        and return a SnowparkTable object with uri
        snowflake://<ACCOUNT>.<REGION>?&stage=<FQ_STAGE>&key=<DAG_ID>/<TASK_ID>/<RUN_ID>/0/return_value.parquet'
        """

        stage_name = f"{temp_data_dict['temp_data_stage']}".upper()
        fq_stage_name = f"{database}.{schema}.{stage_name}".upper()
        assert len(fq_stage_name.split('.')) == 3, "stage for snowpark dataframe serialization is not fully-qualified"
        
        uri = f"{base_uri}&stage={fq_stage_name}&key={dag_id}/{task_id}/{run_id}/{multi_index}/return_value.parquet"

        spdf.write.copy_into_location(file_format_type="parquet",
                                    overwrite=temp_data_dict['temp_data_overwrite'],
                                    header=True, 
                                    single=True,
                                    location=f"{fq_stage_name}/{dag_id}/{task_id}/{run_id}/{multi_index}/return_value.parquet")

        return SnowparkTable(name='__file__', uri=uri, metadata={'schema': schema, 'database': database}).to_json()

    elif temp_data_dict['temp_data_output'] == 'table':
        """
        Save to table <DATABASE>.<SCHEMA>.<PREFIX><DAG_ID>__<TASK_ID>__<TS_NODASH>_INDEX
        and return SnowparkTable object with uri
        SnowparkTable(name=<DATABASE>.<SCHEMA>.<PREFIX><DAG_ID>__<TASK_ID>__<TS_NODASH>_INDEX)
        snowflake://<ACCOUNT>.<REGION>?&table=<DATABASE>.<SCHEMA>.<PREFIX><DAG_ID>__<TASK_ID>__<TS_NODASH>_INDEX&key=*
        """
        table_name = f"{temp_data_dict['temp_data_table_prefix'] or ''}{dag_id}__{task_id.replace('.','_')}__{ts_nodash}__{multi_index}".upper()
        fq_table_name = f"{database}.{schema}.{table_name}".upper()
        assert len(fq_table_name.split('.')) == 3, "table for snowpark dataframe serialization is not fully-qualified"

        if temp_data_dict['temp_data_overwrite']:
            mode = 'overwrite'
        else:
            mode = 'errorifexists'

        spdf.write.save_as_table(fq_table_name, mode=mode)

        uri = f"{base_uri}&table={fq_table_name}&key=*"

        return SnowparkTable(name=table_name, uri=uri, metadata={'schema': schema, 'database': database}).to_json()
    else:
        raise Exception("temp_data_output must be one of 'stage' | 'table' | None")

def _serialize_snowpark_results(res:Any, 
                                snowpark_session:SnowparkSession, 
                                temp_data_dict:dict, 
                                conn_params:dict, 
                                dag_id:str, 
                                task_id:str, 
                                run_id:str, 
                                ts_nodash:str, 
                                multi_index:int):
    
    from snowflake.snowpark import DataFrame as Snowpark_DataFrame

    if temp_data_dict.get('temp_data_output') in ['stage', 'table']:
        
        if isinstance(res, Snowpark_DataFrame): 
            multi_index+=1
            return _write_snowpark_dataframe(res, 
                                             snowpark_session, 
                                             temp_data_dict, 
                                             conn_params, dag_id, 
                                             task_id, 
                                             run_id, 
                                             ts_nodash, 
                                             multi_index), multi_index 
        elif isinstance(res, dict):
            tmp={}
            for k, v in res.items():
                tmp[k], multi_index = _serialize_snowpark_results(v, 
                                                                  snowpark_session, 
                                                                  temp_data_dict, 
                                                                  conn_params, 
                                                                  dag_id, 
                                                                  task_id, 
                                                                  run_id, 
                                                                  ts_nodash, 
                                                                  multi_index)
            return tmp, multi_index
        elif isinstance(res, (list, tuple)):
            tmp = []
            for item in res:
                ret_val, multi_index =_serialize_snowpark_results(item, 
                                                                  snowpark_session, 
                                                                  temp_data_dict, 
                                                                  conn_params, 
                                                                  dag_id, 
                                                                  task_id, 
                                                                  run_id, 
                                                                  ts_nodash, 
                                                                  multi_index)
                tmp.append(ret_val)
            return res.__class__(tmp), multi_index
        else:
            return res, multi_index
    else:
        return res, multi_index

def _serialize_table_args(arg:Any):
    """
    Recursively serializes SnowparkTable objects to json
    """
    if isinstance(arg, SnowparkTable): 
        return arg.to_json()
            
    elif isinstance(arg, dict):
        tmp = {}
        for k, v in arg.items():
            if isinstance(v, (dict)):
                tmp[k] = _serialize_table_args(arg.get(k, {}))
            elif isinstance(v, list):
                tmp[k] = _serialize_table_args(arg.get(k, []))
            elif isinstance(v, tuple):
                tmp[k] = _serialize_table_args(arg.get(k, ()))
            else:
                tmp[k] = _serialize_table_args(arg.get(k))
        return tmp
    elif isinstance(arg, list):
            return [_serialize_table_args(item) for item in arg]
    elif isinstance(arg, tuple):
            return tuple(_serialize_table_args(item) for item in arg)
    else:
        return arg

def _deserialize_snowpark_tables(arg:Any):
            
    if isinstance(arg, dict):
        if arg.get("class", "") == "SnowparkTable":
            return SnowparkTable.from_json(arg)
        else:
            tmp = {}
            for k, v in arg.items():
                if isinstance(v, (dict)):
                    tmp[k] = _deserialize_snowpark_tables(arg.get(k, {}))
                elif isinstance(v, list):
                    tmp[k] = _deserialize_snowpark_tables(arg.get(k, []))
                elif isinstance(v, tuple):
                    tmp[k] = _deserialize_snowpark_tables(arg.get(k, ()))
                else:
                    tmp[k] = _deserialize_snowpark_tables(arg.get(k))
            return tmp
    elif isinstance(arg, list):
            return [_deserialize_snowpark_tables(item) for item in arg]
    elif isinstance(arg, tuple):
            return tuple(_deserialize_snowpark_tables(item) for item in arg)
    else:
        return arg

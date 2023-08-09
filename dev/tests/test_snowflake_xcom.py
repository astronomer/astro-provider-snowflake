import os

import numpy as np
import pandas as pd
from airflow.models.dataset import Dataset
from astro.files import File
from astro.table import Metadata, Table

os.environ["AIRFLOW__CORE__XCOM_SNOWFLAKE_CONN_NAME"] = "snowflake_default"
os.environ["AIRFLOW__CORE__XCOM_SNOWFLAKE_TABLE"] = "AIRFLOW_XCOM.PUBLIC.XCOM_TABLE"
os.environ[
    "AIRFLOW__CORE__XCOM_BACKEND"
] = "astronomer.providers.snowflake.xcom_backends.snowflake.SnowflakeXComBackend"
os.environ["AIRFLOW__CORE__XCOM_SNOWFLAKE_STAGE"] = "AIRFLOW_XCOM.PUBLIC.XCOM_STAGE"


# from astronomer.providers.snowflake.xcom_backends.snowflake import SnowflakeXComBackend
self = SnowflakeXComBackend()

dag_id = "test_dag_id"
task_id = "test_task_id"
run_id = "test_run_id"
multi_index = -1
key = "mykey"

# value = "some crazy junk"
# uri = self._serialize(value=value, dag_id=dag_id, task_id=task_id, run_id=run_id, multi_index=multi_index, key='mykey')
# assert value == self._deserialize(uri)

# value = json.dumps(dict.fromkeys(range(8))) #.replace("{", "\\\{").replace("}", "\\\}")
# len(json.dumps(value)) - _SNOWFLAKE_VARIANT_SIZE_LIMIT
# uri = self._serialize(value=value, dag_id=dag_id, task_id=task_id, run_id=run_id, multi_index=multi_index, key='mykey2')
# assert value == self._deserialize(uri)

# med_str_value = json.dumps(dict.fromkeys(range(98324)))
# len(json.dumps(med_str_value)) - _SNOWFLAKE_VARIANT_SIZE_LIMIT
# uri = self._serialize(value=med_str_value, dag_id=dag_id, task_id=task_id, run_id=run_id, multi_index=multi_index, key='mymedstr')
# test_med_str = self._deserialize(uri)
# assert med_str_value == test_med_str

# long_str = json.dumps(dict.fromkeys(range(1052255)))
# len(long_str) #- _SNOWFLAKE_VARIANT_SIZE_LIMIT
# uri = self._serialize(value=long_str, dag_id=dag_id, task_id=task_id, run_id=run_id, multi_index=multi_index, key='mylongstr')
# test_long_str = self._deserialize(uri)
# assert long_str == test_long_str

# small_non_serializable_dict = {"a": 1, "b": 1, 3: 'c', 4: 6}
# uri = self._serialize(value=small_non_serializable_dict, dag_id=dag_id, task_id=task_id, run_id=run_id, multi_index=multi_index, key='mysmdict')
# test_ns_small_dict = self._deserialize(uri)
# assert small_non_serializable_dict == test_ns_small_dict

# small_serializable_dict_value = {"a": 1, "b": 1, '3': 'c', '4': 6}
# uri = self._serialize(value=small_serializable_dict_value, dag_id=dag_id, task_id=task_id, run_id=run_id, multi_index=multi_index, key='mysmdict2')
# test_s_small_dict = self._deserialize(uri)
# assert small_serializable_dict_value == test_s_small_dict

# med_s_dict = json.loads(json.dumps(dict.fromkeys(range(1052254))))
# len(json.dumps(med_s_dict).encode('utf-8')) - _SNOWFLAKE_VARIANT_SIZE_LIMIT
# len(str(med_s_dict))- _SNOWFLAKE_VARIANT_SIZE_LIMIT
# uri = self._serialize(value=med_s_dict, dag_id=dag_id, task_id=task_id, run_id=run_id, multi_index=multi_index, key='mymddict')
# test_md_dict = self._deserialize(uri)
# assert dict(sorted(med_s_dict.items())) == dict(sorted(test_md_dict.items()))

# med_ns_dict = dict.fromkeys(range(1052254))
# len(json.dumps(med_ns_dict).encode('utf-8'))
# uri = self._serialize(value=med_ns_dict, dag_id=dag_id, task_id=task_id, run_id=run_id, multi_index=multi_index, key='mymddict')
# test_md_dict = self._deserialize(uri)
# assert dict(sorted(med_ns_dict.items())) == dict(sorted(test_md_dict.items()))

# large_dict = dict.fromkeys(range(1052255))
# len(json.dumps(large_dict).encode('utf-8'))
# uri = self._serialize(value=large_dict, dag_id=dag_id, task_id=task_id, run_id=run_id, multi_index=multi_index, key='mylgdict')
# test_dict = self._deserialize(uri)
# assert large_dict == test_dict

list_value = [{"a": 1, "b": 1}, {"a": 2, "b": 4}, {"a": 3, "b": 9}]
# uri = self._serialize(value=list_value, dag_id=dag_id, task_id=task_id, run_id=run_id, multi_index=multi_index, key='mylistkey')
# assert list_value == self._deserialize(uri)

np_value = np.array(list_value)
# uri = self._serialize(value=np_value, dag_id=dag_id, task_id=task_id, run_id=run_id, multi_index=multi_index, key='mynp')
# assert np_value == self._deserialize(uri)

df_value = pd.DataFrame(list_value)
# uri = self._serialize(df_value, key, dag_id, task_id, run_id, multi_index)
# assert df_value == self._deserialize(uri)

# series_value=df_value['a']
# uri = self._serialize(value=series_value, dag_id=dag_id, task_id=task_id, run_id=run_id, multi_index=multi_index, key='myseries')
# series = self._deserialize(uri)
# type(series_value)
# type(series)
# assert series_value == series

# bin_value=np_value.dumps()
# uri = self._serialize(value=bin_value, dag_id=dag_id, task_id=task_id, run_id=run_id, multi_index=multi_index, key='mybin')
# assert bin_value == self._deserialize(uri)


empty_value = None
uri = self._serialize(
    value=empty_value,
    dag_id=dag_id,
    task_id=task_id,
    run_id=run_id,
    multi_index=multi_index,
    key="mynone",
)
assert empty_value == self._deserialize(uri)

empty_string = ""
uri = self._serialize(
    value=empty_string,
    dag_id=dag_id,
    task_id=task_id,
    run_id=run_id,
    multi_index=multi_index,
    key="mynostr",
)
assert empty_string == self._deserialize(uri)

tp_value = ("x", "y")
uri = self._serialize(
    value=tp_value,
    dag_id=dag_id,
    task_id=task_id,
    run_id=run_id,
    multi_index=multi_index,
    key="mytp",
)
assert tp_value == self._deserialize(uri)

tpn_value = ("x", None, [])
uri = self._serialize(
    value=tpn_value,
    dag_id=dag_id,
    task_id=task_id,
    run_id=run_id,
    multi_index=multi_index,
    key="mytpn",
)
assert tpn_value == self._deserialize(uri)

complex_value = (list_value, np_value, df_value)
uris = self.serialize_value(
    value=complex_value,
    dag_id=dag_id,
    task_id=task_id,
    run_id=run_id,
    multi_index=multi_index,
    key="mycomplextp",
)
# assert complex_value == self.deserialize_value(uris.decode())

table_value = Table(
    name="TAXI_RAW",
    conn_id="snowflake_default",
    metadata=Metadata(schema="", database="SANDBOX"),
    columns=[],
    temp=False,
)
uri = self._serialize(
    value=table_value,
    dag_id=dag_id,
    task_id=task_id,
    run_id=run_id,
    multi_index=multi_index,
    key="mytbl",
)
assert table_value == self._deserialize(uri)

value = File("/tmp")

value = Dataset("s3://tmp", extra={"x": "1"})


# from unittest import mock
# import pandas as pd
# import numpy as np
# import os

# import pytest
# # from snowflake.connector import ProgrammingError
# # from snowflake.connector.constants import QueryStatus

# from astronomer.providers.snowflake.hooks.snowflake import SnowflakeHook

# # POLL_INTERVAL = 1


# class TestPytestSnowflakeXcomBackend:
#     @pytest.mark.parametrize(
#         "sql,expected_sql,expected_query_ids",
#         [
#             ("select * from table", ["select * from table"], ["uuid"]),
#             (
#                 "select * from table;select * from table2",
#                 ["select * from table;", "select * from table2"],
#                 ["uuid1", "uuid2"],
#             ),
#             (["select * from table;"], ["select * from table;"], ["uuid1"]),
#             (
#                 ["select * from table;", "select * from table2;"],
#                 ["select * from table;", "select * from table2;"],
#                 ["uuid1", "uuid2"],
#             ),
#         ],
#     )
#     @mock.patch("astronomer.providers.snowflake.hooks.snowflake.SnowflakeHookAsync.get_conn")
#     def test_run_storing_query_ids(self, mock_conn, sql, expected_sql, expected_query_ids):
#         """Test run method and store, return the query ids"""
#         hook = SnowflakeHookAsync()
#         conn = mock_conn.return_value
#         cur = mock.MagicMock(rowcount=0)
#         conn.cursor.return_value = cur
#         type(cur).sfqid = mock.PropertyMock(side_effect=expected_query_ids)
#         mock_params = {"mock_param": "mock_param"}
#         hook.run(sql, parameters=mock_params)

#         cur.execute_async.assert_has_calls([mock.call(query, mock_params) for query in expected_sql])
#         assert hook.query_ids == expected_query_ids
#         cur.close.assert_called()

#     @pytest.mark.asyncio
#     @pytest.mark.parametrize(
#         "query_ids, expected_state, expected_result",
#         [
#             (["uuid"], QueryStatus.SUCCESS, {"status": "success", "query_ids": ["uuid"]}),
#             (
#                 ["uuid1"],
#                 QueryStatus.ABORTING,
#                 {
#                     "status": "error",
#                     "type": "ABORTING",
#                     "message": "The query is in the process of being aborted on the server side.",
#                     "query_id": "uuid1",
#                 },
#             ),
#             (
#                 ["uuid1"],
#                 QueryStatus.FAILED_WITH_ERROR,
#                 {
#                     "status": "error",
#                     "type": "FAILED_WITH_ERROR",
#                     "message": "The query finished unsuccessfully.",
#                     "query_id": "uuid1",
#                 },
#             ),
#             (
#                 ["uuid1"],
#                 QueryStatus.BLOCKED,
#                 {
#                     "status": "error",
#                     "message": "Unknown status: QueryStatus.BLOCKED",
#                 },
#             ),
#         ],
#     )
#     @mock.patch("astronomer.providers.snowflake.hooks.snowflake.SnowflakeHookAsync.get_conn")
#     async def test_get_query_status(self, mock_conn, query_ids, expected_state, expected_result):
#         """Test get_query_status async in run state"""
#         hook = SnowflakeHookAsync()
#         conn = mock_conn.return_value
#         conn.is_still_running.return_value = False
#         conn.get_query_status.return_value = expected_state
#         result = await hook.get_query_status(query_ids=query_ids, poll_interval=POLL_INTERVAL)
#         assert result == expected_result

#     @pytest.mark.asyncio
#     @mock.patch(
#         "astronomer.providers.snowflake.hooks.snowflake.SnowflakeHookAsync.get_conn",
#         side_effect=Exception("Connection Errors"),
#     )
#     async def test_get_query_status_error(self, mock_conn):
#         """Test get_query_status async with exception"""
#         hook = SnowflakeHookAsync()
#         conn = mock_conn.return_value
#         conn.is_still_running.side_effect = Exception("Test exception")
#         result = await hook.get_query_status(query_ids=["uuid1"], poll_interval=POLL_INTERVAL)
#         assert result == {"status": "error", "message": "Connection Errors", "type": "ERROR"}

#     @pytest.mark.asyncio
#     @mock.patch("astronomer.providers.snowflake.hooks.snowflake.SnowflakeHookAsync.get_conn")
#     async def test_get_query_status_programming_error(self, mock_conn):
#         """Test get_query_status async with Programming Error"""
#         hook = SnowflakeHookAsync()
#         conn = mock_conn.return_value
#         conn.is_still_running.return_value = False
#         conn.get_query_status.side_effect = ProgrammingError("Connection Errors")
#         result = await hook.get_query_status(query_ids=["uuid1"], poll_interval=POLL_INTERVAL)
#         assert result == {
#             "status": "error",
#             "message": "Programming Error: Connection Errors",
#             "type": "ERROR",
#         }

#     @pytest.mark.parametrize(
#         "query_ids, handler, return_last",
#         [
#             (["uuid", "uuid1"], fetch_all_snowflake_handler, False),
#             (["uuid", "uuid1"], fetch_all_snowflake_handler, True),
#             (["uuid", "uuid1"], fetch_one_snowflake_handler, True),
#             (["uuid", "uuid1"], None, True),
#         ],
#     )
#     @mock.patch("astronomer.providers.snowflake.hooks.snowflake.SnowflakeHookAsync.get_conn")
#     def test_check_query_output_query_ids(self, mock_conn, query_ids, handler, return_last):
#         """Test check_query_output by query id passed as params"""
#         hook = SnowflakeHookAsync()
#         conn = mock_conn.return_value
#         cur = mock.MagicMock(rowcount=0)
#         conn.cursor.return_value = cur
#         hook.check_query_output(query_ids=query_ids, handler=handler, return_last=return_last)

#         cur.get_results_from_sfqid.assert_has_calls([mock.call(query_id) for query_id in query_ids])
#         cur.close.assert_called()

#     @pytest.mark.parametrize(
#         "sql,expected_sql,expected_query_ids",
#         [
#             ("select * from table", ["select * from table"], ["uuid"]),
#             (
#                 "select * from table;select * from table2",
#                 ["select * from table;", "select * from table2"],
#                 ["uuid1", "uuid2"],
#             ),
#             (["select * from table;"], ["select * from table;"], ["uuid1"]),
#             (
#                 ["select * from table;", "select * from table2;"],
#                 ["select * from table;", "select * from table2;"],
#                 ["uuid1", "uuid2"],
#             ),
#         ],
#     )
#     @mock.patch("astronomer.providers.snowflake.hooks.snowflake.SnowflakeHookAsync.get_conn")
#     def test_run_storing_query_ids_without_params(self, mock_conn, sql, expected_sql, expected_query_ids):
#         """Test run method without params and store, return the query ids"""
#         hook = SnowflakeHookAsync()
#         conn = mock_conn.return_value
#         cur = mock.MagicMock(rowcount=0)
#         conn.cursor.return_value = cur
#         type(cur).sfqid = mock.PropertyMock(side_effect=expected_query_ids)
#         hook.run(sql)

#         cur.execute_async.assert_has_calls([mock.call(query) for query in expected_sql])
#         assert hook.query_ids == expected_query_ids
#         cur.close.assert_called()

from __future__ import annotations

import json
import pickle
import sys
import tempfile
from ast import literal_eval
from pathlib import Path
from typing import TYPE_CHECKING, Any

import numpy
import pandas
from airflow.models.xcom import BaseXCom

if TYPE_CHECKING:
    from airflow.models.xcom import XCom

from airflow.exceptions import AirflowException
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from astronomer.providers.snowflake.utils.snowpark_helpers import SnowparkTable
from astronomer.providers.snowflake.utils.xcom_helpers import (
    _try_parse_snowflake_xcom_uri,
    get_snowflake_xcom_objects,
)

try:
    from astro.files import File
except:
    File = None
try:
    from astro.table import Table, TempTable
except:
    Table = None
    TempTable = None
try:
    from airflow.models.dataset import Dataset
except:
    Dataset = None

_ENCODING = "utf-8"
_SNOWFLAKE_VARIANT_SIZE_LIMIT = 16777216
_SUPPORTED_FILE_SERIALIZATION_TYPES = (
    pandas.DataFrame,
    pandas.core.series.Series,
    numpy.ndarray,
    bytes,
    bytearray,
)


def _write_to_snowflake(
    value: Any,
    hook: SnowflakeHook,
    base_uri: str,
    snowflake_xcom_objects: dict,
    multi_index: int,
    key: str,
    dag_id: str,
    task_id: str,
    run_id: str,
):
    # Other downstream systems such as Snowpark operators may have serialized to
    # Snowflake already. Check for a valid xcom URI in return value and pass it through.
    if _try_parse_snowflake_xcom_uri(value):
        return value
    elif Dataset and isinstance(value, Dataset):
        json_str = json.dumps({"uri": value.uri, "extra": value.extra})
        json_serializable = True
        value_type = "airflow_Dataset"
    elif isinstance(value, _SUPPORTED_FILE_SERIALIZATION_TYPES):
        json_serializable = False
    elif value is None:
        return None
    elif isinstance(value, str):  # json dump adds double quotes
        json_str = value
        json_serializable = True
        value_type = type(value).__name__
    else:
        try:
            # check serializability
            json_str = json.dumps(value)
            json_serializable = True
            value_type = type(value).__name__

            if isinstance(value, dict) and set(map(type, value.keys())) != {str}:
                # serializing non-string keys to converts to strings
                print(
                    "Non-string-type keys found in dict. Resorting to file serialization to stage."
                )
                json_str = value
                json_serializable = False
                value_type = "bytes_dict"

        except Exception as e:
            if isinstance(e, TypeError) and "not JSON serializable" in e.args[0]:
                # return for recursion on Iterable
                return False
            else:
                raise e()

    if (
        json_serializable
        and len(json_str.encode(_ENCODING)) > _SNOWFLAKE_VARIANT_SIZE_LIMIT
    ):
        print(
            f"XCOM value size exceeds Snowflake cell size limit {_SNOWFLAKE_VARIANT_SIZE_LIMIT}. Resorting to file serialization to stage."
        )
        json_serializable = False

    if json_serializable:
        # upcert
        hook.run(
            f"""
            MERGE INTO {snowflake_xcom_objects['table']} tab1
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
        """
        )

        uri = (
            base_uri
            + f"&table={snowflake_xcom_objects['table']}&key={dag_id}/{task_id}/{run_id}/{multi_index}/{key}"
        )

    else:
        with tempfile.TemporaryDirectory() as td:
            if "json_str" in locals():  # large data or non-serializable use pickle
                temp_file = Path(f"{td}/{key}.pickle")
                with open(temp_file, "wb") as tf:
                    pickle.dump(value, tf)

            elif isinstance(value, (pandas.DataFrame, pandas.core.series.Series)):
                temp_file = Path(f"{td}/{key}.parquet")
                pandas.DataFrame(value).to_parquet(temp_file)

            elif isinstance(value, numpy.ndarray):
                temp_file = Path(f"{td}/{key}.np")
                _ = temp_file.write_bytes(value.dumps())

            elif isinstance(value, (bytes, bytearray)):
                temp_file = Path(f"{td}/{key}.bin")
                _ = temp_file.write_bytes(value)
            else:
                raise AttributeError(
                    f"Could not serialize object of type {type(value)}"
                )

            hook.run(
                f"""
                PUT file://{temp_file} @{snowflake_xcom_objects['stage']}/{dag_id}/{task_id}/{run_id}/{multi_index}/
                AUTO_COMPRESS = FALSE
                SOURCE_COMPRESSION = NONE
                OVERWRITE = TRUE
            """
            )

            uri = f"{base_uri}&stage={snowflake_xcom_objects['stage']}&key={dag_id}/{task_id}/{run_id}/{multi_index}/{temp_file.name}"

    return uri


def _serialize_table_values(value: Any):
    if any(
        (
            isinstance(value, SnowparkTable),
            (File and isinstance(value, File)),
            (Table and isinstance(value, Table)),
            (TempTable and isinstance(value, TempTable)),
        )
    ):
        return value.to_json()

    if isinstance(value, dict):
        return {k: _serialize_table_values(v) for k, v in value.items()}

    elif isinstance(value, (list, tuple)):
        return value.__class__(_serialize_table_values(item) for item in value)

    else:
        return value


def _read_from_snowflake(
    parsed_uri: str, hook: SnowflakeHook, snowflake_xcom_objects: dict
) -> Any:
    if parsed_uri["xcom_stage"]:
        with tempfile.TemporaryDirectory() as td:
            hook.run(
                f"GET @{snowflake_xcom_objects['stage']}/{parsed_uri['xcom_key']} file://{td}"
            )

            temp_file = Path(td).joinpath(Path(parsed_uri["xcom_key"]).name)
            temp_file_type = temp_file.as_posix().split(".")[-1]

            if temp_file_type == "parquet":
                return pandas.read_parquet(temp_file)
            elif temp_file_type == "np":
                return numpy.load(temp_file, allow_pickle=True)
            elif temp_file_type == "bin":
                return temp_file.read_bytes()
            elif (
                temp_file_type == "str"
            ):  # could be a stringified dict.  Don't want to return a dict
                return temp_file.read_text()
            elif temp_file_type == "pickle":
                with open(temp_file, "rb") as tf:
                    return pickle.load(tf)
            else:
                try:
                    return getattr(sys.modules["builtins"], temp_file_type)(
                        literal_eval(temp_file.read_text())
                    )
                except:
                    raise AirflowException(
                        f"Cannot parse URI for file type {temp_file_type}"
                    )

    elif parsed_uri["xcom_table"]:
        xcom_cols = parsed_uri["xcom_key"].split("/")

        ret_value_type, ret_value = hook.get_records(
            f"""
                                            SELECT VALUE_TYPE, VALUE FROM {parsed_uri['xcom_table']}
                                            WHERE dag_id = '{xcom_cols[0]}'
                                            AND task_id = '{xcom_cols[1]}'
                                            AND run_id = '{xcom_cols[2]}'
                                            AND multi_index = '{xcom_cols[3]}'
                                            AND key = '{xcom_cols[4]}'
                                        ;"""
        )[0]

        if ret_value_type == "str":
            return ret_value

        elif ret_value_type == "dict":
            class_type = json.loads(ret_value).get("class")
            if class_type in ("File", "Table", "TempTable", "SnowparkTable"):
                obj_type = globals().get(class_type, None)
                if obj_type:
                    return obj_type.from_json(json.loads(ret_value))
                else:
                    raise AirflowException(
                        f"Trying to deserialize object of type {class_type}. But packages are not installed."
                    )
            else:
                return json.loads(ret_value)

        elif ret_value_type == "airflow_Dataset":
            if Dataset:
                dataset_dict = json.loads(ret_value)
                return Dataset(uri=dataset_dict["uri"], extra=dataset_dict["extra"])
            else:
                raise AirflowException(
                    "Trying to deserialize object of type Dataset. But packages are not installed."
                )
        else:
            try:
                return getattr(sys.modules["builtins"], ret_value_type)(
                    json.loads(ret_value)
                )
            except:
                raise AirflowException(
                    f"Cannot parse URI for file type {ret_value_type}"
                )


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
    def _serialize_values(
        value: Any,
        hook: SnowflakeHook,
        base_uri: str,
        snowflake_xcom_objects: dict,
        multi_index: int,
        key: str,
        dag_id: str,
        task_id: str,
        run_id: str,
    ):
        uri = _write_to_snowflake(
            value,
            hook,
            base_uri,
            snowflake_xcom_objects,
            multi_index,
            key,
            dag_id,
            task_id,
            run_id,
        )
        if uri:
            multi_index += 1
            return uri, multi_index
        elif uri is None:
            return uri, multi_index

        if isinstance(value, dict):
            tmp = {}
            for k, v in value.items():
                tmp[k], multi_index = SnowflakeXComBackend._serialize_values(
                    v,
                    hook,
                    base_uri,
                    snowflake_xcom_objects,
                    multi_index,
                    key,
                    dag_id,
                    task_id,
                    run_id,
                )
            return tmp, multi_index
        elif isinstance(value, (list, tuple)):
            tmp = []
            for item in value:
                ret_val, multi_index = SnowflakeXComBackend._serialize_values(
                    item,
                    hook,
                    base_uri,
                    snowflake_xcom_objects,
                    multi_index,
                    key,
                    dag_id,
                    task_id,
                    run_id,
                )
                tmp.append(ret_val)
            return value.__class__(tmp), multi_index
        else:
            assert Exception("recursion fall through")

    @staticmethod
    def _deserialize_values(
        uri: Any, hook: SnowflakeHook, snowflake_xcom_objects: dict
    ):
        parsed_uri = _try_parse_snowflake_xcom_uri(uri)

        if parsed_uri:
            hook.account = parsed_uri["account"]
            hook.region = parsed_uri["region"]
            return _read_from_snowflake(parsed_uri, hook, snowflake_xcom_objects)

        if isinstance(uri, dict):
            return {
                k: SnowflakeXComBackend._deserialize_values(
                    v, hook, snowflake_xcom_objects
                )
                for k, v in uri.items()
            }
        elif isinstance(uri, (list, tuple)):
            return uri.__class__(
                SnowflakeXComBackend._deserialize_values(
                    item, hook, snowflake_xcom_objects
                )
                for item in uri
            )
        else:
            assert Exception("fall through")

    @staticmethod
    def serialize_value(
        value: Any,
        key: str,
        task_id: str,
        dag_id: str,
        run_id: str,
        map_index: int | None = None,
        **kwargs,
    ):
        """
        Custom XCom Wrapper serializes to Snowflake stage/table and returns the URI to Xcom.

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
        :param map_index:
        :type map_index: int
        :return: The byte encoded uri string.
        :rtype: bytes

        """

        snowflake_xcom_objects = get_snowflake_xcom_objects()
        hook = SnowflakeHook(snowflake_conn_id=snowflake_xcom_objects["conn_id"])

        conn_params = hook._get_conn_params()

        if conn_params["region"]:
            base_uri = f"snowflake://{conn_params['account']}.{conn_params['region']}?"
        else:
            base_uri = f"snowflake://{conn_params['account']}?"

        multi_index = 0

        # first serialize any Table, TempTable, File or SnowparkTable values to json with their serializers
        value = _serialize_table_values(value)

        # try to serialize the whole value and then recurse over iterable if necessary
        uris, _ = SnowflakeXComBackend._serialize_values(
            value=value,
            hook=hook,
            base_uri=base_uri,
            snowflake_xcom_objects=snowflake_xcom_objects,
            key=key,
            dag_id=dag_id,
            task_id=task_id,
            run_id=run_id,
            multi_index=multi_index,
        )
        return BaseXCom.serialize_value(value=uris, **kwargs)

    @staticmethod
    def deserialize_value(result: XCom) -> Any:
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

        snowflake_xcom_objects = get_snowflake_xcom_objects()
        hook = SnowflakeHook(snowflake_conn_id=snowflake_xcom_objects["conn_id"])

        uris = BaseXCom.deserialize_value(result)

        # try to serialize the whole value and then recurse over iterable if necessary
        return SnowflakeXComBackend._deserialize_values(
            uri=uris, hook=hook, snowflake_xcom_objects=snowflake_xcom_objects
        )

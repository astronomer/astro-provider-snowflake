# import io
# import pandas as pd
# from airflow.decorators import dag, task
# from airflow.utils.dates import days_ago

# from astro import sql as aql
# from astro.sql.table import Table
# from astro.files import File
# from astronomer.providers.snowflake.utils.snowpark_helpers import SnowparkTable

# PACKAGES = [
#     "snowflake-snowpark-python",
#     "scikit-learn",
#     "pandas",
#     "numpy",
#     "joblib",
#     "cachetools",
# ]

# _SNOWPARK_BIN = '/home/astro/.venv/snowpark/bin/python'

# @dag(
#     default_args={"owner": "Airflow", 
#                   "temp_data_output": 'table',
#                   "temp_data_schema": 'xcom',
#                   "temp_data_overwrite": True},
#     schedule_interval=None,
#     start_date=days_ago(2),
#     tags=["example"],
# )
# def sproc_test_dag():
#     """
#     Predict `MEDIAN_HOUSE_VALUE` using a Random Forest Regressor.

#     Tasks:
#         1. Register sproc that creates a model
#         2. Define prediction UDF
#         3. Predict
#         4. Show results
#     """

#     @task.snowpark_ext_python(python=_SNOWPARK_BIN)
#     def extract_housing_data():
#         from sklearn.datasets import fetch_california_housing
#         df = fetch_california_housing(download_if_missing=True, as_frame=True).frame
#         return snowpark_session.create_dataframe(df)

#     @task.snowpark_ext_python(python=_SNOWPARK_BIN)
#     def register_sproc_to_create_model_func(housingdf:SnowparkTable, PACKAGES:list):
#         from snowflake.snowpark.functions import sproc

#         snowpark_session.add_packages(*PACKAGES)

#         def train_model(snowpark_session: SnowparkSession, housingdf_name:str) -> float:
#             import numpy as np
#             import joblib
#             from sklearn.compose import ColumnTransformer
#             from sklearn.ensemble import RandomForestRegressor
#             from sklearn.impute import SimpleImputer
#             from sklearn.metrics import mean_squared_error
#             from sklearn.model_selection import train_test_split
#             from sklearn.pipeline import Pipeline
#             from sklearn.preprocessing import OneHotEncoder, StandardScaler

#             housingdf = snowpark_session.table(housingdf_name)
#             model_path = "@XCOM_STAGE/housing_RF_reg.joblib"

#             train, test = housingdf.random_split([0.8, 0.2], seed=42)

#             X = train.drop('"MedHouseVal"').to_pandas()
#             y = train.select('"MedHouseVal"').to_pandas()
#             X_test = test.drop('"MedHouseVal"').to_pandas()
#             y_test = test.select('"MedHouseVal"').to_pandas()

#             pipeline = Pipeline(
#                 [
#                     ("imputer", SimpleImputer(strategy="median")),
#                     ("std_scaler", StandardScaler()),
#                 ]
#             )

#             preprocessor = ColumnTransformer(
#                 [
#                     ("num", pipeline, list(X.columns)),
#                 ]
#             )
#             full_pipeline = Pipeline(
#                 [
#                     ("preprocessor", preprocessor),
#                     ("model", RandomForestRegressor(n_estimators=100, random_state=42)),
#                 ]
#             )

#             full_pipeline.fit(X, y)

#             # save full pipeline including the model
#             with io.BytesIO() as input_stream:
#                 joblib.dump(full_pipeline, input_stream)
#                 snowpark_session._conn.upload_stream(input_stream, model_path)

#             mse = mean_squared_error(y_test, full_pipeline.predict(X_test))
#             rmse = np.sqrt(mse)

#             return {'rmse': rmse, 'model_path': model_path}

#         train_model_sp = sproc(
#             train_model,
#             replace=True,
#             is_permanent=False,
#             name="housing_sproc_from_local_test",
#             # stage_location="@XCOM_STAGE",
#             session=snowpark_session,
#         )
#         # to-do: return train_model_sp as SPROC() object - `session.udf.register`

#         train_model_sp(session=snowpark_session, )

#         print(train_model_sp.__dict__)

#     @task.snowpark_ext_python(python=_SNOWPARK_BIN)
#     def define_predict_udf_func():
#         from snowflake.snowpark.functions import udf

#         snowpark_session.add_packages(PACKAGES)
#         snowpark_session.add_import("@MODELS/housing_fores_reg.joblib")

#         @udf(
#             name="predict",
#             is_permanent=True,
#             stage_location="@udf",
#             replace=True,
#             session=snowpark_session,
#             packages=PACKAGES,
#         )
#         def predict(
#             LONGITUDE: float,
#             LATITUDE: float,
#             HOUSING_MEDIAN_AGE: float,
#             TOTAL_ROOMS: float,
#             TOTAL_BEDROOMS: float,
#             POPULATION: float,
#             HOUSEHOLDS: float,
#             MEDIAN_INCOME: float,
#             OCEAN_PROXIMITY: str,
#         ) -> float:
#             import os
#             import sys
#             import cachetools

#             @cachetools.cached(cache={})
#             def read_file(filename):
#                 import joblib

#                 import_dir = sys._xoptions.get("snowflake_import_directory")
#                 if import_dir:
#                     with open(os.path.join(import_dir, filename), "rb") as file:
#                         m = joblib.load(file)
#                         return m

#             features = [
#                 "LONGITUDE",
#                 "LATITUDE",
#                 "HOUSING_MEDIAN_AGE",
#                 "TOTAL_ROOMS",
#                 "TOTAL_BEDROOMS",
#                 "POPULATION",
#                 "HOUSEHOLDS",
#                 "MEDIAN_INCOME",
#                 "OCEAN_PROXIMITY",
#             ]

#             m = read_file("housing_fores_reg.joblib")
#             row = pd.DataFrame([locals()], columns=features)
#             prediction = m.predict(row)[0]
#             return prediction

#         # to-do: return predict
#         # AttributeError: Can't pickle local object 'dataframe_poc_dag.<locals>.define_predict_udf_func.<locals>.predict'

#     @task.snowpark_ext_python(python=_SNOWPARK_BIN)
#     def do_prediction_func():

#         snowdf_test = snowpark_session.table("HOUSING_TEST")
#         inputs = snowdf_test.drop("MEDIAN_HOUSE_VALUE")
#         snowdf_test.show()

#         return snowdf_test.select(
#             *inputs,
#             F.call_udf("predict", *inputs).alias("PREDICTION"),
#             (F.col("MEDIAN_HOUSE_VALUE")).alias("ACTUAL_LABEL"),
#             (
#                 (F.col("ACTUAL_LABEL") - F.col("PREDICTION"))
#                 / F.col("ACTUAL_LABEL")
#                 * 100
#             ).alias("DELTA_PCT")
#         ).limit(20)

#     @task.snowpark_ext_python(python=_SNOWPARK_BIN)
#     def show_results_func(df: SnowparkTable):
#         df.select("ACTUAL_LABEL", "PREDICTION", "DELTA_PCT").show()

#     register_sproc_to_create_model = register_sproc_to_create_model_func(PACKAGES)
#     define_predict_udf = define_predict_udf_func()
#     do_prediction = do_prediction_func()
#     show_results = show_results_func(do_prediction)

#     register_sproc_to_create_model >> define_predict_udf >> do_prediction


# dag_obj = sproc_test_dag()

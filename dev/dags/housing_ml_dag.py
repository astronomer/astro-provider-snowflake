# import datetime
# import io
# import json

# import pandas as pd
# import snowflake.snowpark
# from airflow.decorators import dag, task
# from airflow.utils.dates import days_ago
# from snowflake.snowpark import functions as F
# from snowflake.snowpark import version as v
# from snowflake.snowpark.functions import col, sproc, udf
# from snowflake.snowpark.session import Session

# # from astronomer.providers.snowflake.operators import BaseSnowparkOperator

# # from snowpark_provider.decorators import snowpark_df_decorator
# # from snowpark_provider.utils.table import Table

# # to-do: declare global snowpark_session.add_packages(...)
# PACKAGES = [
#     "snowflake-snowpark-python",
#     "scikit-learn",
#     "pandas",
#     "numpy",
#     "joblib",
#     "cachetools",
# ]


# @dag(
#     default_args={"owner": "Airflow"},
#     schedule_interval=None,
#     start_date=days_ago(2),
#     tags=["example"],
# )
# def housing_ml_dag():
#     """
#     Predict `MEDIAN_HOUSE_VALUE` using a Random Forest Regressor.

#     Tasks:
#         1. Register sproc that creates a model
#         2. Define prediction UDF
#         3. Predict
#         4. Show results
#     """

#     @snowpark_df_decorator(
#         conn_id="snowflake_default", task_id="register_sproc_to_create_model"
#     )
#     def register_sproc_to_create_model_func(snowpark_session: Session):
#         snowpark_session.add_packages(*PACKAGES)
#         snowpark_session.sql("USE SCHEMA SNOWPARK_DEMO").collect()

#         def save_file(session, model, path):
#             import joblib

#             input_stream = io.BytesIO()
#             joblib.dump(model, input_stream)
#             session._conn._cursor.upload_stream(input_stream, path)
#             return "successfully created file: " + path

#         def train_model(snowpark_session: Session) -> float:
#             import numpy as np
#             from sklearn.compose import ColumnTransformer
#             from sklearn.ensemble import RandomForestRegressor
#             from sklearn.impute import SimpleImputer
#             from sklearn.metrics import mean_squared_error
#             from sklearn.model_selection import train_test_split
#             from sklearn.pipeline import Pipeline
#             from sklearn.preprocessing import OneHotEncoder, StandardScaler

#             snowdf = snowpark_session.table("HOUSING_DATA")

#             # split into train and test set
#             snowdf_train, snowdf_test = snowdf.random_split([0.8, 0.2], seed=82)

#             # save train and test sets as timesttamped tables in Snowflake
#             snowdf_train.write.mode("overwrite").save_as_table("HOUSING_TRAIN")
#             snowdf_test.write.mode("overwrite").save_as_table("HOUSING_TEST")

#             # drop labels for training set
#             housing = snowdf_train.drop("MEDIAN_HOUSE_VALUE").to_pandas()
#             housing_labels = snowdf_train.select("MEDIAN_HOUSE_VALUE").to_pandas()
#             housing_test = snowdf_test.drop("MEDIAN_HOUSE_VALUE").to_pandas()
#             housing_test_labels = snowdf_test.select("MEDIAN_HOUSE_VALUE").to_pandas()

#             # numerical features
#             housing_num = housing.drop("OCEAN_PROXIMITY", axis=1)
#             # create a pipeline for numerical features
#             num_pipeline = Pipeline(
#                 [
#                     ("imputer", SimpleImputer(strategy="median")),
#                     ("std_scaler", StandardScaler()),
#                 ]
#             )

#             num_attribs = list(housing_num)
#             cat_attribs = ["OCEAN_PROXIMITY"]

#             preprocessor = ColumnTransformer(
#                 [
#                     ("num", num_pipeline, num_attribs),
#                     ("cat", OneHotEncoder(), cat_attribs),
#                 ]
#             )
#             full_pipeline = Pipeline(
#                 [
#                     ("preprocessor", preprocessor),
#                     ("model", RandomForestRegressor(n_estimators=100, random_state=42)),
#                 ]
#             )

#             # fit the preprocessing pipeline and the model
#             full_pipeline.fit(housing, housing_labels)

#             # save full pipeline including the model
#             save_file(
#                 snowpark_session, full_pipeline, "@MODELS/housing_fores_reg.joblib"
#             )

#             # predict on the test set and return the RMSE
#             housing_predictions = full_pipeline.predict(housing_test)
#             lin_mse = mean_squared_error(housing_test_labels, housing_predictions)
#             lin_rmse = np.sqrt(lin_mse)
#             return lin_rmse

#         train_model_sp = sproc(
#             train_model,
#             replace=True,
#             is_permanent=True,
#             name="housing_sproc_from_local_test",
#             stage_location="@SNOWPARK_ML_STAGE",
#             session=snowpark_session,
#         )
#         # to-do: return train_model_sp as SPROC() object - `session.udf.register`

#         train_model_sp(session=snowpark_session)

#         print(train_model_sp.__dict__)

#     @snowpark_df_decorator(conn_id="snowflake_default", task_id="define_predict_udf")
#     def define_predict_udf_func(snowpark_session: Session):

#         snowpark_session.add_packages(PACKAGES)
#         snowpark_session.add_import("@MODELS/housing_fores_reg.joblib")
#         snowpark_session.sql("USE SCHEMA SNOWPARK_DEMO").collect()

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

#     @snowpark_df_decorator(conn_id="snowflake_default", task_id="do_prediction")
#     def do_prediction_func(snowpark_session: Session):
#         snowpark_session.sql("USE SCHEMA SNOWPARK_DEMO").collect()

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

#     @snowpark_df_decorator(conn_id="snowflake_default", task_id="show_results")
#     def show_results_func(df: Table, snowpark_session: Session):
#         df2 = df.select("ACTUAL_LABEL", "PREDICTION", "DELTA_PCT")
#         df2.show()

#     register_sproc_to_create_model = register_sproc_to_create_model_func()
#     define_predict_udf = define_predict_udf_func()
#     do_prediction = do_prediction_func()
#     show_results = show_results_func(do_prediction)

#     register_sproc_to_create_model >> define_predict_udf >> do_prediction


# dag_obj = housing_ml_dag()

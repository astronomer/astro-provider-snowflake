# import json

# from airflow.decorators import dag, task
# from airflow.utils.dates import days_ago
# from snowflake.snowpark.functions import col

# from snowpark_provider.decorators import snowpark_df_decorator
# from snowpark_provider.utils.table import Table


# @dag(
#     default_args={"owner": "Airflow"},
#     schedule_interval=None,
#     start_date=days_ago(2),
#     tags=["example"],
# )
# def dataframe_poc_dag():
#     """
#     task_1 creates a Snowpark dataframe and passes it to task_2
#     """

#     @snowpark_df_decorator(conn_id="snowflake_default", task_id="task_1")
#     def task_1_func(snowpark_session):
#         df = snowpark_session.create_dataframe([1, 2, 3, 4]).to_df(
#             "snowpark_task_function"
#         )
#         df.show()
#         df.write.mode("overwrite").save_as_table("custom_table_saved_with_session")
#         return df

#     @snowpark_df_decorator(conn_id="snowflake_default", task_id="task_2")
#     def task_2_func(df: Table, snowpark_session):
#         print("This is output from the upstream task:")
#         print(df)
#         df.show()
#         return df

#     task_1 = task_1_func()
#     task_2 = task_2_func(task_1)


# dag_obj = dataframe_poc_dag()

# Apache Airflow Provider for Snowflake

This provider from Astronomer extends the capabilities of the base [Snowflake Provider](https://github.com/apache/airflow/tree/main/airflow/providers/snowflake) with additional components for:  
  
- Custom Xcom Backend: To provide additional security and data governance this feature allows storing task input and output in Snowflake. Rather than storing potentially-sensitive data in the Airflow XCom tables Snowflake users can now ensure that all their data stays in Snowflake.  JSON-serializable data is stored in an XCom table and non-JSON serializable data is stored as objects in a Snowflake stage.
      
- [Snowpark Python](https://docs.snowflake.com/en/developer-guide/snowpark/python/index): This feature from Snowflake provides an intuitive Dataframe API as well as the ability to run python-based user-defined functions and stored procedures for processing data in Snowflake.  The provider includes operators and decorators to remove boiler-plate setup and teardown operations and generally simplify running Snowpark python tasks.  All operators instantiate a Snowpark Python session named `snowpark_session` which can be referenced in the python callable.  Additionally, there is a `SnowparkTable` object.  Any SnowparkTable arguments of the python callable are instantiated as Snowpark dataframe objects.
  - `SnowparkPythonOperator`: This is the simplest operator which runs as a PythonOperator in the Airflow instance.  This requires that the Airflow instance is running a version of python supported by Snowpark and has Snowpark Python package installed. NOTE: Currently Snowpark only supports python 3.8 so this operator has limited functionality.  Snowpark python for 3.9 and 3.10 is expected soon.
  - `SnowparkVirtualenvOperator`: This operator creates a python virtualenv to run the python callable in a subprocess.  Users can specify python package requirements (ie. snowflake-snowpark-python).  
  - `SnowparkExternalPythonOperator`: This operator runs the Snowpark python callable in a pre-existing virtualenv. It is assumed that Snowpark is already installed in that environment. Using the [Astronomer buildkit](https://github.com/astronomer/astro-provider-venv) will simplify building this environment.
  - `SnowparkPythonUDFOperator`: (TBD) 
  - `SnowparkPythonSPROCOperator`: (TBD)
  - `snowpark_python_task`: Decorator for SnowparkPythonOperator
  - `snowpark_virtualenv_task`: Decorator for SnowparkVirtualenvOperator
  - `snowpark_ext_python_task`: Decorator for SnowparkExternalPythonOperator
# Apache Airflow Provider for Snowflake

This provider from Astronomer extends the capabilities of the base [Snowflake Provider](https://github.com/apache/airflow/tree/main/airflow/providers/snowflake) with additional components for:  
  
- Custom Xcom Backend: To provide additional security and data governance this feature allows storing task input and output in Snowflake. Rather than storing potentially-sensitive data in the Airflow XCom tables Snowflake users can now ensure that all their data stays in Snowflake.  JSON-serializable data is stored in an XCom table and non-JSON serializable data is stored as objects in a Snowflake stage.
  
- Snowservices: Snowflake's new compute offering provides managed, container-based compute "next" to the data in Snowflake.  By providing non-shared compute Snowservices will reduce some limitations of Snowpark Python including egress access to running services, specialized compute such as GPUs and simpler dependency management with the ability to install any language, software or library.
  - Hook: The Snowservices hook allows simple connection to Snowservices as well as a python interface for CRUD operations on Snowservices.  Additionally the hook has integrations with the [Astro CLI](https://github.com/astronomer/astro-cli) to support local development.  Snowservices can be created in local docker containers for integration and testing before being pushed to Snowservices for Dev/Prod. 
  - Python Operator: The Snowservices python operator is used in conjunction with a Snowservices Runner [(see example)](https://github.com/astronomer/airflow-snowservices-demo/tree/main/include/airflow-runner) container to bring code to the data.  This allows execution of Python callable functions where the data sits in Snowflake. 
  - Decorator: This provides cleaner Airflow DAG code while simplifying task input and output, and lineage tracking.
    
- [Snowpark Python](https://docs.snowflake.com/en/developer-guide/snowpark/python/index) (in-progress): This feature from Snowflake provides an intuitive Dataframe API as well as the ability to run python-based user-defined functions and stored procedures for processing data in Snowflake.  The provider includes a decorator to remove boiler-plate setup and teardown operations and generally simplify running Snowpark python tasks.
  
- [SnowML](https://docs.snowflake.com/LIMITEDACCESS/data-tranformation-snowpark-python) (TBD): 
  
See the [Airflow Snowservices Demo](https://github.com/mpgreg/airflow-snowservices-demo/) for details on how to use these features.
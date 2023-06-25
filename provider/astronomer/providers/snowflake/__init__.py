__version__ = "0.0.1a1"


def get_provider_info():
    return {
        "package-name": "astro-provider-snowflake",
        "name": "astro-provider-snowflake",
        "description": "Snowpark Decorators.",
        "task-decorators": [
            {
                "name": "snowpark_python",
                "class-name": "astronomer.providers.snowflake.decorators.snowpark.snowpark_python_task",
            },
            {
                "name": "snowpark_virtualenv",
                "class-name": "astronomer.providers.snowflake.decorators.snowpark.snowpark_virtualenv_task",
            },
            {
                "name": "snowpark_ext_python",
                "class-name": "astronomer.providers.snowflake.decorators.snowpark.snowpark_ext_python_task",
            },
        ],
    }
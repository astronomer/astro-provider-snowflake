# syntax=quay.io/astronomer/airflow-extensions:latest

FROM quay.io/astronomer/astro-runtime:7.5.0-base

COPY include/astro_provider_snowflake-0.0.0-py3-none-any.whl /tmp

PYENV 3.8 snowpark requirements-snowpark.txt

#Seed the base 3.8 python with snowpark packages for virtualenv operator
COPY requirements-snowpark.txt /tmp
RUN python3.8 -m pip install -r /tmp/requirements-snowpark.txt

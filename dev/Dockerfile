# syntax=quay.io/astronomer/airflow-extensions:latest

FROM quay.io/astronomer/astro-runtime:10.2.0

COPY include/astro_provider_snowpark-0.0.0-py3-none-any.whl /tmp

PYENV 3.8 snowpark requirements-snowpark.txt
PYENV 3.9 snowpark9 requirements-snowpark.txt

#Seed the base 3.8 python with snowpark packages for virtualenv operator
COPY requirements-snowpark.txt /tmp
RUN python3.8 -m pip install -r /tmp/requirements-snowpark.txt
RUN python3.9 -m pip install -r /tmp/requirements-snowpark.txt

# syntax=quay.io/astronomer/airflow-extensions:latest

FROM quay.io/astronomer/astro-runtime:7.4.2-base

# COPY include/astro_provider_snowflake-0.0.1-py3-none-any.whl /tmp

PYENV 3.8 snowpark requirements-snowpark.txt

#TODO fix after merge https://github.com/astronomer/astro-provider-venv/pull/14/
USER root
RUN chown -R astro:astro /home/astro/.cache
USER astro

#Seed packages in the 3.8 base for system-site-packages with PythonVirtualenvOperator
RUN python3.8 -m pip install 'snowflake-snowpark-python[pandas]==1.2.0' virtualenv 
#/tmp/astro_provider_snowflake-0.0.1-py3-none-any.whl


# USER root

# # onbuild stuff
# SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]
# LABEL maintainer="Astronomer <humans@astronomer.io>"
# LABEL io.astronomer.docker=true
# LABEL io.astronomer.docker.airflow.onbuild=true

# COPY dev/packages.txt .
# RUN if [[ -s packages.txt ]]; then \
#     apt-get update && cat packages.txt | tr '\r\n' '\n' | sed -e 's/#.*//' | xargs apt-get install -y --no-install-recommends \
#     && apt-get clean \
#     && rm -rf /var/lib/apt/lists/*; \
#   fi

# # Install python packages
# COPY dev/requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt


# # Install astro-sdk-template
# COPY ./pyproject.toml  ${AIRFLOW_HOME}/astro_sdk_template/
# # The following file are needed because version they are referenced from pyproject.toml
# COPY ./README.md  ${AIRFLOW_HOME}/astro_sdk_template/
# COPY ./src/astro_sdk_template/__init__.py  ${AIRFLOW_HOME}/astro_sdk_template/src/astro_sdk_template/__init__.py

# RUN ls -l ${AIRFLOW_HOME}/astro_sdk_template/
# RUN pip install -e "${AIRFLOW_HOME}/astro_sdk_template"


# USER astro

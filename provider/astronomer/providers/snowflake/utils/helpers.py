from __future__ import annotations
import os
from pathlib import Path
import yaml
import warnings

_default_runner_endpoint_name = 'airflow-runner'
_default_runner_port = 8001
_default_runner_image_uri = 'mpgregor/snowservices-runner:latest'

#'AIRFLOW_CONN_SNOWFLAKE_XCOM': 'snowflake://<USER_NAME>:<PASSWORD>@/<SCHEMA_NAME>?account=<ACCOUNT_NAME>&region=<REGION_NAME>&database=<DATABASE_NAME>&warehouse=<WAREHOUSE_NAME>&role=<ROLE_NAME>',
#'AIRFLOW__CORE__XCOM_BACKEND': 'astronomer.providers.snowflake.xcom_backends.snowflake.SnowflakeXComBackend',
#'AIRFLOW__CORE__XCOM_SNOWFLAKE_TABLE': 'AIRFLOW_XCOM.PUBLIC.XCOM_TABLE',
#'AIRFLOW__CORE__XCOM_SNOWFLAKE_STAGE': 'AIRFLOW_XCOM.PUBLIC.XCOM_STAGE',
#'AIRFLOW__CORE__XCOM_SNOWFLAKE_CONN_NAME': 'snowflake_xcom'


def create_runner_spec(
    service_name:str, 
    runner_image: str,
    runner_port:int, 
    runner_endpoint_name:str = None,
    local_test:str | None = None
    ):
    
    if runner_port and not isinstance(runner_port, int):
        raise AttributeError('runner_port must be an integer.')
    
    #For testing in local (aka Docker Desktop) we will create a docker compose spec.
    if local_test:
        local_spec = {
            'services': 
            {
                service_name: 
                {
                    'image': runner_image, 
                    'command': ["python3.9", "-m", "gunicorn", "api:app", "--bind", f"0.0.0.0:{runner_port}", "-w", "6", "-k", "uvicorn.workers.UvicornWorker", "--timeout", "0"],
                    'ports': [f'{runner_port}:{runner_port}'], 
                    'environment': {
                        'AIRFLOW__CORE__LOAD_EXAMPLES': 'False',
                        'AIRFLOW__CORE__ENABLE_XCOM_PICKLING': 'True',
                        'PYTHONUNBUFFERED': 1,
                        'AIRFLOW__CORE__XCOM_BACKEND':'astronomer.providers.snowflake.xcom_backends.localfile.LocalFileXComBackend',
                        'AIRFLOW__CORE__XCOM_LOCALFILE_DIR':'/tmp',
                    }
                    # 'volumes': [f'{os.getcwd()}/include/xcom:/xcom'], 
                }
            }
        }
    
    snowservices_spec = {
        'spec': 
        {
            'container': 
            [
                {
                    'name': service_name, 
                    'image': runner_image,
                    'command': ["python3.9", "-m", "gunicorn", "api:app", "--bind", f"0.0.0.0:{runner_port}", "-w", "6", "-k", "uvicorn.workers.UvicornWorker", "--timeout", "0"],
                    'env': {
                        'AIRFLOW__CORE__XCOM_BACKEND': 'astronomer.providers.snowflake.xcom_backends.localfile.LocalFileXComBackend',
                        'AIRFLOW__CORE__XCOM_LOCALFILE_DIR': '/tmp',
                        'AIRFLOW__CORE__LOAD_EXAMPLES': 'False',
                        'AIRFLOW__CORE__ENABLE_XCOM_PICKLING': 'True',
                        'PYTHONUNBUFFERED': 1,
                    }
                }
            ],
            # 'volume': 
            # [
            #     # {'name': 'xcom', 'source': 'local'},  
            # ]
            'endpoint': 
            [
                {
                    'name': service_name if not runner_endpoint_name else runner_endpoint_name, 
                    'port': runner_port, 
                },    
            ]
        }
    }

    if local_test:
        return {'snowservice': snowservices_spec, 'local': local_spec}
    else:
        return {'snowservice': snowservices_spec}

def get_specs_from_file(spec_file_name:str) -> dict:

    spec_file = Path(spec_file_name)
    
    try: 
        tfile = spec_file.open()
        tfile.close()
    except:
        raise FileExistsError(f"Error opening spec file {spec_file_name}.")

    else:
        service_spec: list = {}
        for doc in yaml.safe_load_all(spec_file.read_text()):
            try:          
                service_spec.update(doc)        
            except:
                raise yaml.YAMLError
                
    return service_spec


def get_specs(self):

    if self.service_type == 'airflow-runner' and not self.spec_file_name:
        services_spec: dict = create_runner_spec(
                                service_name=self.service_name, 
                                runner_endpoint_name=self.runner_endpoint_name or _default_runner_endpoint_name,
                                runner_port=self.runner_port or _default_runner_port,
                                runner_image=self.runner_image_uri or _default_runner_image_uri,
                                local_test=self.local_test,
                            )
    else:
        services_spec: dict = get_specs_from_file(spec_file_name=self.spec_file_name) 
    
    return services_spec

class SnowService():
    def __init__(self, **kwargs) -> None:
        self.service_name = kwargs.get('service_name')
        self.pool_name = kwargs.get('pool_name') or None
        self.service_type = kwargs.get('service_type') or None
        self.runner_endpoint_name = kwargs.get('runner_endpoint_name') or None
        self.runner_port = kwargs.get('runner_port') or None
        self.runner_image_uri = kwargs.get('runner_image_uri') or None
        self.spec_file_name = kwargs.get('spec_file_name') or None
        self.replace_existing = kwargs.get('replace_existing') or False
        self.min_inst = kwargs.get('min_inst') or None
        self.max_inst = kwargs.get('max_inst') or None
        self.local_test = kwargs.get('local_test') or None

        assert self.service_type=='airflow-runner' or self.spec_file_name, "Must specify either service_type='airflow-runner' or provide a spec_file_name."

        if self.local_test != 'astro_cli':
            assert self.pool_name, "Must specify pool_name if not running local_test mode."

        if self.spec_file_name:
            if self.runner_endpoint_name or self.runner_port:
                self.runner_endpoint_name = self.runner_port = None
                warnings.warn('Both spec_file_name and runner_endpoint_name / runner_port parameters provided. endpoint/port will be ignored.')
            
        self.services_spec: dict = get_specs(self)
from __future__ import annotations

import os
from typing import Any, Tuple
import warnings
from pathlib import Path
import tempfile
import yaml
from uuid import uuid4

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from astronomer.providers.snowflake.utils.helpers import SnowService

try:
    from astronomer.providers.snowflake.utils.astro_cli_docker_helpers import (
        docker_compose_up, 
        docker_compose_ps,
        docker_compose_kill,
        docker_compose_pause,
        docker_compose_unpause
    )# noqa
except:
    warnings.warn("The docker-compose package is not installed.")


class SnowServicesHook(SnowflakeHook):
    """
    SnowServices Hook to create and manage Snowservices instances as well as return a SnowService instance endpoint URL.

    :param conn_id: Snowflake connection id
    :type conn_id: str
    :param account: snowflake account name
    :type account: str
    :param warehouse: name of snowflake warehouse
    :type warehouse: str
    :param database: name of snowflake database
    :type database: str
    :param region: name of snowflake region
    :type region: str
    :param role: name of snowflake role
    :type role: str
    :param schema: name of snowflake schema
    :type schema: str
    :param session_parameters: You can set session-level parameters at
        the time you connect to Snowflake
    :type session_parameters: str
    TODO: test authenticator
    :param authenticator: authenticator for Snowflake.
        'snowflake' (default) to use the internal Snowflake authenticator
        'externalbrowser' to authenticate using your web browser and
        Okta, ADFS or any other SAML 2.0-compliant identify provider
        (IdP) that has been defined for your account
        'https://<your_okta_account_name>.okta.com' to authenticate
        through native Okta.
    :type authenticator: str
    :param local_test: Optionally deploy services as local containers for development 
        before deploying. Current options are: 'astro_cli'
    :type local_test: str
    """

    conn_name_attr = "snowflake_conn_id"
    default_conn_name = "snowflake_default"
    conn_type = "snowflake"
    hook_name = "SnowServicesHook"
    instance_types = ['STANDARD_1', 'STANDARD_2', 'STANDARD_3', 'STANDARD_4', 'STANDARD_5']
    gpu_types = ['NVIDIAA10', 'NVIDIATESLAV100', 'NVIDIAAMPEREA100']
    local_modes = ['astro_cli', None]

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.local_test = kwargs.get("local_test") or None

        if self.local_test:
            try:
                Path('/var/run/docker.sock').stat()
            except Exception as e:
                if isinstance(e, FileNotFoundError):
                    raise AttributeError('It looks like you are trying to run SnowServicesHook with local_test mode from a Docker container. To avoid a docker-in-docker inception problem please run local_test mode from non-containerized python.')

        assert self.local_test in self.local_modes, \
            f"Unrecognized option for local_test={self.local_test}.  Current options are: {self.local_modes}."

    def _get_uri_from_conn_params(self) -> str:
        """
        Returns a URI for snowflake connection environment variable.
        conn_params_str = SnowServicesHook()._get_uri_from_conn_params()
        os.environ['AIRFLOW_CONN_SNOWFLAKE_MYCONN'] = conn_params_str
        SnowServicesHook(snowflake_conn_id='SNOWFLAKE_MYCONN').test_connection()
        """

        #TODO: add session parameters and oath options
        conn_params = self._get_conn_params()
        return f"snowflake://{conn_params['user']}:\
                             {conn_params['password']}@/\
                             {conn_params['schema']}\
                             ?account={conn_params['account']}\
                             &region={conn_params['region']}\
                             &database={conn_params['database']}\
                             &warehouse={conn_params['warehouse']}\
                             &role={conn_params['role']}".replace(' ','')
    
    def _get_json_from_conn_params(self) -> str:
        """
        Returns a json object which can be used as an environment variable for snowflake connections.
        example: 
            conn_params_str = SnowServicesHook()._get_json_from_conn_params()
            os.environ['AIRFLOW_CONN_SNOWFLAKE_MYCONN'] = conn_params_str
            SnowServicesHook(snowflake_conn_id='SNOWFLAKE_MYCONN').test_connection()
        """
        conn_params = self._get_conn_params()
        return f'{{\
            "conn_type": "snowflake",\
            "login": "{conn_params["user"]}",\
            "password": "{conn_params["password"]}",\
            "schema": "{conn_params.get("schema", "")}",\
            "extra": {{\
                "account": "{conn_params.get("account", "")}",\
                "database": "{conn_params.get("database", "")}",\
                "region": "{conn_params.get("region", "")}",\
                "warehouse": "{conn_params.get("warehouse", "")}",\
                "role": "{conn_params.get("role", "")}",\
                "authenticator": "{conn_params.get("authenticator", "")}",\
                "private_key_file": "{conn_params.get("private_key_file", "")}",\
                "private_key_content": "{conn_params.get("private_key_content", "")}",\
                "session_parameters": "{conn_params.get("session_parameters", "")}",\
                "insecure_mode": "{conn_params.get("insecure_mode", "")}"\
            }}\
        }}'.replace(' ', '')

    def create_pool(self, 
        pool_name : str, 
        instance_family:str = 'standard_1' , 
        replace_existing = False, 
        min_nodes = 1, 
        max_nodes = 1, 
        gpu_name : str = None):
        """
        Create (or replace an existing) Snowservices compute pool.

        Todo: check instance/gpu for compatibility

        :param pool_name: Name of compute pool to create
        :type pool_name: str
        :param instance_family: Compute node instance family (ie. STANDARD_<1-5>)
        :type instance_family: str
        :param replace_existing: Whether an existing compute pool should be replaced or exit with failure.
        :type replace_existing: bool
        :param min_nodes: The minimum number of nodes for scaling group
        :type min_nodes: int
        :param max_nodes: The maximum number of nodes to scale to
        :type max_nodes: int
        :param gpu_name: Whether to use GPU nodes (ie. NvidiaA10)
        :type gpu_name: str
        """

        if not self.local_test:
            if gpu_name and gpu_name.upper() not in self.gpu_types:
                raise AttributeError(f"Unsupported option {gpu_name} specified for gpu_name.")

            if instance_family and instance_family.upper() not in self.instance_types:
                raise AttributeError(f"Unsupported option {instance_family} specified for instance_family.")

            gpu_option_str = ''
            if gpu_name:
                if instance_family not in ['standard_1', 'standard_2', 'standard_3']:
                    raise AttributeError("Invalid combination of instance_family and gpu_name.")
                else:
                    gpu_option_str = f" GPU_OPTIONS = ( accelerator = {gpu_name} ) "

            replace_existing_str = ' IF NOT EXISTS ' if not replace_existing else ''

            self.run(
                ' '.join(f"CREATE COMPUTE POOL {replace_existing_str} {pool_name} \
                    MIN_NODES = {min_nodes} \
                    MAX_NODES = {max_nodes} \
                    INSTANCE_FAMILY = {instance_family} \
                    {gpu_option_str};".split())
            )

        return pool_name
    
    def remove_pool(self, pool_name:str, force_all=False):
        """
        Remove an existing Snowservices compute pool.
        :param pool_name: Name of compute pool to drop (required)
        :type pool_name: str
        :param force_all: Forcibly delete all existing snowservices before dropping the pool
        :type force_all: bool
        """
        
        if not self.local_test:    
            force_all = 'true' if force_all else 'false'
            self.run(
                ' '.join(f"ALTER SESSION SET COMPUTE_POOL_FORCE_DELETE_ALL_SNOWSERVICES_ON_DROP = {force_all}; \
                    DROP COMPUTE POOL {pool_name};".split())
            )

        
    def list_pools(self, name_prefix:str = None, regex_pattern:str = None, limit:int = None):
        """
        List current Snowservices compute pools

        :param name_prefix: List only pools with names starting with prefix.
        :type name_prefix: str
        :param regex_pattern: Provide a regex string to specify pool names.
        :type regex_pattern: str
        :param limit: Limit returned result to specific number.
        :type limit: int
        """
        ##TODO: Add starts FROM logic

        if not self.local_test:    
            if name_prefix:
                prefix_str = f" STARTS WITH {name_prefix} "

            if regex_pattern: 
                like_str = f" LIKE {regex_pattern} "

            if limit:
                limit_str = f" LIMIT {limit} "

            # response = self.get_conn().cursor().execute(f"SHOW COMPUTE POOLS {like_str} {prefix_str} {limit_str};").fetchall()
            response = self.run(f"SHOW COMPUTE POOLS {like_str} {prefix_str} {limit_str};")
            return response
        else:
            return None

    def create_service(self, 
        service_name : str, 
        pool_name: str = None, 
        service_type: str = None, 
        runner_endpoint_name: str = None,
        runner_port: int = None,
        runner_image_uri: str = None,
        spec_file_name : str = None,
        replace_existing: bool = False, 
        min_inst = 1, 
        max_inst = 1
        ) -> str:
        
        """
        Create (or replace an existing) Snowservice using a build-runner.

        :param service_name: Name of Snowservice to create
        :type service_name: str
        :param pool_name: Compute pool to use for service execution
        :type pool_name: str
        :param service_type: Specify 'airflow-runner' to use defaults for runner.
        :type service_type: str
        :param runner_endpoint_name: Endpoint name for the snowservice airflow runner.  
        If not set use service_name as runner_endpoint_name.
        :type runner_endpoint_name: str
        :param runner_port: Port number (int) for the snowservice runner. Default 8081
        :type runner_port: int
        :param runner_image_uri: Name of Docker image to use for the service.
        :type runner_image_uri: str
        :param replace_existing: Whether an existing service should be replaced or exit with failure.
        :type replace_existing: bool
        :param min_inst: The minimum number of nodes for scaling group
        :type min_inst: int
        :param max_inst: The maximum number of nodes to scale to
        :type max_inst: int
        :param spec_file_name: Optional path to an existing YAML specification for the service
        :type spec_file: str
        """
            
        snowservice = SnowService(
            service_name = service_name, 
            pool_name = pool_name, 
            service_type = service_type, 
            runner_endpoint_name = runner_endpoint_name,
            runner_port = runner_port,
            runner_image_uri = runner_image_uri,
            spec_file_name = spec_file_name,
            replace_existing = replace_existing, 
            min_inst = min_inst, 
            max_inst = max_inst,
            local_test = self.local_test
        )

        if self.local_test == 'astro_cli':

            try:
                local_service_spec = snowservice.services_spec['local']
            except:
                raise AttributeError('Provided spec does not include local docker compose specs.')

            try: 
                
                services = docker_compose_ps(local_service_spec=local_service_spec, status='running')
                
                if len(services) > 0 and not replace_existing:
                    warnings.warn('Services currently running but replace_existing=False.  Not recreating')

                docker_compose_up(local_service_spec=local_service_spec, replace_existing=replace_existing)
            except:
                raise()

            return service_name
            
        else:
            try:
                snowservice_service_spec = snowservice.services_spec['snowservice']
            except:
                raise AttributeError('Provided spec does not include snowservice docker compose specs.')

            with tempfile.NamedTemporaryFile(mode='w+', dir=os.getcwd(), suffix='_spec.yml') as tf:
                temp_spec_file = Path(tf.name)
                spec_string = yaml.dump(snowservice_service_spec, default_flow_style=False)
                _ = temp_spec_file.write_text(spec_string)

                replace_existing_str = ' IF NOT EXISTS ' if not replace_existing else ''
            
                temp_stage_postfix = str(uuid4()).replace('-','_')
                temp_stage_name = f'{service_name}_{temp_stage_postfix}'
                # temp_spec_file_name = f'{temp_stage_name}_spec.yml'                    

                try:
                    self.run(
                        ' '.join(f"CREATE TEMPORARY STAGE {temp_stage_name}; \
                                PUT file://{temp_spec_file.as_posix()} @{temp_stage_name} \
                                    AUTO_COMPRESS = False \
                                    SOURCE_COMPRESSION = NONE; \
                                CREATE SERVICE {replace_existing_str} {service_name} \
                                    MIN_INSTANCES = {min_inst} \
                                    MAX_INSTANCES = {max_inst} \
                                    COMPUTE_POOL = {pool_name} \
                                    SPEC = @{temp_stage_name}/{temp_spec_file.name};".split())
                    )
                except:
                    print()
                    return None
    
            ##TODO: need wait loop or asycn operation to make sure it is up


    def suspend_service(self, service_name:str, service_type: str = None, spec_file_name:str = None):

        if self.local_test == 'astro_cli':

            snowservice = SnowService(
                service_name = service_name, 
                service_type = service_type,
                spec_file_name = spec_file_name,
                local_test = self.local_test,
            )

            try:
                local_service_spec = snowservice.services_spec['local']
            except:
                raise AttributeError('Provided spec does not include local docker compose specs.')

            try: 
                
                services = docker_compose_ps(local_service_spec=local_service_spec, status='running')
                
                if len(services) <= 0:
                    warnings.warn('Services do not appear to be running.')
                else:
                    docker_compose_pause(local_service_spec=local_service_spec)
                    return 'success'

            except:
                return 'failed'
            
        else: 
            try:   
                self.run(f'ALTER SERVICE IF EXISTS {service_name} SUSPEND')
                return 'success'
            except: 
                return 'failed'

    def resume_service(self, service_name:str, service_type: str = None, spec_file_name:str = None):

        if self.local_test == 'astro_cli':

            snowservice = SnowService(
                service_name = service_name, 
                service_type = service_type,
                spec_file_name = spec_file_name,
                local_test = self.local_test,
            )

            try:
                local_service_spec = snowservice.services_spec['local']
            except:
                raise AttributeError('Provided spec does not include local docker compose specs.')

            try: 
                
                services = docker_compose_ps(local_service_spec=local_service_spec, status='paused')
                
                if len(services) <= 0:
                    warnings.warn('Services do not appear to be paused.')
                else:
                    docker_compose_unpause(local_service_spec=local_service_spec)
                    return 'success'
            except:
                return 'failed'

        else: 
            try:
                self.run(f'ALTER SERVICE IF EXISTS {service_name} RESUME')
                return 'success'
            except:
                return 'failed'

    def remove_service(self, service_name:str, service_type: str = None, spec_file_name:str = None):

        if self.local_test == 'astro_cli':

            snowservice = SnowService(
                service_name = service_name, 
                service_type = service_type,
                spec_file_name = spec_file_name,
                local_test = self.local_test,
            )

            try:
                local_service_spec = snowservice.services_spec['local']
            except:
                raise AttributeError('Provided spec does not include local docker compose specs.')

            try: 
                docker_compose_kill(local_service_spec=local_service_spec)
                return 'success'
            except:
                return 'failed'
        
        else:    
            try: 
                self.run(f'DROP SERVICE IF EXISTS {service_name}')
            except: 
                return None

    def describe_service(self, service_name:str, service_type: str = None, spec_file_name:str = None):
        # response = {'pods': {}, 'services': {}, 'deployments': {}}

        if self.local_test == 'astro_cli':
            snowservice = SnowService(
                service_name = service_name, 
                service_type = service_type,
                spec_file_name = spec_file_name,
                local_test = self.local_test,
            )

            try:
                local_service_spec = snowservice.services_spec['local']
            except:
                raise AttributeError('Provided spec does not include local docker compose specs.')

            try: 
                result = docker_compose_ps(local_service_spec=local_service_spec)
                return result
            except:
                return None

        else:  
            try:  
                response = self.get_conn().cursor().execute(f'CALL SYSTEM$GET_SNOWSERVICE_STATUS({service_name}').fetchall()
                # print(f"CALL SYSTEM$GET_SNOWSERVICE_STATUS({service_name}")
                response = {'ingress_url': 'localhost:8001'}
                return response
            except:
                return None
            
    def get_runner_url(self, service_name: str, service_type='airflow-runner', spec_file_name:str = None): 

        if self.local_test == 'astro_cli':

            snowservice = SnowService(
                service_name = service_name, 
                service_type = service_type,
                spec_file_name = spec_file_name,
                local_test = self.local_test,
            )
            local_port = snowservice.services_spec['local']['services'][service_type]['ports'][0].split(':')[0]
            return f'http://host.docker.internal:{local_port}'

        else:    
            try:
                
                #response = self.get_conn().cursor().execute(f'CALL SYSTEM$GET_SNOWSERVICE_STATUS({service_name}').fetchall()
                conn_params = self._get_conn_params()
                response = f"http://{service_name}.{conn_params['schema']}.{conn_params['database']}.snowflakecomputing.internal"
                return response
            except:
                return None
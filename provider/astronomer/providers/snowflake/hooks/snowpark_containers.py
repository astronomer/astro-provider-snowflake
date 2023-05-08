from __future__ import annotations
import json 
import os
from typing import Any, Tuple
import warnings
from pathlib import Path
import tempfile
import yaml
from uuid import uuid4
import requests
from time import sleep

from airflow.exceptions import AirflowException
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from astronomer.providers.snowflake import SnowparkContainerService

try:
    from astronomer.providers.snowflake.utils.astro_cli_docker_helpers import (
        docker_compose_up, 
        docker_compose_ps,
        docker_compose_kill,
        docker_compose_pause,
        docker_compose_unpause
    ) # noqa
except:
    pass


class SnowparkContainersHook(SnowflakeHook):
    """
    Snowpark Container Hook to create and manage Snowpark Container objects.

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
        the time you connect to Snowflake. 
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
    hook_name = "SnowparkContainersHook"
    instance_types = ['STANDARD_1', 'STANDARD_2', 'STANDARD_3', 'STANDARD_4', 'STANDARD_5']
    gpu_types = ['NVIDIAA10', 'NVIDIATESLAV100', 'NVIDIAAMPEREA100']
    local_modes = ['astro_cli', None]

    def __init__(self, *args, **kwargs) -> None:

        #PYTHON_CONNECTOR_QUERY_RESULT_FORMAT should be json for oauth token fetching
        if kwargs.get('session_parameters'):
            assert isinstance(kwargs.get('session_parameters'), dict), "session_parameters in kwargs must be a dictionary"
            kwargs['session_parameters']['PYTHON_CONNECTOR_QUERY_RESULT_FORMAT']='json'
        else:
            kwargs['session_parameters']={'PYTHON_CONNECTOR_QUERY_RESULT_FORMAT':'json'}

        super().__init__(*args, **kwargs)
        self.local_test = kwargs.get("local_test") or None
        self.conn_params = self._get_conn_params()        

        if self.local_test:
            try:
                Path('/var/run/docker.sock').stat()
            except Exception as e:
                if isinstance(e, FileNotFoundError):
                    raise AttributeError('It looks like you are trying to run SnowparkContainersHook with local_test mode from a Docker container. To avoid a docker-in-docker inception problem please run local_test mode from non-containerized python.')

        assert self.local_test in self.local_modes, \
            f"Unrecognized option for local_test={self.local_test}.  Current options are: {self.local_modes}."

    def _get_uri_from_conn_params(self) -> str:
        """
        Returns a URI for snowflake connection environment variable.
        conn_params_str = SnowparkContainersHook()._get_uri_from_conn_params()
        os.environ['AIRFLOW_CONN_SNOWFLAKE_MYCONN'] = conn_params_str
        SnowServicesHook(snowflake_conn_id='SNOWFLAKE_MYCONN').test_connection()
        """

        #TODO: add session parameters and oath options
        return f"snowflake://{self.conn_params['user']}:\
                             {self.conn_params['password']}@/\
                             {self.conn_params['schema']}\
                             ?account={self.conn_params['account']}\
                             &region={self.conn_params['region']}\
                             &database={self.conn_params['database']}\
                             &warehouse={self.conn_params['warehouse']}\
                             &role={self.conn_params['role']}".replace(' ','')
    
    def create_pool(self, 
        pool_name : str, 
        instance_family:str = 'standard_1', 
        replace_existing = False, 
        min_nodes = 1, 
        max_nodes = 1, 
        gpu_name : str = None):
        """
        Create (or replace an existing) Snowpark Container compute pool.

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

            replace_existing_str = 'OR REPLACE COMPUTE POOL' if replace_existing else 'COMPUTE POOL IF NOT EXISTS'

            response = self.get_pandas_df(f"""CREATE {replace_existing_str} {pool_name}
                                                MIN_NODES = {min_nodes} 
                                                MAX_NODES = {max_nodes} 
                                                INSTANCE_FAMILY = {instance_family} 
                                                {gpu_option_str};""")
            
            if 'fail' in response['status'][0]:
                raise AirflowException(f"Create service failed. Status {response['status'][0]}")
            else:
                return pool_name
        else:
            return None

    def suspend_pool(self, pool_name:str):
        """
        Suspend an existing Snowpark Container compute pool.
        :param pool_name: Name of compute pool to suspend (required)
        :type pool_name: str
        """
        
        if not self.local_test:  
            self.run(f"ALTER COMPUTE POOL {pool_name} SUSPEND;")

    def resume_pool(self, pool_name:str):
        """
        Resume an existing, suspended Snowpark Container compute pool.
        :param pool_name: Name of compute pool to resume (required)
        :type pool_name: str
        """
        
        if not self.local_test:  
            self.run(f"ALTER COMPUTE POOL {pool_name} RESUME;")

    def list_pools(self, 
                   name_prefix:str = None, 
                   pool_name:str = None, 
                   limit:int = None
        ) -> dict or None:
        """
        List current Snowpark Container compute pools

        :param name_prefix: List only pools with names starting with prefix.
        :type name_prefix: str
        :param pool_name: Provide a regex string to specify pool names.
        :type pool_name: str
        :param limit: Limit returned result to specific number.
        :type limit: int
        """

        if not self.local_test:
            prefix_str = f" STARTS WITH '{name_prefix.upper()}' " if name_prefix else ''
            like_str = f" LIKE '{pool_name.upper()}' " if pool_name else ''
            limit_str = f" LIMIT {limit} " if limit else ''

            response = self.get_pandas_df(f"SHOW COMPUTE POOLS {like_str} {prefix_str} {limit_str};")
            response = response.set_index('name').to_dict('index')

            return response
        else:
            return None

    def remove_pool(self, pool_name:str):
        """
        Remove an existing Snowpark Container compute pool after all services are removed.
        :param pool_name: Name of compute pool to drop (required)
        :type pool_name: str
        """
        
        if not self.local_test:  

            if self.list_pools(pool_name=pool_name)[pool_name.upper()]['num_services'] > 0:
                raise AirflowException("All services must be stopped before removing a compute pool.")
            else:
                self.run(f"DROP COMPUTE POOL {pool_name};")

    def create_repository(self, 
        repository_name:str, 
        database:str = None, 
        schema:str = None,
        replace_existing = False):
        """
        Create (or replace an existing) Snowpark Container repository.

        :param repository_name: Name of repository
        :type repository_name: str
        :param database: Optional: Database in which to create the repository.
        :type database: str
        :param schema: Optional: Schema in which to create the repository.
        :type schema: str
        :param replace_existing: Whether an existing repository should be replaced.
        :type replace_existing: bool
        """

        if not self.local_test:

            database = database if database else self.database or self.conn_params['database']
            schema = schema if schema else self.schema or self.conn_params['schema']

            assert database and schema, "Database and schema must be set in conn params, hook params or args."

            replace_existing_str = 'OR REPLACE IMAGE REPOSITORY' if replace_existing else 'IMAGE REPOSITORY IF NOT EXISTS'

            response = self.get_pandas_df(f"""CREATE {replace_existing_str} {database}.{schema}.{repository_name};""")
            
            return repository_name
        else:
            return None
    
    def list_repositories(self, 
                          name_prefix:str = None, 
                          repository_name:str = None, 
                          database:str = None, 
                          schema:str = None,
                          limit:int = None
        ) -> dict or None:
        """
        Return a dictionary of existing Snowpark Container repositories.

        :param repository_name: Optinoally provide a regex string to specify repository names.
        :type repository_name: str
        :param database: Optional: Database in which to create the repository.
        :type database: str
        :param schema: Optional: Schema in which to create the repository.
        :type schema: str
        :param name_prefix: Optionally list only repositories with names starting with prefix.
        :type name_prefix: str
        :param limit: Optionally limit returned result to specific number.
        :type limit: int
        """

        if not self.local_test:

            database = database if database else self.database or self.conn_params['database']
            schema = schema if schema else self.schema or self.conn_params['schema']

            assert database and schema, "Database and schema must be set in conn params, hook params or args."

            prefix_str = f" STARTS WITH '{name_prefix.upper()}' " if name_prefix else ''
            like_str = f" LIKE '{repository_name.upper()}' " if repository_name else ''
            limit_str = f" LIMIT {limit} " if limit else ''

            response = self.get_pandas_df(f"SHOW IMAGE REPOSITORIES {like_str} {prefix_str} {limit_str};")
            response = response.set_index('name').to_dict('index')

            return response
        else:
            return None

    def remove_repository(self, 
        repository_name:str, 
        database:str = None, 
        schema:str = None):
        """
        Drop an existing Snowpark Container repository.

        :param repository_name: Name of repository
        :type repository_name: str
        :param database: Optional: Database in which to create the repository.
        :type database: str
        :param schema: Optional: Schema in which to create the repository.
        :type schema: str
        """

        if not self.local_test:

            database = database if database else self.database or self.conn_params['database']
            schema = schema if schema else self.schema or self.conn_params['schema']

            assert database and schema, "Database and schema must be set in conn params, hook params or args."

            response = self.get_pandas_df(f"""DROP IMAGE REPOSITORY {database}.{schema}.{repository_name};""")
            
            return repository_name
        else:
            return None
        
    def create_service(self, 
        service_name : str, 
        spec_file_name : str,
        repository_name: str = None,
        pool_name: str = None, 
        database:str = None, 
        schema:str = None,
        replace_existing: bool = True, 
        min_inst = 1, 
        max_inst = 1
        ) -> str:
        
        """
        Create a SnowparkContainer Service.

        If there is an existing service with the same name it must be removed first.

        :param service_name: Name of SnowparkContainer Service to create
        :type service_name: str
        :param spec_file_name: Path to an existing YAML specification for the service
        :type spec_file: str
        :param repository_name: Name of the Snowpark Container respository where the image has been uploaded. 
            Optional in local mode.
        :type repository_name: str
        :param pool_name: Compute pool to use for service execution
        :type pool_name: str
        :param database: Optional: Database in which to create the repository.
        :type database: str
        :param schema: Optional: Schema in which to create the repository.
        :type schema: str
        :param min_inst: The minimum number of nodes for scaling group. Default: 1
        :type min_inst: int
        :param max_inst: The maximum number of nodes to scale to. Default: 1
        :type max_inst: int
        """
            
        SCService = SnowparkContainerService(
            service_name = service_name, 
            pool_name = pool_name, 
            spec_file_name = spec_file_name,
            replace_existing = replace_existing, 
            min_inst = min_inst, 
            max_inst = max_inst,
            local_test = self.local_test
        )

        services = self.list_services(service_name=service_name, spec_file_name=spec_file_name, status='running')

        if self.local_test == 'astro_cli':

            try:
                local_service_spec = SCService.service_spec['local']
            except:
                raise AttributeError('Provided spec does not include local docker compose specs.')

            try: 
                
                if len(services) > 0 and not replace_existing:
                    warnings.warn('Services currently running but replace_existing=False.  Not recreating')

                docker_compose_up(local_service_spec=local_service_spec, replace_existing=replace_existing)
            except:
                raise()

            return service_name
            
        else:

            assert repository_name, "A repository_name is needed for creating a Snowpark Container service."

            self.database = database if database else self.database or self.conn_params['database']
            self.schema = schema if schema else self.schema or self.conn_params['schema']

            assert database and schema, "Database and schema must be set in conn params, hook params or args."
            
            if services.get(service_name.upper()):
                raise AirflowException(f"Service {service_name.upper()} exists.  The service must be removed before attempting creation.")

            try:
                SCService_service_spec = SCService.service_spec['snowpark_container_service']
            except:
                raise AttributeError('Provided spec does not include Snowpark Container Service specs.')
            
            image_name = SCService_service_spec['spec']['container'][0]['image']

            repository_url = self.list_repositories(repository_name=repository_name)[repository_name.upper()]['repository_url'].split('.')
            repository_url[0] = f"{self.conn_params['account']}.registry"
            repository_url = '.'.join(repository_url)

            SCService_service_spec['spec']['container'][0]['image'] = repository_url+"/"+image_name

            with tempfile.NamedTemporaryFile(mode='w+', dir=os.getcwd(), suffix='_spec.yaml') as tf:
                temp_spec_file = Path(tf.name)
                spec_string = yaml.dump(SCService_service_spec, default_flow_style=False)
                _ = temp_spec_file.write_text(spec_string)
            
                temp_stage_postfix = str(uuid4()).replace('-','_')
                temp_stage_name = f'{service_name}_{temp_stage_postfix}'

                try:
                    self.run(f"""CREATE TEMPORARY STAGE {temp_stage_name};
                                PUT file://{temp_spec_file.as_posix()} @{temp_stage_name}
                                    AUTO_COMPRESS = False 
                                    SOURCE_COMPRESSION = NONE; 
                                CREATE SERVICE {service_name} 
                                    MIN_INSTANCES = {min_inst} 
                                    MAX_INSTANCES = {max_inst} 
                                    COMPUTE_POOL = {pool_name} 
                                    SPEC = @{temp_stage_name}/{temp_spec_file.name};""")
                except Exception as e:
                    raise e
    
            try_count=10
            while try_count >= 0:
                response = self.list_services(service_name=service_name)[service_name.upper()]['service_status']
                if response['status'] != 'READY':
                    print(f"Service created with status {response['status']}. Sleeping for retry.")
                    sleep(1)
                    try_count-=1 
                    if try_count == 0:
                        raise AirflowException(f"Attempted to cretate service {service_name.upper()} but it is '{response['status']}' with message '{response['message']}'")
                else:
                    return service_name



            ##TODO: need wait loop or asycn operation to make sure it is up

    def suspend_service(self, 
                        service_name:str, 
                        spec_file_name:str = None,
                        database:str = None, 
                        schema:str = None,
        ):
        """
        Suspend a running Snowpark Container service.

        :param service_name: Name of service to suspend (required)
        :type service_name: str
        :param spec_file_name: Path to an existing YAML specification for the service
        :type spec_file: str
        :param database: Optional: Database of the running service.
        :type database: str
        :param schema: Optional: Schema of the running service.
        :type schema: str
        """

        if self.local_test == 'astro_cli':

            SCService = SnowparkContainerService(
                service_name = service_name, 
                spec_file_name = spec_file_name,
                local_test = self.local_test,
            )

            try:
                local_service_spec = SCService.service_spec['local']
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
            self.database = database if database else self.database or self.conn_params['database']
            self.schema = schema if schema else self.schema or self.conn_params['schema']
            
            assert database and schema, "Database and schema must be set in conn params, hook params or args."

            try:   
                self.run(f'ALTER SERVICE IF EXISTS {service_name} SUSPEND')
                return 'success'
            except: 
                return 'failed'

    def resume_service(self, 
                       service_name:str, 
                       spec_file_name:str = None,
                       database:str = None, 
                       schema:str = None,
        ):
        """
        Resume a suspend Snowpark Container service.

        :param service_name: Name of service to resume (required)
        :type service_name: str
        :param spec_file_name: Path to an existing YAML specification for the service
        :type spec_file: str
        :param database: Optional: Database of the suspended service.
        :type database: str
        :param schema: Optional: Schema of the suspended service.
        :type schema: str
        """

        if self.local_test == 'astro_cli':

            SCService = SnowparkContainerService(
                service_name = service_name, 
                spec_file_name = spec_file_name,
                local_test = self.local_test,
            )

            try:
                local_service_spec = SCService.service_spec['local']
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
                self.database = database if database else self.database or self.conn_params['database']
                self.schema = schema if schema else self.schema or self.conn_params['schema']

                assert database and schema, "Database and schema must be set in conn params, hook params or args."

                self.run(f'ALTER SERVICE IF EXISTS {service_name} RESUME')
                return 'success'
            except:
                return 'failed'

    def remove_service(self, 
                       service_name:str, 
                       spec_file_name:str = None,
                       database:str = None, 
                       schema:str = None,
        ):
        """
        Remove a Snowpark Container service.

        :param service_name: Name of service to remove (required)
        :type service_name: str
        :param spec_file_name: Path to an existing YAML specification for the service
        :type spec_file: str
        :param database: Optional: Database of the service.
        :type database: str
        :param schema: Optional: Schema of the service.
        :type schema: str
        """

        if self.local_test == 'astro_cli':

            SCService = SnowparkContainerService(
                service_name = service_name, 
                spec_file_name = spec_file_name,
                local_test = self.local_test,
            )

            try:
                local_service_spec = SCService.service_spec['local']
            except:
                raise AttributeError('Provided spec does not include local docker compose specs.')

            try: 
                docker_compose_kill(local_service_spec=local_service_spec)
                return 'success'
            except:
                return 'failed'
        
        else:    
            try: 
                self.database = database if database else self.database or self.conn_params['database']
                self.schema = schema if schema else self.schema or self.conn_params['schema']

                assert database and schema, "Database and schema must be set in conn params, hook params or args."

                self.run(f'DROP SERVICE IF EXISTS {service_name}')
            except: 
                return None

    def list_services(self,
                      service_name:str = None,
                      spec_file_name:str = None, 
                      database:str = None, 
                      schema:str = None,
                      name_prefix:str = None, 
                      limit:int = None,
                      status:str = None
        ) -> dict or None:
        """
        Return a dictionary of existing Snowpark Container services.

        :param service_name: Optionally provide a regex string to specify service names.
        :type service_name: str
        :param spec_file_name: For local_mode a service spec must be provided.
        :type spec_file_name: str
        :param database: Optional: Database of the service.
        :type database: str
        :param schema: Optional: Schema of the service.
        :type schema: str
        :param name_prefix: Optionally list only repositories with names starting with prefix.
        :type name_prefix: str        
        :param limit: Optionally limit returned result to specific number.
        :type limit: int
        """

        if self.local_test == 'astro_cli':
            assert spec_file_name, "Local test mode requires a spec file."
            SCService = SnowparkContainerService(
                service_name = service_name, 
                spec_file_name = spec_file_name,
                local_test = self.local_test,
            )

            try:
                local_service_spec = SCService.service_spec['local']
            except:
                raise AttributeError('Provided spec does not include local docker compose specs.')

            try: 
                response = docker_compose_ps(local_service_spec=local_service_spec, status=status)
                return response
            except:
                return None
        
        else:
            self.database = database if database else self.database or self.conn_params['database']
            self.schema = schema if schema else self.schema or self.conn_params['schema']

            assert database and schema, "Database and schema must be set in conn params, hook params or args."

            prefix_str = f" STARTS WITH '{name_prefix.upper()}' " if name_prefix else ''
            like_str = f" LIKE '{service_name.upper()}' " if service_name else ''
            limit_str = f" LIMIT {limit} " if limit else ''

            services = self.get_pandas_df(f"SHOW SERVICES {like_str} {prefix_str} {limit_str};").set_index('name').to_dict('index')

            for service in services.keys():
                service_status = self.get_pandas_df(f"call system$get_service_status('{service.upper()}');")
                services[service]['service_status'] = json.loads(service_status.iloc[0]['SYSTEM$GET_SERVICE_STATUS'])[0]

            return services
            
    def get_service_url(self, 
                        service_name: str, 
                        spec_file_name:str = None) -> tuple(str, str): 

        if self.local_test == 'astro_cli':
            assert spec_file_name, "Local test mode requires a spec file."

            SCService = SnowparkContainerService(
                service_name = service_name, 
                spec_file_name = spec_file_name,
                local_test = self.local_test,
            )
            local_port = SCService.service_spec['local']['services'][service_name]['ports'][0].split(':')[0]
            url = f'http://host.docker.internal:{local_port}'
            headers = None

        else:    

            service = self.list_services()[service_name.upper()]
            container_name = service['service_status']['containerName']
            endpoint = json.loads(service['public_endpoints'])[container_name]
            
            #need session_parameters PYTHON_CONNECTOR_QUERY_RESULT_FORMAT = json for oauth token fetching
            conn = self.get_conn()
            token_data = conn._rest._token_request('ISSUE')
            conn.close()

            token = f"\"{token_data['data']['sessionToken']}\""
            headers = {'Authorization': f'Snowflake Token={token}'}

            url = f"https://{endpoint}"

            return url, headers
            
            
        return url, headers
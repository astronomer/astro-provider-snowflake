from __future__ import annotations
import json 
import os
from typing import Any, Tuple
import warnings
from pathlib import Path
import tempfile
import yaml
from uuid import uuid4
from time import sleep
import pandas as pd

from airflow.exceptions import AirflowException
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from astronomer.providers.snowflake import SnowparkContainerService

from astronomer.providers.snowflake.utils.astro_cli_docker_helpers import (
        docker_compose_up, 
        docker_compose_ps,
        docker_ps,
        docker_compose_kill,
        docker_compose_pause,
        docker_compose_unpause,
        docker_push,
        docker_ls,
        docker_pull,
        docker_logs,
)


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
        # if kwargs.get('session_parameters'):
        #     assert isinstance(kwargs.get('session_parameters'), dict), "session_parameters in kwargs must be a dictionary"
        #     kwargs['session_parameters']['PYTHON_CONNECTOR_QUERY_RESULT_FORMAT']='json'
        # else:
        #     kwargs['session_parameters']={'PYTHON_CONNECTOR_QUERY_RESULT_FORMAT':'json'}

        super().__init__(*args, **kwargs)
        self.local_test = kwargs.get("local_test") or None
        self.conn_params = self._get_conn_params()        

        # if self.local_test:
        #     try:
        #         Path('/var/run/docker.sock').stat()
        #     except Exception as e:
        #         if isinstance(e, FileNotFoundError):
        #             raise AttributeError('It looks like you are trying to run SnowparkContainersHook with local_test mode from a Docker container. To avoid a docker-in-docker inception problem please run local_test mode from non-containerized python.')

        assert self.local_test in self.local_modes, \
            f"Unrecognized option for local_test={self.local_test}.  Current options are: {self.local_modes}."

    def _get_uri_from_conn_params(self) -> str:
        """
        Returns a URI for snowflake connection environment variable.
        conn_params_str = SnowparkContainersHook()._get_uri_from_conn_params()
        os.environ['AIRFLOW_CONN_SNOWFLAKE_MYCONN'] = conn_params_str
        SnowparkContainersHook(snowflake_conn_id='SNOWFLAKE_MYCONN').test_connection()
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
        Create a Snowpark Container service.

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

        if self.local_test == 'astro_cli':

            local_service_spec = SCService.service_spec['local']

            docker_compose_up(local_service_spec=local_service_spec, replace_existing=replace_existing)
                 
            return service_name
            
        else:

            assert repository_name, "A repository_name is needed for creating a Snowpark Container service."

            self.database = database if database else self.database or self.conn_params['database']
            self.schema = schema if schema else self.schema or self.conn_params['schema']

            assert self.database and self.schema, "Database and schema must be set in conn params, hook params or args."

            repository_url = self.get_repo_url(repository_name=repository_name, database=self.database, schema=self.schema)

            try:
                SCService_service_spec = SCService.service_spec['snowpark_container_service']
            except:
                raise AirflowException('Provided spec does not include Snowpark Container Service specs.')
            
            for container in SCService_service_spec['spec']['container']:
                container['image'] = repository_url+"/"+container['image'].split('/')[-1]
            
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
                    if 'already exists' in e.msg:
                        raise AirflowException(f"Service {service_name.upper()} exists.  The service must be removed before attempting creation.")

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

            assert spec_file_name, "spec_file_name is needed for local_test mode."

            SCService = SnowparkContainerService(
                service_name = service_name, 
                spec_file_name = spec_file_name,
                local_test = self.local_test,
            )

            local_service_spec = SCService.service_spec['local']

            docker_compose_pause(local_service_spec=local_service_spec)
            
            
        else: 
            self.database = database if database else self.database or self.conn_params['database']
            self.schema = schema if schema else self.schema or self.conn_params['schema']
            
            assert self.database and self.schema, "Database and schema must be set in conn params, hook params or args."

            self.run(f'ALTER SERVICE IF EXISTS {service_name} SUSPEND')

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

            assert spec_file_name, "spec_file_name is needed for local_test mode."

            SCService = SnowparkContainerService(
                service_name = service_name, 
                spec_file_name = spec_file_name,
                local_test = self.local_test,
            )

            local_service_spec = SCService.service_spec['local']

            docker_compose_unpause(local_service_spec=local_service_spec)
            

        else: 
            self.database = database if database else self.database or self.conn_params['database']
            self.schema = schema if schema else self.schema or self.conn_params['schema']

            assert self.database and self.schema, "Database and schema must be set in conn params, hook params or args."

            self.run(f'ALTER SERVICE IF EXISTS {service_name} RESUME')
               
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

            assert spec_file_name, "spec_file_name is needed for local_test mode."

            SCService = SnowparkContainerService(
                service_name = service_name, 
                spec_file_name = spec_file_name,
                local_test = self.local_test,
            )

            local_service_spec = SCService.service_spec['local']

            docker_compose_kill(local_service_spec=local_service_spec)
            
        
        else:    
            self.database = database if database else self.database or self.conn_params['database']
            self.schema = schema if schema else self.schema or self.conn_params['schema']

            assert self.database and self.schema, "Database and schema must be set in conn params, hook params or args."

            self.run(f'DROP SERVICE IF EXISTS {service_name}')

    def list_services(self,
                      service_name:str = None,
                      database:str = None,
                      schema:str = None,
                      name_prefix:str = None, 
                      limit:int = None,
                      status:str = None,
                      quiet:bool = False,
        ) -> dict:
        """
        Return a dictionary of existing Snowpark Container services.

        :param service_name: Optionally provide a regex string to specify service names.
        :type service_name: str
        :param database: Optional: Database of the service.
        :type database: str
        :param schema: Optional: Schema of the service.
        :type schema: str
        :param name_prefix: Optionally list only repositories with names starting with prefix.
        :type name_prefix: str        
        :param limit: Optionally limit returned result to specific number.
        :type limit: int
        :param status: Optionally limit status to certain types.
        :type status: str
        :param quiet: Return a subset of data with current status only.
        :type quiet: bool
        """

        services = {}

        if self.local_test == 'astro_cli':
            services = docker_ps(service_name=service_name)     
        
        else:
            self.database = database if database else self.database or self.conn_params['database']
            self.schema = schema if schema else self.schema or self.conn_params['schema']

            assert self.database and self.schema, "Database and schema must be set in conn params, hook params or args."

            prefix_str = f" STARTS WITH '{name_prefix.upper()}' " if name_prefix else ''
            like_str = f" LIKE '{service_name.upper()}' " if service_name else ''
            limit_str = f" LIMIT {limit} " if limit else ''

            services = self.get_pandas_df(f"SHOW SERVICES {like_str} {prefix_str} {limit_str};").set_index('name').to_dict('index')

            for service in services.keys():
                services[service]['public_endpoints'] = json.loads(services[service]['public_endpoints'])
                
                services[service]['service_status'] = {}

                service_status = self.get_first(f"call system$get_service_status('{self.database}.{self.schema}.{service.upper()}', 10);")
                for container in json.loads(service_status[0]):
                    container_name = container.pop('containerName')
                    if container_name not in services[service]['service_status'].keys():
                        services[service]['service_status'].update({container_name: container})

        if quiet:
            status={}
            for service in services.items():
                for container in service[1]['service_status'].items():
                    if service[0] in status.keys():
                        status[service[0]].update({
                            container[0]: {'status': container[1]['status'], 
                                        'message': container[1]['message']}})
                    else:
                        status[service[0]] = {
                            container[0]: {'status': container[1]['status'], 
                                        'message': container[1]['message']}}
            return status 
        else: 
            return services
    
    def get_service_logs(self,
                         service_name:str,
                         spec_file_name:str = None, 
                         database:str = None, 
                         schema:str = None,
        ) -> dict or None:
        """
        Return a string of container logs for existing Snowpark Container services.

        :param service_name: Optionally provide a regex string to specify service names.
        :type service_name: str
        :param spec_file_name: For local_mode a service spec must be provided.
        :type spec_file_name: str
        :param database: Optional: Database of the service.
        :type database: str
        :param schema: Optional: Schema of the service.
        :type schema: str
        """

        logs = {}

        if self.local_test == 'astro_cli':
            
            assert spec_file_name, "spec_file_name is needed for local_test mode."
            
            SCService = SnowparkContainerService(
                service_name = service_name, 
                spec_file_name = spec_file_name,
                local_test = self.local_test,
            )

            local_service_spec = SCService.service_spec['local']

            logs = docker_logs(local_service_spec=local_service_spec)
            
            return logs
                
        else:
            self.database = database if database else self.database or self.conn_params['database']
            self.schema = schema if schema else self.schema or self.conn_params['schema']

            assert self.database and self.schema, "Database and schema must be set in conn params, hook params or args."

            try:
                status = self.list_services(service_name=service_name)[service_name.upper()]['service_status']
            except KeyError:
                raise AirflowException(f"Service '{service_name} is not running.  Cannot fetch logs.")
            
            for container, container_status in status.items():
                instance_id = container_status['instanceId']
                instance_logs = self.get_records(f"CALL SYSTEM$GET_SERVICE_LOGS('{self.database}.{self.schema}.{service_name}', {instance_id}, '{container}');")
                logs[container] = {instance_id: instance_logs[0][0]}

            return logs
            
    def get_service_urls(self, service_name: str) -> tuple(dict, dict): 
        
        service = self.list_services(service_name=service_name)[service_name.upper()]
            
        assert service, f"Service '{service_name}' is not running."

        if self.local_test == 'astro_cli':
            urls = {k: f"http://{v}" for k, v in service['public_endpoints'].items()}
            headers = None

        else:    
            urls = {k: f"https://{v}" for k, v in service['public_endpoints'].items()}
            headers = {'Authorization': f'Snowflake Token="{self.get_conn().rest.token}"'}
            
        return urls, headers
    
    def get_repo_url(self, 
                     repository_name:str, 
                     database:str = None, 
                     schema:str=None):
        
        """
        Returns a properly formated URL for docker login, push, pull, etc.

        Snowpark Containers list repo does not include account information currently.
        
        """

        database = database if database else self.database or self.conn_params['database']
        schema = schema if schema else self.schema or self.conn_params['schema']

        assert database and schema, "Database and schema must be set in conn params, hook params or args."

        account = self.conn_params['account']
        
        assert len(account.split('-')) >= 2, "For Snowpark Container registry operations the account must be listed with '<org_name>-<account_name>.  Account name may contain dashes or underscores."

        repository_url = self.list_repositories(
            repository_name=repository_name,
            database=database,
            schema=schema)[repository_name.upper()]['repository_url']
        #.split('.')
        
        #repository_url[0] = f"{account}".replace('_','-')
        #repository_url = '.'.join(repository_url)   

        return repository_url #.lower()
    
    def push_images(self, 
        service_name : str = None, 
        spec_file_name : str = None,
        repository_name: str = None,
        database:str = None, 
        schema:str = None,
        image_sources:list = []
        ) -> dict:
        
        """
        Pull and push an images to a Snowpark Container Repository.


        :param service_name: Name of SnowparkContainer Service holding the image name.
        :type service_name: str
        :param spec_file_name: Path to an existing YAML specification for the service
        :type spec_file: str
        :param repository_name: Name of the Snowpark Container respository where the image will be pushed. 
            Optional in local mode.
        :type repository_name: str
        :param database: Optional: Database of the repository.
        :type database: str
        :param schema: Optional: Schema of the repository.
        :type schema: str
        """

        database = database if database else self.database or self.conn_params['database']
        schema = schema if schema else self.schema or self.conn_params['schema']

        assert database and schema, "Database and schema must be set in conn params, hook params or args."
            
        SCService = SnowparkContainerService(
            service_name = service_name, 
            pool_name = 'dummy', 
            spec_file_name = spec_file_name,
            local_test = self.local_test
        )

        if self.local_test != 'astro_cli':
            if not image_sources:

                repository_url = self.get_repo_url(repository_name=repository_name, database=database, schema=schema)

                try:
                    image_sources = {}
                    for container in SCService.service_spec['snowpark_container_service']['spec']['container']:
                        image_sources[container['name']] = {'image_source': container['image']}
                except:
                    raise AttributeError('No image source provided and no image found in docker compose specs.')
            else:
                
                temp_sources = {}
                for image_source in image_sources:
                    name = image_source.split('/')[-1].split(':')[0]
                    temp_sources[name]={'image_source': image_source}
                image_sources = temp_sources

            for container in image_sources.keys():
                image_name = image_sources[container]['image_source'].split('/')[-1].split(':')
                image_sources[container]['image_dest'] = f'{repository_url}/{image_name[0]}'
                image_sources[container]['tag'] = image_name[1]

            auth_config = {'username': self.conn_params['user'], 'password': self.conn_params['password']}
            
            for container in image_sources.keys():
                image_source = image_sources[container]['image_source']
                image_dest = image_sources[container]['image_dest']
                image_tag = image_sources[container]['tag']

                image = docker_ls(image_name=image_source)
                if image and image.attrs['Architecture'] == 'amd64':
                    print("Found image in local docker.")
                else:
                    print(f"Pulling image: {image_source}")
                    image = docker_pull(image_source=image_source, platform='linux/amd64')

                print(f"Pushing {image.id} to {image_dest}:{image_tag}")
                image_dest = docker_push(image_source=image, image_dest=image_dest, tag=image_tag, auth_config=auth_config)

            return image_sources
            
        else:
            return None
    
import tempfile
from pathlib import Path
import yaml
import io 
from contextlib import redirect_stdout
from airflow.exceptions import AirflowException
import os
import warnings

try:
    import docker
    from compose.cli.main import TopLevelCommand, project_from_options # noqa
except ImportError:
    warnings.warn(
        "SnowparkContainersHook requires the docker and docker-compose packages \
         for local_test mode. Install with [docker] extras.\
        Note: To avoid docker-in-docker inception problems these functions will \
         not be available to Airflow tasks in a docker container."
    ) # noqa

class ComposeClient():

    def __init__(self, **kwargs) -> None:

        self.local_service_spec = kwargs.get('local_service_spec')
        
        try:
            astro_config = yaml.safe_load(Path('.astro/config.yaml').read_text())
            self.project_name = astro_config['project']['name']
            self.project_dir = os.getcwd()
        except Exception as e:
            raise AttributeError('Could not open astro cli config.  Make sure you are running from a astro dev init environment.')

    def __enter__(self):

        self.tf = tempfile.NamedTemporaryFile(mode='w+', dir=self.project_dir)

        self.temp_spec_file = Path(self.tf.name)
        spec_string = yaml.dump(self.local_service_spec, default_flow_style=False)
        _ = self.temp_spec_file.write_text(spec_string)

        self.docker_compose_options = {
            "--file": [self.temp_spec_file.as_posix()],
            "--project-name": self.project_name,
            "SERVICE": "",
        }
        self.project = project_from_options(project_dir = self.temp_spec_file.parent.as_posix(), 
                                            options=self.docker_compose_options)
        self.cmd = TopLevelCommand(project=self.project)

        return self

    def __exit__(self, type, value, traceback):
        self.tf.close()

def docker_compose_up(local_service_spec:dict, replace_existing=False):

    with ComposeClient(local_service_spec=local_service_spec) as client:      
        client.docker_compose_options.update({
            "--no-deps": False,
            "--abort-on-container-exit": False,
            "--remove-orphans": False,
            "--no-recreate": not replace_existing,
            "--force-recreate": replace_existing,
            "--build": False,
            "--no-build": False,
            "--always-recreate-deps": False,
            "--scale": [],
            "--detach": True,
        })

        client.cmd.up(client.docker_compose_options)

def docker_ps(service_name:str = None) -> dict:

    try: 
        client = docker.from_env()

        astro_config = yaml.safe_load(Path('.astro/config.yaml').read_text())

        containers = client.containers.list()
        running_containers={}
        for container in containers:

            project_name = container.name.split('_')[0]

            if project_name == astro_config['project']['name'] and 'io.astronomer.docker' not in container.attrs['Config']['Labels'].keys():
                container_name = ''.join(container.name.split('_')[1:-1])
                running_service_name = container.attrs['Config']['Labels']['service_name'].upper()
                extra_host_name = container.attrs['HostConfig']['ExtraHosts'][0].split(':')[0]
                port_names = container.attrs['Config']['Labels']['port_names'].split(',')
                port_numbers = [port.split('/')[0] for port in list(container.attrs['Config']['ExposedPorts'])]

                assert len(port_names) == len(port_numbers), "Number of ports and port names differ in docker spec. Specify 'port_names' as a comma-separated"

                assert running_service_name, "Service name must be listed as a 'label' in docker compose spec."

                if running_service_name not in running_containers.keys():
                    running_containers[running_service_name] = {
                            'database_name': '', 
                            'schema_name': '', 
                            'owner': '', 
                            'compute_pool': '', 
                            'spec': '',
                            'dns_name': '', 
                            'public_endpoints': {}, 
                            'min_instances': '', 
                            'max_instances': '', 
                            'created_on': container.attrs['Created'], 
                            'service_status': {}
                    }
                for port_name, port_number in zip(port_names, port_numbers):
                    if port_name not in running_containers[running_service_name]['public_endpoints'].keys():
                        running_containers[running_service_name]['public_endpoints'].update({port_name: f'{extra_host_name}:{port_number}'})

                    if container_name not in running_containers[running_service_name]['service_status'].keys():
                        running_containers[running_service_name]['service_status'].update({
                            container_name: {'status': container.status,
                                            'message': '',
                                            'instanceId': str(int(container.name.split('_')[2])-1),
                                            'serviceName': running_service_name,
                                            'image': container.attrs['Config']['Image'],
                                            'restartCount': container.attrs['RestartCount']
                                }
                        })
        
        if service_name:
            try: 
                return {service_name.upper(): running_containers[service_name.upper()]}
            except:
                return {}
        else:
            return running_containers

    except NameError:
        print('Docker is not installed.')
        return {}

def docker_compose_ps(local_service_spec:dict, status:str = None) -> list:

    with ComposeClient(local_service_spec=local_service_spec) as client, \
        io.StringIO() as stdout, \
            redirect_stdout(stdout):

        client.docker_compose_options.update({
            "--quiet": False,
            "--services": True,
            "--all": True,
        })
        if status:
            client.docker_compose_options.update({"--filter": 'status='+status})

        client.cmd.ps(client.docker_compose_options)
        stdout_val = stdout.getvalue()

        return [val for val in stdout_val.split('\n') if len(val) > 0]
        
def docker_compose_pause(local_service_spec:dict):

    with ComposeClient(local_service_spec=local_service_spec) as client:      
        client.cmd.pause(client.docker_compose_options)

def docker_compose_unpause(local_service_spec:dict):

    with ComposeClient(local_service_spec=local_service_spec) as client:      
        client.cmd.unpause(client.docker_compose_options)

def docker_compose_kill(local_service_spec:dict):

    with ComposeClient(local_service_spec=local_service_spec) as client:
        client.cmd.kill(client.docker_compose_options)

def docker_ls(image_name:str) -> bool:

    client = docker.from_env()
    try:        
        return client.images.get(image_name)
    except:
        return False

def docker_pull(image_source:str, platform='linux/amd64') -> str:

    client = docker.from_env()
    image = client.images.pull(repository=image_source, platform=platform)

    return image

def docker_push(image_source:str, image_dest:str, tag:str, auth_config:dict) -> str:

    client = docker.from_env()

    if isinstance(image_source, str):
        assert client.images.list(image_source), "The image does not exist in the local repository."
        image = client.images.get(image_source)
    else:
        image = image_source

    assert image.attrs.get('Architecture') == 'amd64'

    image.tag(repository=image_dest, tag=tag)
    
    client.images.push(repository=image_dest, tag=tag, auth_config=auth_config)

def docker_logs(local_service_spec:dict) -> dict:

    client = docker.from_env()

    compose_client = ComposeClient(local_service_spec=local_service_spec)
    service_conatiner_names = docker_compose_ps(local_service_spec=local_service_spec)
    running_containers = client.containers.list()

    logs= {} 

    for container_name in service_conatiner_names:
        for running_container in running_containers:
            if f"{compose_client.project_name}_{container_name}" in running_container.name:
                instance_id = str(int(running_container.name.split('_')[2])-1)
                logs[container_name] = {instance_id: running_container.logs().decode()}
    
    return logs

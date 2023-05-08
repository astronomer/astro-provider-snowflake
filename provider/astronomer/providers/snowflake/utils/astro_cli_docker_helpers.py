import tempfile
from pathlib import Path
import yaml
import io 
from contextlib import redirect_stdout
from airflow.exceptions import AirflowException
import os

try:
    from compose.cli.main import TopLevelCommand, project_from_options # noqa
except ImportError:
    raise AirflowException(
        "The local_test mode for SnowparkContainersHook requires the docker-compose package. Install with [docker] extras."
    )

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
        self.project = project_from_options(project_dir = self.temp_spec_file.parent.as_posix(), options=self.docker_compose_options)
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

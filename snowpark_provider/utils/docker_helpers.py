try:
    import docker
except ImportError:
    pass
    print(
        "SnowparkContainersHook requires the docker and docker-compose packages \
         for local_test mode. Install with [docker] extras.\
        Note: To avoid docker-in-docker inception problems these functions will \
         not be available to Airflow tasks in a docker container."
    )

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
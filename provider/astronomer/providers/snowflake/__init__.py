from __future__ import annotations
from attr import define, field
import warnings
from pathlib import Path
import yaml

"""Description of the package"""

__version__ = "0.0.1-dev"

def get_provider_info():
    return {
        "package-name": "astro-provider-snowflake", 
        "name": "SnowparkContainers and Snowpark Airflow Provider", 
        "description": "Decorator providers for SnowparkContainers and Snowpark.", 
        "hook-class-names": [
            "astronomer.providers.snowflake.hooks.snowpark_containers.SnowparkContainersHook",
            ],
        "extra-links": [
            "astronomer.providers.snowflake.decorators.snowpark_containers.snowpark_containers_python",
            "astronomer.providers.snowflake.decorators.snowpark.dataframe_decorator"
        ],
        "versions": ["0.0.1-dev"],
    }

@define
class Metadata:
    schema: str | None = None
    database: str | None = None


@define(slots=False)
class SnowparkTable:
    """
    This class allows the Snowpark operators and decorators to create instances of Snowpark Dataframes 
    for any arguments passed to the python callable.

    It is a slim version of the Astro Python SDK Table class.  Therefore users can pass either astro.sql.table.Table or 
    astronomer.providers.snowflake.SnowparkTable objects as arguments interchangeably. 
    
    """

    template_fields = ("name",)
    name: str = field(default="")
    conn_id: str = field(default="")

    # Setting converter allows passing a dictionary to metadata arg
    metadata: Metadata = field(
        factory=Metadata,
        converter=lambda val: Metadata(**val) if isinstance(val, dict) else val,
    )

    # We need this method to pickle SnowparkTable object, without this we cannot push/pull this object from xcom.
    def __getstate__(self):
        return self.__dict__

    def to_json(self):
        return {
            "class": "SnowparkTable",
            "name": self.name,
            "metadata": {
                "schema": self.metadata.schema,
                "database": self.metadata.database,
            },
            "conn_id": self.conn_id,
        }

    @classmethod
    def from_json(cls, obj: dict):
        return SnowparkTable(
            name=obj["name"],
            metadata=Metadata(**obj["metadata"]),
            conn_id=obj["conn_id"],
        )

class SnowparkContainerService():
    def __init__(self, **kwargs) -> None:
        self.service_name = kwargs.get('service_name')
        self.pool_name = kwargs.get('pool_name') or None
        self.spec_file_name = kwargs.get('spec_file_name') or None
        self.replace_existing = kwargs.get('replace_existing') or False
        self.min_inst = kwargs.get('min_inst') or None
        self.max_inst = kwargs.get('max_inst') or None
        self.local_test = kwargs.get('local_test') or None

        if self.local_test != 'astro_cli':
            assert self.pool_name, "Must specify pool_name if not running local_test mode."
    
        self.service_spec: dict = self.get_specs_from_file(self)

    @staticmethod
    def get_specs_from_file(self) -> dict:

        spec_file = Path(self.spec_file_name)
        
        try: 
            _ = spec_file.read_text()
        except:
            raise FileExistsError(f"Spec file {self.spec_file_name} does not exist or is not a readable file.")

        else:
            service_spec: list = {}
            for doc in yaml.safe_load_all(spec_file.read_text()):
                try:          
                    service_spec.update(doc)        
                except:
                    raise yaml.YAMLError
                    
        return service_spec
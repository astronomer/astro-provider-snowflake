from __future__ import annotations
from attr import define, field

"""Description of the package"""

__version__ = "0.0.1"

def get_provider_info():
    return {
        "package-name": "astro-provider-snowflake",  # Required
        "name": "SnowServices and Snowpark Airflow Provider",  # Required
        "description": "Decorator providers for SnowServices and Snowpark.",  # Required
        "hook-class-names": [
            "astronomer.providers.snowflake.hooks.snowservices.SnowServicesHook",
            "astronomer.providers.snowflake.hooks.snowpark.SnowparkHook"],
        "extra-links": [
            "astronomer.providers.snowflake.decorators.snowservices.snowservices_python",
            "astronomer.providers.snowflake.decorators.snowpark.dataframe_decorator"
        ],
        "versions": ["0.0.1"],  # Required
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

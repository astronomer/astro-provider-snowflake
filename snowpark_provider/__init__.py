from __future__ import annotations

from typing import Any
from attr import define, field

__version__ = "0.0.0"


def get_provider_info():
    return {
        "package-name": "astro-provider-snowpark",
        "name": "astro-provider-snowpark",
        "description": "Snowpark Decorators.",
        "task-decorators": [
            {
                "name": "snowpark_python",
                "class-name": "snowpark_provider.decorators.snowpark.snowpark_python_task",
            },
            {
                "name": "snowpark_virtualenv",
                "class-name": "snowpark_provider.decorators.snowpark.snowpark_virtualenv_task",
            },
            {
                "name": "snowpark_ext_python",
                "class-name": "snowpark_provider.decorators.snowpark.snowpark_ext_python_task",
            },
            {
                "name": "snowpark_containers_python",
                "class-name": "snowpark_provider.decorators.snowpark.snowpark_containers_python_task",
            },
        ],
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
    snowpark_provider.utils.SnowparkTable objects as arguments interchangeably.

    """

    template_fields = ("name",)
    name: str = field(default="")
    uri: str = field(default="")
    extra: dict | None = field(default="")
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
            "uri": self.uri,
            "extra": self.extra,
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
            uri=obj["uri"],
            extra=obj["extra"],
            metadata=Metadata(**obj["metadata"]),
            conn_id=obj["conn_id"],
        )

    def serialize(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "uri": self.uri,
            "extra": self.extra,
            "conn_id": self.conn_id,
            "metadata": {
                "schema": self.metadata.schema,
                "database": self.metadata.database,
            },
        }

    @staticmethod
    def deserialize(data: dict[str, Any], version: int):
        return SnowparkTable(
            name=data["name"],
            uri=data["uri"],
            extra=data["extra"],
            conn_id=data["conn_id"],
            metadata=Metadata(**data["metadata"]),
        )

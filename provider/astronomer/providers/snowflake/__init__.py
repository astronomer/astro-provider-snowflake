from __future__ import annotations
from attr import define, field, fields_dict

"""Description of the package"""

__version__ = "0.0.1"


## This is needed to allow Airflow to pick up specific metadata fields it needs for certain features. We recognize it's a bit unclean to define these in multiple places, but at this point it's the only workaround if you'd like your custom conn type to show up in the Airflow UI.
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

@define
class SnowparkTable():

    name: str = field()
    metadata: Metadata = field(
        factory=Metadata,
        converter=lambda val: Metadata(**val) if isinstance(val, dict) else val,
    )
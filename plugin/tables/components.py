from datetime import date
from typing import Any, Generator

import pyarrow as pa
from cloudquery.sdk.scheduler import TableResolver
from cloudquery.sdk.schema import Column
from cloudquery.sdk.schema import Table
from cloudquery.sdk.schema.resource import Resource
from cloudquery.sdk.types import UUIDType, JSONType

from plugin.client import Client


class Components(Table):
    def __init__(self) -> None:
        super().__init__(
            name="pb_components",
            title="ProductBoard Components",
            columns=[
                Column("id", UUIDType(), primary_key=True),
                Column("name", pa.string()),
                Column("description", pa.string()),
                Column("links", JSONType()),
                Column("parent", JSONType()),
                Column("owner", JSONType()),
                Column("created_at", pa.timestamp(unit="s")),
                Column("updated_at", pa.timestamp(unit="s")),
            ],
        )
        self._resolver = ComponentResolver(table=self)

    @property
    def resolver(self):
        return self._resolver


def get_component(component: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": component["id"],
        "name": component["name"],
        "description": component["description"],
        "links": component["links"],
        "parent": component["parent"],
        "owner": component["owner"],
        "created_at": component["createdAt"],
        "updated_at": component["updatedAt"],
    }

class ComponentResolver(TableResolver):
    def __init__(self, table) -> None:
        super().__init__(table=table)

    def resolve(
        self, client: Client, parent_resource: Resource
    ) -> Generator[Any, None, None]:
        for c in client.client.component_iterator():
            yield get_component(c)

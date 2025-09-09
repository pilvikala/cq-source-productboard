from datetime import date
from typing import Any, Generator

import pyarrow as pa
from cloudquery.sdk.scheduler import TableResolver
from cloudquery.sdk.schema import Column
from cloudquery.sdk.schema import Table
from cloudquery.sdk.schema.resource import Resource
from cloudquery.sdk.types import UUIDType, JSONType

from plugin.client import Client


class Features(Table):
    def __init__(self) -> None:
        super().__init__(
            name="pb_features",
            title="ProductBoard Features",
            columns=[
                Column("_cq_id", UUIDType(), primary_key=True),
                Column("id", UUIDType()),
                Column("name", pa.string()),
                Column("description", pa.string()),
                Column("type", pa.string()),
                Column("status", JSONType()),
                Column("parent", JSONType()),
                Column("links", JSONType()),
                Column("archived", pa.bool_()),
                Column("timeframe", JSONType()),
                Column("owner", JSONType()),
                Column("created_at", pa.timestamp(unit="s")),
                Column("updated_at", pa.timestamp(unit="s")),
                Column("lastHealthUpdate", pa.timestamp(unit="s")),
            ],
        )
        self._resolver = FeatureResolver(table=self)

    @property
    def resolver(self):
        return self._resolver


def get_feature(feature: dict[str, Any]) -> dict[str, Any]:
    return {
        "_cq_id": str(feature["id"]),
        "id": feature["id"],
        "name": feature["name"],
        "description": feature["description"],
        "type": feature["type"],
        "status": feature["status"],
        "parent": feature["parent"],
        "links": feature["links"],
        "archived": feature["archived"],
        "timeframe": feature["timeframe"],
        "owner": feature["owner"],
        "created_at": feature["createdAt"],
        "updated_at": feature["updatedAt"],
        "lastHealthUpdate": feature["lastHealthUpdate"]
    }

class FeatureResolver(TableResolver):
    def __init__(self, table) -> None:
        super().__init__(table=table)

    def resolve(
        self, client: Client, parent_resource: Resource
    ) -> Generator[Any, None, None]:
        for c in client.client.feature_iterator():
            yield get_feature(c)

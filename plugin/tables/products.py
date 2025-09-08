from datetime import date
from typing import Any, Generator

import pyarrow as pa
from cloudquery.sdk.scheduler import TableResolver
from cloudquery.sdk.schema import Column
from cloudquery.sdk.schema import Table
from cloudquery.sdk.schema.resource import Resource
from cloudquery.sdk.types import UUIDType, JSONType

from plugin.client import Client


class Products(Table):
    def __init__(self) -> None:
        super().__init__(
            name="pb_products",
            title="ProductBoard Products",
            columns=[
                Column("id", UUIDType(), primary_key=True),
                Column("name", pa.string()),
                Column("description", pa.string()),
                Column("links", JSONType()),
                Column("owner", JSONType()),
                Column("created_at", pa.timestamp(unit="s")),
                Column("updated_at", pa.timestamp(unit="s")),
            ],
        )
        self._resolver = ProductResolver(table=self)

    @property
    def resolver(self):
        return self._resolver


def get_product(product: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": product["id"],
        "name": product["name"],
        "description": product["description"],
        "links": product["links"],
        "owner": product["owner"],
        "created_at": product["createdAt"],
        "updated_at": product["updatedAt"],
    }

class ProductResolver(TableResolver):
    def __init__(self, table) -> None:
        super().__init__(table=table)

    def resolve(
        self, client: Client, parent_resource: Resource
    ) -> Generator[Any, None, None]:
        for c in client.client.product_iterator():
            yield get_product(c)

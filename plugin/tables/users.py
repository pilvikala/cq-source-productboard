from datetime import date
from typing import Any, Generator

import pyarrow as pa
from cloudquery.sdk.scheduler import TableResolver
from cloudquery.sdk.schema import Column
from cloudquery.sdk.schema import Table
from cloudquery.sdk.schema.resource import Resource
from cloudquery.sdk.types import UUIDType, JSONType

from plugin.client import Client


class Users(Table):
    def __init__(self) -> None:
        super().__init__(
            name="pb_users",
            title="ProductBoard Users",
            columns=[
                Column("id", UUIDType(), primary_key=True),
                Column("email", pa.string()),
                Column("external_id", pa.string()),
                Column("name", pa.string()),
            ],
        )
        self._resolver = UserResolver(table=self)

    @property
    def resolver(self):
        return self._resolver


def get_user(user: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": user["id"],
        "email": user["email"],
        "name": user["name"],
        "external_id": user["externalId"]
    }

class UserResolver(TableResolver):
    def __init__(self, table) -> None:
        super().__init__(table=table)

    def resolve(
        self, client: Client, parent_resource: Resource
    ) -> Generator[Any, None, None]:
        for c in client.client.user_iterator():
            yield get_user(c)

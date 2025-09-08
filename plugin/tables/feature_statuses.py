from datetime import date
from typing import Any, Generator

import pyarrow as pa
from cloudquery.sdk.scheduler import TableResolver
from cloudquery.sdk.schema import Column
from cloudquery.sdk.schema import Table
from cloudquery.sdk.schema.resource import Resource
from cloudquery.sdk.types import UUIDType, JSONType

from plugin.client import Client


class FeatureStatuses(Table):
    def __init__(self) -> None:
        super().__init__(
            name="pb_feature_statuses",
            title="ProductBoard Feature Statuses",
            columns=[
                Column("id", UUIDType(), primary_key=True),
                Column("name", pa.string()),
                Column("completed", pa.bool_()),
            ],
        )
        self._resolver = FeatureStatusResolver(table=self)

    @property
    def resolver(self):
        return self._resolver


def get_feature_status(feature_status: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": feature_status["id"],
        "name": feature_status["name"],
        "completed": feature_status["completed"],
    }

class FeatureStatusResolver(TableResolver):
    def __init__(self, table) -> None:
        super().__init__(table=table)

    def resolve(
        self, client: Client, parent_resource: Resource
    ) -> Generator[Any, None, None]:
        for c in client.client.feature_status_iterator():
            yield get_feature_status(c)

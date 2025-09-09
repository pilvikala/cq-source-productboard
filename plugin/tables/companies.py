from datetime import date
from typing import Any, Generator

import pyarrow as pa
from cloudquery.sdk.scheduler import TableResolver
from cloudquery.sdk.schema import Column
from cloudquery.sdk.schema import Table
from cloudquery.sdk.schema.resource import Resource
from cloudquery.sdk.types import UUIDType

from plugin.client import Client


class Companies(Table):
    def __init__(self) -> None:
        super().__init__(
            name="pb_companies",
            title="ProductBoard Companies",
            columns=[
                Column("_cq_id", UUIDType(), primary_key=True),
                Column("id", UUIDType()),
                Column("name", pa.string()),
                Column("domain", pa.string()),
                Column("description", pa.string()),
                Column("source_origin", pa.string()),
                Column("source_record_id", pa.string()),
            ],
        )
        self._resolver = CompanyResolver(table=self)

    @property
    def resolver(self):
        return self._resolver


def get_company(company: dict[str, Any]) -> dict[str, Any]:
    return {
        "_cq_id": str(company["id"]),
        "id": company["id"],
        "name": company["name"],
        "domain": company["domain"],
        "description": company["description"],
        "source_origin": company["sourceOrigin"],
        "source_record_id": company["sourceRecordId"],
    }

class CompanyResolver(TableResolver):
    def __init__(self, table) -> None:
        super().__init__(table=table)

    def resolve(
        self, client: Client, parent_resource: Resource
    ) -> Generator[Any, None, None]:
        for c in client.client.company_iterator():
            yield get_company(c)

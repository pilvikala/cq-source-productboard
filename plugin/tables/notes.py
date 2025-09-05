from datetime import date
from typing import Any, Generator

import pyarrow as pa
from cloudquery.sdk.scheduler import TableResolver
from cloudquery.sdk.schema import Column
from cloudquery.sdk.schema import Table
from cloudquery.sdk.schema.resource import Resource
from cloudquery.sdk.types import JSONType, UUIDType

from plugin.client import Client


class Notes(Table):
    def __init__(self) -> None:
        super().__init__(
            name="pb_notes",
            title="ProductBoard Notes",
            columns=[
                Column("id", UUIDType(), primary_key=True),
                Column("title", pa.string()),
                Column("content", pa.string()),
                Column("display_url", pa.string()),
                Column("external_display_url", pa.string()),
                Column("company", JSONType()),
                Column("user", JSONType()),
                Column("owner", JSONType()),
                Column("followers", JSONType()),
                Column("state", pa.string()),
                Column("source", JSONType()),
                Column("tags", pa.string()),
                Column("features", JSONType()),
                Column("created_at", pa.timestamp(unit="s")),
                Column("updated_at", pa.timestamp(unit="s")),
                Column("createdBy", JSONType()),
            ],
        )
        self._resolver = NoteResolver(table=self)

    @property
    def resolver(self):
        return self._resolver


def get_note(note_response: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": note_response["id"],
        "title": note_response["title"],
        "content": note_response["content"],
        "display_url": note_response["displayUrl"],
    }

class NoteResolver(TableResolver):
    def __init__(self, table) -> None:
        super().__init__(table=table)

    def resolve(
        self, client: Client, parent_resource: Resource
    ) -> Generator[Any, None, None]:
        for note_response in client.client.note_iterator():
            yield get_note(note_response)

from dataclasses import dataclass, field
from cloudquery.sdk.scheduler import Client as ClientABC

from plugin.productboard.client import ProductBoardClient

DEFAULT_CONCURRENCY = 100
DEFAULT_QUEUE_SIZE = 10000


@dataclass
class Spec:
    api_token: str
    concurrency: int = field(default=DEFAULT_CONCURRENCY)
    queue_size: int = field(default=DEFAULT_QUEUE_SIZE)

    def validate(self):
        pass
        # if self.access_token is None:
        #     raise Exception("access_token must be provided")


class Client(ClientABC):
    def __init__(self, spec: Spec) -> None:
        self._spec = spec
        self._client = ProductBoardClient(spec.api_token)

    def id(self):
        return "productboard"

    @property
    def client(self) -> ProductBoardClient:
        return self._client

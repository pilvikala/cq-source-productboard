from typing import Generator, Dict, Any
import requests

class ProductBoardClient:
    def __init__(self, api_token):
        self._api_token = api_token
        self._base_url = "https://api.productboard.com"

    def get_page_with_notes(self, page_cursor: str = None) -> Dict[str, Any]:
        url = f"{self._base_url}/notes?pageCursor={page_cursor if page_cursor else ''}"
        headers = {
            "Authorization": f"Bearer {self._api_token}",
            "X-Version": "1",
            "accept": "application/json"
        }
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            raise Exception(f"Failed to list notes{response.text}")
        json_response = response.json()
        return json_response["data"], json_response["pageCursor"]

    def note_iterator(self, page_cursor: str = None) -> Generator[Dict[str, Any], None, None]:
        page, page_cursor = self.get_page_with_notes(page_cursor)
        while page_cursor is not None:
            for note in page:
                yield note
            page, page_cursor = self.get_page_with_notes(page_cursor)

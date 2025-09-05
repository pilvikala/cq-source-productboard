from typing import Generator, Dict, Any
import requests

class ProductBoardClient:
    def __init__(self, api_token):
        self._api_token = api_token
        self._base_url = "https://api.productboard.com"

    def get_cursor(self, response: dict[str, Any]) -> str:
        if "pageCursor" in response:
            return response["pageCursor"]
        if "links" in response and "next" in response["links"] and response["links"]["next"] is not None:
            return response["links"]["next"].split("pageCursor=")[1]
        return None

    def get_page(self, path: str, page_cursor: str = None) -> Dict[str, Any]:
        url = f"{self._base_url}/{path}?pageCursor={page_cursor if page_cursor else ''}"
        headers = {
            "Authorization": f"Bearer {self._api_token}",
            "X-Version": "1",
            "accept": "application/json"
        }
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            raise Exception(f"Failed to list {path}{response.text}")
        json_response = response.json()
        cursor = self.get_cursor(json_response)
        return json_response["data"], cursor
    
    def company_iterator(self, page_cursor: str = None) -> Generator[Dict[str, Any], None, None]:
        while True:
            page, page_cursor = self.get_page('companies',page_cursor)
            for company in page:
                yield company
            if page_cursor is None:
                break

    def note_iterator(self, page_cursor: str = None) -> Generator[Dict[str, Any], None, None]:
        while True:
            page, page_cursor = self.get_page('notes', page_cursor)
            for note in page:
                yield note
            if page_cursor is None:
                break

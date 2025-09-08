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

    def _get(self, url: str) -> Dict[str, Any]:
        headers = {
            "Authorization": f"Bearer {self._api_token}",
            "X-Version": "1",
            "accept": "application/json"
        }
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            raise Exception(f"Failed to list {url}{response.text}")
        return response.json()

    def get_page(self, path: str, page_cursor: str = None) -> Dict[str, Any]:
        url = f"{self._base_url}/{path}?pageCursor={page_cursor if page_cursor else ''}"
        json_response = self._get(url)
        cursor = self.get_cursor(json_response)
        return json_response["data"], cursor
    
    def get_page_from_url(self, url: str) -> Dict[str, Any]:
        json_response = self._get(url)
        next_url = None
        if "links" in json_response and "next" in json_response["links"] and json_response["links"]["next"] is not None:
            next_url = json_response["links"]["next"]
        return json_response["data"], next_url

    def iterator(self, path: str, page_cursor: str = None) -> Generator[Dict[str, Any], None, None]:
        while True:
            page, page_cursor = self.get_page(path, page_cursor)
            for item in page:
                yield item
            if page_cursor is None:
                break
    
    
    def company_iterator(self, page_cursor: str = None) -> Generator[Dict[str, Any], None, None]:
        return self.iterator('companies', page_cursor)

    def note_iterator(self, page_cursor: str = None) -> Generator[Dict[str, Any], None, None]:
        return self.iterator('notes', page_cursor)

    def feature_iterator(self) -> Generator[Dict[str, Any], None, None]:
        return self.url_iterator('features')

    def user_iterator(self, page_cursor: str = None) -> Generator[Dict[str, Any], None, None]:
        return self.iterator('users', page_cursor)

    def component_iterator(self) -> Generator[Dict[str, Any], None, None]:
        return self.url_iterator('components')
    
    def url_iterator(self, path: str) -> Generator[Dict[str, Any], None, None]:
        next_url = f"{self._base_url}/{path}"
        while True:
            page, next_url = self.get_page_from_url(next_url)
            for item in page:
                yield item
            if next_url is None:
                break
    
    def product_iterator(self) -> Generator[Dict[str, Any], None, None]:
        return self.url_iterator('products')
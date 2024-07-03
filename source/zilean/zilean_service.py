import requests

from utils.logger import setup_logger
from constants import DMM_SEARCH_API_URL

logger = setup_logger(__name__)


class ZileanService:
    def __init__(self, config, base_url=DMM_SEARCH_API_URL):
        self.base_url = base_url
        self.search_endpoint = "/dmm/search"
        self.session = requests.Session()

    def search(self, media):
        url = self.base_url + self.search_endpoint
        headers = {"Content-Type": "application/json"}
        payload = {"queryText": media.titles[0]}

        try:
            response = self.session.post(url, json=payload, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.warning(f"Une erreur s'est produite lors de la requête : {e}")
            return None
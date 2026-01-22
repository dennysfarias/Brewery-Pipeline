import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging
import time

class OpenBreweryClient:
    BASE_URL = "https://api.openbrewerydb.org/v1/breweries"

    def __init__(self, retries=3, backoff_factor=0.3):
        self.session = requests.Session()
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            status_forcelist=(500, 502, 504),
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self.logger = logging.getLogger(__name__)

    def fetch_all_breweries(self):
        """
        Fetches all breweries handling pagination.
        dim: 200 per page (max allowed by API usually, checking docs standard is often 50-200)
        """
        all_breweries = []
        page = 1
        per_page = 200
        
        while True:
            params = {'page': page, 'per_page': per_page}
            try:
                self.logger.info(f"Fetching page {page}...")
                response = self.session.get(self.BASE_URL, params=params)
                response.raise_for_status()
                data = response.json()
                
                if not data:
                    break
                
                all_breweries.extend(data)
                self.logger.info(f"Fetched {len(data)} records from page {page}")
                
                page += 1
                # Respect rate limits
                time.sleep(0.5) 
                
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Error fetching data: {e}")
                raise e
                
        self.logger.info(f"Total breweries fetched: {len(all_breweries)}")
        return all_breweries

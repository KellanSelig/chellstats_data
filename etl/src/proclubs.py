import logging
import time
from typing import Any, Dict, Iterable, List

from requests import Session  # type: ignore
from requests.adapters import HTTPAdapter
from requests.exceptions import HTTPError
from requests.packages.urllib3.util.retry import Retry

from etl.src.settings import settings


class ProClubs:
    """Request Data from proclubs api"""

    def __enter__(self):  # type: ignore
        adapter = HTTPAdapter(max_retries=Retry(total=3, backoff_factor=1))
        session = Session()
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        self.session = session
        return self

    def __exit__(self, *exc):  # type: ignore
        self.session.close()
        return False

    def _request(self, url: str, params: Dict = None, **kwargs: Any) -> Any:
        logging.info(f"Making request to {url} using params: {params}")
        resp = self.session.get(url, headers=settings.HEADERS, params=params, **kwargs)
        resp.raise_for_status()
        return resp.json()

    def request_club_list(self, platform: str) -> List[int]:
        url = settings.BASEURL / "seasonRankLeaderboard"
        resp = self._request(url, params={"platform": platform})
        return [r["clubInfo"]["clubId"] for r in resp]

    def request_match(self, platform: str, match_type: str, club_id: int, max_result: int = 50) -> Any:
        params = {"platform": platform, "matchType": match_type, "maxResultCount": max_result, "clubIds": club_id}
        url = settings.BASEURL / "clubs/matches"
        try:
            return self._request(url, params=params)
        except HTTPError as e:
            if e.response.status_code == 500 and max_result > 1:
                logging.warning(f"Match data did not exist for max_results = {max_result}. Retrying with fewer results")
                max_result = max(1, max_result - 5)
                time.sleep(1)
                return self.request_match(platform, match_type, club_id, max_result)
            else:
                raise e

    def get_all_matches(self) -> Iterable[List[Dict]]:
        logging.info("Starting requests for match data")
        for platform in settings.PLATFORMS:
            clubs = self.request_club_list(platform)
            for club_id in clubs:
                for match_type in settings.MATCH_TYPES:
                    yield self.request_match(platform, match_type, club_id)

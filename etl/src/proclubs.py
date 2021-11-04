import logging
from itertools import chain, product
from typing import Any, Dict, Iterable, List

from requests import Session  # type: ignore
from requests.adapters import HTTPAdapter
from requests.exceptions import HTTPError
from requests.packages.urllib3.util.retry import Retry

from etl.src.settings import settings


class ProClubs:
    """Request Data from proclubs api"""

    def __init__(self) -> None:
        session = Session()
        adapter = HTTPAdapter(max_retries=Retry(total=3, backoff_factor=1))
        session.mount("https://", adapter)
        self.session = session

    def _request(self, url: str, params: Dict = None, **kwargs: Any) -> Any:
        logging.info(f"Making request to {url} using params: {params}")
        resp = self.session.get(url, headers=settings.HEADERS, params=params, **kwargs)
        resp.raise_for_status()
        return resp.json()

    def request_club_ids(self, platform: str) -> Iterable[int]:
        """Request club id list for the top 100 clubs of a given platform."""
        url = settings.BASEURL / "seasonRankLeaderboard"
        resp = self._request(url, params={"platform": platform})
        for r in resp:
            yield r["clubInfo"]["clubId"]

    def request_match(self, platform: str, match_type: str, club_id: int, max_result: int = 30) -> Any:
        """Request detailed match information"""
        params = {"platform": platform, "matchType": match_type, "maxResultCount": max_result, "clubIds": club_id}
        url = settings.BASEURL / "clubs/matches"
        try:
            return self._request(url, params=params)
        except HTTPError as e:
            if e.response.status_code == 500 and max_result > 1:
                logging.warning(f"Match data did not exist for max_results = {max_result}. Retrying with fewer results")
                return self.request_match(platform, match_type, club_id, max(1, max_result - 5))
            raise e

    def get_all_matches(self) -> Iterable[List[Dict]]:
        """Get all the most recent match data available."""
        club_ids = map(self.request_club_ids, settings.PLATFORMS)
        requests = product(settings.PLATFORMS, settings.MATCH_TYPES, chain(*club_ids))
        for req in requests:
            yield from self.request_match(*req)

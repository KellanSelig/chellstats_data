import logging
import time
from typing import List, Any, Dict
from furl import furl
from requests import Session
from requests.adapters import HTTPAdapter
from requests.exceptions import HTTPError
from requests.packages.urllib3.util.retry import Retry

BASEURL = furl("https://proclubs.ea.com/api/nhl")
HEADERS = {"referer": "www.ea.com"}
PLATFORMS = ("ps4", "xboxone")
MATCH_TYPES = ("gameType5", "gameType10", "club_private")


class ProClubs:
    """Request Data from proclubs api"""

    def __init__(self):
        adapter = HTTPAdapter(max_retries=Retry(total=3, backoff_factor=1))
        session = Session()
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        self.session = session

    def _request(self, url: str, params=None, **kwargs) -> Any:
        logging.info(params)
        resp = self.session.get(url, headers=HEADERS, params=params, **kwargs)
        resp.raise_for_status()
        return resp.json()

    def request_club_list(self, platform) -> List[int]:
        url = BASEURL / "seasonRankLeaderboard"
        resp = self._request(url, params={"platform": platform})
        return [r["clubInfo"]["clubId"] for r in resp]

    def request_match(self, platform: str, match_type: str, club_id: int, max_result: int = 50) -> List[Dict]:
        params = {"platform": platform, "matchType": match_type, "maxResultCount": max_result, "clubIds": club_id}
        url = BASEURL / "clubs/matches"
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

    def get_all_matches(self):
        for platform in PLATFORMS:
            clubs = self.request_club_list(platform)
            for club_id in clubs:
                for match_type in MATCH_TYPES:
                    yield self.request_match(platform, match_type, club_id)

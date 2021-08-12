from google.cloud import bigquery
import requests
from typing import List, Any, Dict
import logging
import time

BASEURL = "https://proclubs.ea.com/api/nhl"
HEADERS = {"referer": 'www.ea.com'}

platforms = ('ps4', 'xboxone')
match_types = (
    'gameType5',  # regular match
    'gameType10',  # playoff match
    'club_private'  # private match
)

session = requests.session()


def request_(url: str, params=None, **kwargs) -> Any:
    logging.info(params)
    resp = session.get(url, headers=HEADERS, params=params, **kwargs)
    resp.raise_for_status()
    return resp.json()


def get_clubs(platform) -> List[int]:
    url = f'{BASEURL}/seasonRankLeaderboard'
    params = {'platform': platform}
    resp = request_(url, params=params)
    return [r['clubInfo']['clubId'] for r in resp]


def request_match_data(platform: str, match_type: str, club_id: int, max_result: int = 1) -> List[Dict]:
    params = {'platform': platform, 'matchType': match_type, 'maxResultCount': max_result, 'clubIds': club_id}
    url = f'{BASEURL}/clubs/matches'
    try:
        return request_(url, params=params)
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 500 and max_result > 1:
            max_result = max(1, max_result - 5)
            time.sleep(2)
            return request_match_data(platform, match_type, club_id, max_result)
        else:
            raise e


def main():
    """Add note about those clubs! """
    data = []
    for platform in platforms:
        clubs = get_clubs(platform)
        for club_id in clubs:
            data.extend(request_match_data(platform, match_type, club_id) for match_type in match_types)


    # TODO setup table, temp dataset. upsert method
    # bq_client = bigquery.Client()
    # table: bigquery.Table = bq_client.get_table("tablename")
    # job_config = bigquery.LoadJobConfig(
    #     schema=table.schema,
    #     ignore_unknown_values=True,
    #     write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    # )
    # bq_client.load_table_from_json(data, "destingation", job_config=job_config)


if __name__ == '__main__':
    # main()
    m = request_match_data(platform='ps4', match_type='gameType10', club_id=38415, max_result=85)
    print(m)

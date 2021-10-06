from furl import furl as Url  # type: ignore
from google.cloud.bigquery import TimePartitioning, TimePartitioningType, WriteDisposition  # type: ignore
from pydantic import BaseSettings


class Settings(BaseSettings):
    # proclub settings
    BASEURL = Url("https://proclubs.ea.com/api/nhl")
    HEADERS = {"referer": "www.ea.com"}
    PLATFORMS = ("ps4", "xboxone")
    MATCH_TYPES = ("gameType5", "gameType10", "club_private")

    # Json settings
    local_json_path = "proclubs_match_data.json"

    # BigQuery Settings
    table_name = "matches"
    dataset = "records"
    bq_load_args = {
        "write_disposition": WriteDisposition.WRITE_APPEND,
        "time_partitioning": TimePartitioning(TimePartitioningType.DAY, field="created_at_dt"),
        "ignore_unknown_values": True,
    }


settings = Settings()

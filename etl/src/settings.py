from furl import furl as Url  # type: ignore
from google.cloud.bigquery import TimePartitioning, TimePartitioningType, WriteDisposition  # type: ignore
from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

    # proclub settings
    BASEURL: Url = Url("https://proclubs.ea.com/api/nhl")
    HEADERS: dict = {"referer": "www.ea.com"}
    PLATFORMS: set = {"ps4", "xboxone"}
    MATCH_TYPES: set = {"gameType5", "gameType10", "club_private"}

    # Json settings
    local_json_path: str = "proclubs_match_data.json"

    # BigQuery Settings
    table_name: str
    dataset: str
    project: str = Field(..., env="GOOGLE_CLOUD_PROJECT")
    bq_load_args: dict = {
        "write_disposition": WriteDisposition.WRITE_APPEND,
        "time_partitioning": TimePartitioning(TimePartitioningType.DAY, field="created_at_dt"),
        "ignore_unknown_values": True,
    }

    @property
    def table(self) -> str:
        return f"{self.project}.{self.dataset}.{self.table_name}"


settings = Settings()

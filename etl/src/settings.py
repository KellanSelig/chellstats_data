import google.auth  # type: ignore
from furl import furl as Url  # type: ignore
from pydantic import BaseSettings
import uuid
from jinja2 import Environment, FileSystemLoader
from pathlib import Path


class Settings(BaseSettings):
    jinja_env = Environment(loader=FileSystemLoader(Path("etl/sql").resolve()))

    # proclub settings
    BASEURL: Url = Url("https://proclubs.ea.com/api/nhl")
    HEADERS: dict = {"referer": "www.ea.com"}
    PLATFORMS: set = {"ps4", "xboxone"}
    MATCH_TYPES: set = {"gameType5", "gameType10", "club_private"}

    # Json settings
    local_json_path: str = "proclubs_match_data.json"

    # BigQuery Settings
    project: str = google.auth.default()[1]

    @property
    def match_table(self) -> str:
        return f"{self.project}.records.matches"

    @property
    def temp_table(self) -> str:
        return f"{self.project}.tmp.matches_{uuid.uuid4().int}"


settings = Settings()

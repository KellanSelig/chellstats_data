import json
import logging
from datetime import datetime
from typing import Any, Dict

from etl.src.load_methods import LoadConfig


class MatchRecords:
    def __init__(self) -> None:
        self.__data: list = []
        self.__created_at_dt: datetime = datetime.now()

    @property
    def data(self) -> Any:
        return json.loads(json.dumps(self.__data))

    def append(self, record: Dict) -> None:
        """Append rows and add created_at_dt."""
        record["created_at_dt"] = self.__created_at_dt.isoformat()
        self.__data.append(self.transform_record(record))

    def write(self, load_config: LoadConfig) -> None:
        """Write records using a LoadMethod"""
        loader = load_config.initialize()
        loader.write(self.data)
        logging.info(f"Records written successfully to {loader.destination}")

    @staticmethod
    def transform_record(d: Dict) -> Dict:
        """
        Transform records into valid format for loading into relational database.
        Sometimes the key is an int, which can't be loaded into a relational database
        """
        d["clubs"] = [club | {"club": club_id} for club_id, club in d["clubs"].items()]
        d["aggregate"] = [club | {"club": club_id} for club_id, club in d["aggregate"].items()]
        d["players"] = [
            player | {"club": club_id, "player": player_id}
            for club_id, players in d["players"].items()
            for player_id, player in players.items()
        ]
        return d

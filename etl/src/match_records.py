import json
import logging
from datetime import datetime
from typing import Any, Dict, Iterable, Union

from etl.src.load_methods import load_methods


class MatchRecords:
    def __init__(self) -> None:
        self.__data: list = []
        self.__created_at_dt: datetime = datetime.now()

    def extend(self, records: Union[Iterable, Dict]) -> None:
        if isinstance(records, Dict):
            records = [records]
        for r in records:
            r["created_at_dt"] = self.__created_at_dt.isoformat()
            self.__data.append(self.transform_record(r))

    @property
    def data(self) -> Any:
        return json.loads(json.dumps(self.__data))

    def write(self, method: str) -> None:
        loader = load_methods[method].initialize()
        loader.write(self.data)
        logging.info(f"Records written successfully to {loader.destination}")

    @staticmethod
    def transform_record(d: Dict) -> Dict:
        """
        Transform records into valid format for loading into relational database.

        The data is sent with some values that cannot be directly loaded into a relational database
        An example is the clubs are sent with a key of the club_id. Relational databases can't take a key that is
        an integer.
        """
        d["clubs"] = [club | {"club": club_id} for club_id, club in d["clubs"].items()]
        d["aggregate"] = [club | {"club": club_id} for club_id, club in d["aggregate"].items()]
        d["players"] = [
            player | {"club": club_id, "player": player_id}
            for club_id, players in d["players"].items()
            for player_id, player in players.items()
        ]
        return d

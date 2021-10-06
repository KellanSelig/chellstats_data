import json
import logging
from datetime import datetime
from typing import Any, Dict, Iterable, List, Union

from pydantic import BaseModel
from pydantic.json import pydantic_encoder
from etl.src.load_methods import BigQueryLoader, JSONLoader, LoaderConfig, get_loader
from etl.src.settings import settings

load_methods = {
    "to_json": LoaderConfig(method=JSONLoader, args=[settings.local_json_path]),
    "to_bq": LoaderConfig(
        method=BigQueryLoader, args=[settings.table_name, settings.dataset], kwargs=settings.bq_load_args
    ),
}


class MatchRecords(BaseModel):
    __data: List[Dict[str, Any]] = []
    created_at_dt: datetime = datetime.now()

    def __enter__(self):  # type: ignore
        return self

    def __exit__(self, *exc):  # type: ignore
        return False

    def extend(self, records: Union[Iterable, Dict]) -> None:
        if isinstance(records, Dict):
            records = [records]
        for r in records:
            r["created_at_dt"] = self.created_at_dt.isoformat()
            self.__data.append(self.transform_record(r))

    @property
    def data(self) -> Any:
        return json.loads(json.dumps(self.__data, default=pydantic_encoder))

    def write(self, method: str) -> None:
        loader = get_loader(method, load_methods)
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

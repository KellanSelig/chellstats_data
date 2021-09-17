import json
import logging
from datetime import datetime
from typing import List, Dict, Union, Iterable

from google.cloud import bigquery
from google import auth
from pydantic import BaseModel


class Records(BaseModel):
    __data: List = []
    created_at_dt: datetime = datetime.now()

    def __enter__(self):
        return self

    def __exit__(self, *exec):
        return False

    def extend(self, records: Union[Iterable, Dict]):
        if isinstance(records, Dict):
            records = [records]
        for r in records:
            r["created_at_dt"] = self.created_at_dt.isoformat()
            self.__data.append(self.transform(r))

    @staticmethod
    def transform(r):
        """
        Transform records into valid format for loading into relational database.

        The data is sent with some values that cannot be directly loaded into a relational database
        An example is the clubs are sent with a key of the club_id. Relational databases can't take a key that is
        an integer.

        :param r: Single record returned by proclubs API.
        """
        r["clubs"] = [club | {"club": club_id} for club_id, club in r["clubs"].items()]
        r["aggregate"] = [club | {"club": club_id} for club_id, club in r["aggregate"].items()]
        r["players"] = [
            player | {"club": club_id, "player": player_id}
            for club_id, players in r["players"].items()
            for player_id, player in players.items()
        ]
        return r


    @property
    def data(self):
        return self.__data

    def to_json(self, path="test_data.json"):
        logging.info(f"Writing records to {path}")
        with open(path, "w") as f:
            for row in self.data:
                f.write(json.dumps(row))
                f.write("\n")

    def to_bq(self) -> None:
        bq_client = bigquery.Client()
        project_id = auth.default()[1]
        table: bigquery.Table = bq_client.get_table(f"{project_id}.records.matches")
        job_config = bigquery.LoadJobConfig(
            schema=table.schema,
            ignore_unknown_values=True,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            time_partitioning=bigquery.TimePartitioning(),
        )
        logging.info(f"Loading records to Bigquery table: {table.to_api_repr()}")
        bq_client.load_table_from_json(self.data, table, job_config=job_config)

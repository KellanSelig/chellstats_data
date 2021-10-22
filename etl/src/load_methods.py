import json
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Iterable, List, Type

from google.cloud.bigquery import Client, WriteDisposition, QueryJob, LoadJobConfig  # type: ignore
from pydantic import BaseModel

from etl.src.settings import settings
from util.consume import consume


class LoadMethod(ABC):
    """Abstract base class for load methods."""
    def __init__(self, destination: Any, *args: Any, **kwargs: Any):
        self.destination = destination

    def __str__(self) -> str:
        return str(self.destination)

    @abstractmethod
    def write(self, data: Any) -> None:
        ...


class JSONLoader(LoadMethod):
    """Write Newline Delimited JSON to a file."""

    def __init__(self, path: str):
        destination = Path(path).resolve()
        super().__init__(destination)

    def write(self, data: Iterable) -> None:
        logging.info(f"Writing records to {self.destination}")
        with self.destination.open("w") as f:
            consume(f.write(json.dumps(row) + "\n") for row in data)


class BigQueryUpsertLoader(LoadMethod):
    """Upsert records into a BigQuery table."""

    def __init__(self, table: str, temp_table: str, **kwargs: Any):
        super().__init__(table)
        self.table = table
        self.temp_table = temp_table
        self.client = Client()
        self.job_config = LoadJobConfig(
            schema=self.client.get_table(table).schema,
            ignore_unknown_values=True,
            write_disposition=WriteDisposition.WRITE_TRUNCATE,
            **kwargs
        )

    def write(self, data: Iterable) -> None:
        logging.info(f"Loading records to temporary table: {self.temp_table}")
        self.client.load_table_from_json(data, self.temp_table, job_config=self.job_config).result()
        job = self._merge()
        job.result()
        logging.info(job.dml_stats)

    def _merge(self) -> QueryJob:
        logging.info(f"Merging records to Bigquery table: {self.destination}")
        dml = settings.jinja_env.get_template("merge.sql").render(table=self.table, temp=self.temp_table)
        return self.client.query(dml)


class LoadConfig(BaseModel):
    method: Type[LoadMethod]
    args: List = []
    kwargs: Dict = {}

    def initialize(self) -> LoadMethod:
        return self.method(*self.args, **self.kwargs)


class InvalidLoadMethod(Exception):
    pass


load_methods = {
    "to_json": LoadConfig(method=JSONLoader, args=[settings.local_json_path]),
    "to_bq": LoadConfig(method=BigQueryUpsertLoader, args=[settings.match_table, settings.temp_table]),
}

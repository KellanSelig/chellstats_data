import json
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Type

from google.cloud import bigquery  # type: ignore
from pydantic import BaseModel


class LoadMethod(ABC):
    def __init__(self, destination: Any, *args: Any, **kwargs: Any):
        self.destination = destination

    def __str__(self) -> str:
        return str(self.destination)

    @abstractmethod
    def write(self, data: Any) -> None:
        ...


class JSONLoader(LoadMethod):
    """Load data to local JSON file."""

    def __init__(self, path: str):
        destination = Path(path).resolve()
        super().__init__(destination)

    def write(self, data: Iterable) -> None:
        logging.info(f"Writing records to {self.destination}")
        with self.destination.open("w") as f:
            for row in data:
                f.write(json.dumps(row))
                f.write("\n")


class BigQueryLoader(LoadMethod):
    """Write data to Bigquery Table."""

    def __init__(
        self,
        table: str,
        client: Optional[bigquery.Client] = None,
        **kwargs: Any,
    ):
        self.client = client or bigquery.Client()
        self.table = self.client.get_table(table)
        self.job_config = bigquery.LoadJobConfig(schema=self.table.schema, **kwargs)
        super().__init__(f"`{self.table.project}:{self.table.dataset_id}.{self.table.table_id}`")

    def write(self, data: Iterable) -> None:
        logging.info(f"Loading records to Bigquery table: {self.destination}")
        self.client.load_table_from_json(data, self.table, job_config=self.job_config).result()


class LoaderConfig(BaseModel):
    method: Type[LoadMethod]
    args: List = []
    kwargs: Dict = {}

    def initialize(self) -> LoadMethod:
        return self.method(*self.args, **self.kwargs)


class InvalidLoadMethod(Exception):
    pass


def get_loader(method: str, load_method_fac: Dict[str, LoaderConfig]) -> LoadMethod:
    try:
        return load_method_fac[method].initialize()
    except KeyError:
        raise InvalidLoadMethod(f"{method} is not in valid methods: {load_method_fac.keys()}")

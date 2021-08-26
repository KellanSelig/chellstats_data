import json
import logging
from datetime import datetime
from typing import List, Dict, Union, Iterable

from google.cloud import bigquery
from pydantic import BaseModel


class Records(BaseModel):
    data: List = []
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
            self.data.append(r)

    def to_json(self, path="test_data.json"):
        logging.info(f"Writing records to {path}")
        with open(path, "w") as f:
            for row in self.data:
                f.write(json.dumps(row))
                f.write("\n")

    def to_bq(self) -> None:
        bq_client = bigquery.Client()
        table: bigquery.Table = bq_client.get_table("tablename")
        job_config = bigquery.LoadJobConfig(
            schema=table.schema,
            ignore_unknown_values=True,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            time_partitioning=bigquery.TimePartitioning(),
        )
        logging.info(f"Loading records to Bigquery table: {table.to_api_repr()}")
        bq_client.load_table_from_json(self.data, table, job_config=job_config)

import logging

from fastapi import FastAPI

from etl.src.match_records import MatchRecords
from etl.src.models import Request
from etl.src.proclubs import ProClubs
from util.consume import consume

logging.basicConfig(level=logging.INFO)

app = FastAPI()


@app.post("/")
def main(req: Request) -> str:
    records = MatchRecords()
    service = ProClubs()
    consume(records.extend(matches) for matches in service.get_all_matches())  # type: ignore
    records.write(req.method)
    return "Done"

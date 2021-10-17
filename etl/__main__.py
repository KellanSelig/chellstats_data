import argparse
import logging
from typing import Any, no_type_check

from etl.src.match_records import MatchRecords, load_methods
from etl.src.proclubs import ProClubs
from util.consume import consume

logging.basicConfig(level=logging.INFO)


@no_type_check
def main(write_method: str) -> None:
    records = MatchRecords()
    service = ProClubs()
    logging.info(f"Starting ETL. Data will be written using {write_method}")
    consume(records.extend(matches) for matches in service.get_all_matches())
    records.write(write_method)


if __name__ == "__main__":
    valid_load_methods = ", ".join(load_methods.keys())

    parser = argparse.ArgumentParser()
    parser.add_argument("--method", action="store", default="to_json", help=f"Valid methods: {valid_load_methods}")
    parser.add_argument("--local", "-l", action="store_true", default=False, help="Run locally")
    args = parser.parse_args()

    assert args.method in load_methods, f"Invalid load method selected. Valid methods: {valid_load_methods}"
    if args.local:
        main(args.method)
    else:
        # If we are not running locally, we need to be able to listen and respond on a port to use cloud run.
        # We set up a wrapper using fast-api to do this
        # TODO - convert from CLI to just a webapp
        import uvicorn  # type: ignore
        from fastapi import FastAPI

        app = FastAPI()

        @app.post("/")
        def run_etl(payload: Any = None) -> str:
            """Wrap function around an endpoint to use with cloud run."""
            main(args.method)
            return "Done"

        uvicorn.run(app, host="0.0.0.0", port=8080)

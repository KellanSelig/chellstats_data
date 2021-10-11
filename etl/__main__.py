import argparse
import logging
from typing import no_type_check

from etl.src.match_records import MatchRecords, load_methods
from etl.src.proclubs import ProClubs
from util.consume import consume

logging.basicConfig(level=logging.INFO)


@no_type_check
def main(write_method: str) -> None:
    logging.info(f"Starting ETL. Data will be written using {write_method}")
    records = MatchRecords()
    with ProClubs() as service:
        consume(records.extend(matches) for matches in service.get_all_matches())
    records.write(write_method)


if __name__ == "__main__":
    valid_load_methods = ", ".join(load_methods.keys())
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--method", action="store", default="to_json", help=f"Valid write methods: {valid_load_methods}"
    )

    args = parser.parse_args()
    assert args.method in load_methods, f"Invalid load method selected. Valid methods: {valid_load_methods}"
    main(args.method)

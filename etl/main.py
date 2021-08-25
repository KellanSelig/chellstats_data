import argparse
import logging

from src.models import Records
from src.proclubs import ProClubs
from util import consume

logging.basicConfig(level=logging.DEBUG)


def main(to_bq: bool = False):
    if to_bq:  # FIXME -> data returned from proclubs currently is not loadable
        raise NotImplementedError("Currently only loading to JSON is supported")

    proclubs = ProClubs()
    with Records() as records:
        logging.info("Starting requests for match data")
        consume(records.extend(matches) for matches in proclubs.get_all_matches())
        getattr(records, "to_bigquery" if to_bq else "to_json")()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-bq", "--to_bigquery", action="store_true", default=False)
    args = parser.parse_args()
    main(args.to_bigquery)

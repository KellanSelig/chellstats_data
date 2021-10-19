import json
from pathlib import Path

from etl.src import match_records


def test_transform() -> None:
    basepath = Path(__file__).parent
    with open(basepath / "test_record_raw.json") as f, open(basepath / "test_record_transformed.json") as e:
        raw = json.load(f)
        expected = json.load(e)

    transformed = match_records.MatchRecords.transform_record(raw)
    assert transformed == expected, "transformed and expected values do not "

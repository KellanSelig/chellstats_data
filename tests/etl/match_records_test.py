import json
from pathlib import Path

from src.match_records import MatchRecords


def test_transform() -> None:
    basepath = Path(__file__).parent
    with open(basepath / "test_record_raw.json") as f, open(basepath / "test_record_transformed.json") as e:
        raw = json.load(f)
        expected = json.load(e)

    transformed = MatchRecords.transform_record(raw)
    assert transformed == expected, "transformed and expected values do not "

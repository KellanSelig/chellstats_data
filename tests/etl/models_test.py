import json

from etl.src.models import Records
from pathlib import Path

def test_transform():
    basepath = Path(__file__).parent
    with open(basepath / "test_record_raw.json") as f, open(basepath / "test_record_transformed.json") as e:
        raw = json.load(f)
        expected = json.load(e)

    transformed = Records.transform(raw)
    assert transformed == expected, "transformed and expected values do not "

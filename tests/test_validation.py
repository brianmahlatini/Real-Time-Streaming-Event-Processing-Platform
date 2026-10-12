import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from producer.generate_events import build_event


def test_generated_event_contains_required_fields():
    event = build_event(sequence=1, late_probability=0, invalid_probability=0)

    assert event["event_id"]
    assert event["account_id"].startswith("acct-")
    assert event["amount"] > 0
    assert event["status"] in {"approved", "declined", "reversed"}


def test_invalid_probability_can_create_invalid_event():
    invalid_events = [build_event(sequence=i, late_probability=0, invalid_probability=1) for i in range(20)]

    assert any(
        event.get("event_id") is None
        or event.get("account_id") is None
        or event.get("event_time") is None
        or event.get("amount") == -1
        for event in invalid_events
    )

import json
import stat

import pytest

from conductress.delivery_journal import DeliveryJournal


def test_journal_persists_active_transitions_and_stats(tmp_path):
    path = tmp_path / "delivery.json"
    journal = DeliveryJournal(path)
    assert journal.active is None

    journal.set_active({"task_id": "t1", "stage": "claimed"})
    journal.update_active(stage="accepted")
    journal.record_import("t1")
    journal.update_stats(control_reachable=True, last_poll_result="claimed")

    reloaded = DeliveryJournal(path)
    assert reloaded.active == {"task_id": "t1", "stage": "accepted"}
    assert reloaded.stats["imported_count_total"] == 1
    assert reloaded.stats["last_imported_task_id"] == "t1"
    assert stat.S_IMODE(path.stat().st_mode) == 0o600

    reloaded.clear_active()
    assert DeliveryJournal(path).active is None


def test_journal_rejects_corrupt_or_unknown_documents(tmp_path):
    path = tmp_path / "delivery.json"
    path.write_text("not json", encoding="utf-8")
    with pytest.raises(json.JSONDecodeError):
        DeliveryJournal(path)

    path.write_text(json.dumps({"schema_version": 99, "active": None, "stats": {}}), encoding="utf-8")
    with pytest.raises(ValueError, match="unsupported"):
        DeliveryJournal(path)

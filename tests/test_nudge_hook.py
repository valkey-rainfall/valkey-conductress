"""Tests for the NudgeHook HTTP webhook subscriber."""

import json
import threading
import urllib.error
import urllib.request
from unittest.mock import MagicMock, patch

import pytest

from conductress import nudge_hook
from conductress.nudge_hook import NudgeHook
from conductress.tasks.task_perf_benchmark import PerfTaskData


@pytest.fixture(autouse=True)
def isolate_config(monkeypatch, tmp_path):
    """Isolate tests from real Conductress output data and config pollution.

    Monkeypatches CONDUCTRESS_OUTPUT in the nudge_hook module to a tmp_path
    file, preventing accidental deletion or modification of real output data.
    Also isolates from config.REPO_NAMES pollution from other test modules.
    """
    import conductress.config as cfg

    monkeypatch.setattr(cfg, "REPO_NAMES", ["valkey", "rainsupreme", "zuiderkwast", "JimB123"])
    monkeypatch.setattr(cfg, "MANUALLY_UPLOADED", "manually_uploaded")
    monkeypatch.setattr(
        "conductress.nudge_hook.CONDUCTRESS_OUTPUT",
        str(tmp_path / "output.jsonl"),
    )
    yield


def _make_perf_task() -> PerfTaskData:
    """Create a minimal PerfTaskData for testing."""
    return PerfTaskData(
        source="valkey",
        specifier="unstable",
        make_args="",
        replicas=0,
        note="test note",
        requirements={},
        test="get",
        val_size=16,
        io_threads=9,
        pipelining=10,
        warmup=30,
        duration=30,
        perf_stat_enabled=False,
        has_expire=False,
        preload_keys=True,
        repetitions=1,
    )


class TestNudgeHookInit:
    """Test NudgeHook initialization."""

    def test_default_events(self):
        hook = NudgeHook("http://example.com/nudge")
        assert hook._endpoint_url == "http://example.com/nudge"
        assert hook._events == {"completed", "failed", "empty"}

    def test_custom_events(self):
        events = {"completed", "empty"}
        hook = NudgeHook("http://example.com/nudge", events=events)
        assert hook._events == events

    def test_no_nudge_for_unset_events(self):
        """When an event type is not in the events set, no request is sent."""
        hook = NudgeHook("http://example.com/nudge", events=set())
        task = _make_perf_task()

        with patch.object(hook, "_send") as mock_send:
            hook.on_task_completed(task)
            mock_send.assert_not_called()


class TestNudgeHookPayload:
    """Test nudge payload construction."""

    @patch("conductress.nudge_hook.logger")
    def test_payload_includes_task_metadata(self, mock_logger):
        hook = NudgeHook("http://example.com/nudge")
        task = _make_perf_task()

        with patch.object(hook, "_send") as mock_send:
            hook.on_task_completed(task)

        mock_send.assert_called_once()
        payload = mock_send.call_args[0][0]
        assert payload["event"] == "completed"
        assert payload["task_id"] == task.task_id
        assert payload["source"] == "valkey"
        assert payload["specifier"] == "unstable"
        assert payload["note"] == "test note"
        assert payload["task_type"] == "PerfTaskData"
        assert payload["test"] == "get"
        assert payload["val_size"] == 16
        assert payload["io_threads"] == 9
        assert payload["pipelining"] == 10

    @patch("conductress.nudge_hook.logger")
    def test_payload_includes_results_from_output_log(self, mock_logger):
        """When a result exists in the output log, score and data are included."""
        hook = NudgeHook("http://example.com/nudge")
        task = _make_perf_task()

        # Write a result line to the output log (now points to tmp_path via fixture)
        result_entry = {
            "task_id": task.task_id,
            "score": 3295847.0,
            "commit_hash": "abc12345",
            "data": {"mean_rps": 3295847.0, "ci_95": 50000.0},
            "source": "valkey",
            "specifier": "unstable",
            "make_args": "",
            "note": "test note",
            "method": "perf-get",
            "end_time": "2026-07-24T15:00:00",
            "features": {},
            "task_type": "perf_runner",
        }
        with open(nudge_hook.CONDUCTRESS_OUTPUT, "a") as f:
            f.write(json.dumps(result_entry) + "\n")

        with patch.object(hook, "_send") as mock_send:
            hook.on_task_completed(task)

        mock_send.assert_called_once()
        payload = mock_send.call_args[0][0]
        assert payload["score"] == 3295847.0
        assert payload["commit_hash"] == "abc12345"
        assert payload["data"]["mean_rps"] == 3295847.0

    def test_queue_empty_payload(self):
        hook = NudgeHook("http://example.com/nudge", events={"empty"})
        with patch.object(hook, "_send") as mock_send:
            hook.on_queue_empty()
        mock_send.assert_called_once()
        sent_payload = mock_send.call_args[0][0]
        assert sent_payload["event"] == "empty"

    def test_task_failed_payload(self):
        hook = NudgeHook("http://example.com/nudge", events={"failed"})
        task = _make_perf_task()
        with patch.object(hook, "_send") as mock_send:
            hook.on_task_failed(task)
        mock_send.assert_called_once()
        payload = mock_send.call_args[0][0]
        assert payload["event"] == "failed"


class TestNudgeHookReadResults:
    """Test the _read_latest_result helper."""

    def test_reads_matching_result(self):
        task = _make_perf_task()
        entry = {
            "task_id": task.task_id,
            "score": 3000000.0,
            "commit_hash": "def67890",
            "data": {},
            "source": "valkey",
            "specifier": "unstable",
            "make_args": "",
            "note": "",
            "method": "perf-get",
            "end_time": "2026-07-24T15:00:00",
            "features": {},
            "task_type": "perf_runner",
        }
        with open(nudge_hook.CONDUCTRESS_OUTPUT, "w") as f:
            f.write(
                json.dumps(
                    {
                        "task_id": "other",
                        "score": 100,
                        "source": "valkey",
                        "specifier": "x",
                        "make_args": "",
                        "note": "",
                        "method": "m",
                        "end_time": "t",
                        "features": {},
                        "task_type": "t",
                    }
                )
                + "\n"
            )
            f.write(json.dumps(entry) + "\n")

        result = NudgeHook._read_latest_result(task.task_id)
        assert result["score"] == 3000000.0
        assert result["commit_hash"] == "def67890"

    def test_returns_none_when_no_match(self):
        with open(nudge_hook.CONDUCTRESS_OUTPUT, "w") as f:
            f.write(
                json.dumps(
                    {
                        "task_id": "other",
                        "score": 100,
                        "source": "valkey",
                        "specifier": "x",
                        "make_args": "",
                        "note": "",
                        "method": "m",
                        "end_time": "t",
                        "features": {},
                        "task_type": "t",
                    }
                )
                + "\n"
            )

        result = NudgeHook._read_latest_result("nonexistent_task")
        assert result is None

    def test_returns_none_when_file_not_found(self):
        result = NudgeHook._read_latest_result("any_task_id")
        assert result is None


class TestNudgeHookSend:
    """Test the _send / _do_send HTTP methods."""

    @patch("conductress.nudge_hook.logger")
    def test_successful_http_post(self, mock_logger):
        """_do_send makes an HTTP POST to the endpoint."""
        hook = NudgeHook("http://example.com/nudge")
        payload = {"event": "completed", "task_id": "test_task"}

        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)

        with patch("urllib.request.urlopen", return_value=mock_response) as mock_urlopen:
            hook._do_send(payload)

        mock_urlopen.assert_called_once()
        request = mock_urlopen.call_args[0][0]
        assert request.full_url == "http://example.com/nudge"
        assert request.method == "POST"
        assert mock_urlopen.call_args[1]["timeout"] == 3

    @patch("conductress.nudge_hook.logger")
    def test_handles_http_error(self, mock_logger):
        """HTTP errors are logged but do not raise."""
        hook = NudgeHook("http://example.com/nudge")
        payload = {"event": "completed", "task_id": "test_task"}

        with patch(
            "urllib.request.urlopen",
            side_effect=urllib.error.HTTPError("http://example.com/nudge", 500, "Internal Server Error", {}, None),
        ):
            hook._do_send(payload)

        mock_logger.warning.assert_called()

    def test_send_delegates_to_thread(self):
        """_send should delegate the HTTP request to a background daemon thread."""
        hook = NudgeHook("http://example.com/nudge")
        call_event = threading.Event()

        def mock_do_send(payload):
            call_event.set()

        with patch.object(hook, "_do_send", side_effect=mock_do_send):
            hook._send({"test": True})
            assert call_event.wait(timeout=5), "Thread did not call _do_send within 5s"

    def test_timeout_reduced_from_10(self):
        """The inner HTTP request timeout should be 3s, not 10s."""
        hook = NudgeHook("http://example.com/nudge")

        with patch("urllib.request.urlopen") as mock_urlopen:
            hook._do_send({"test": True})

        assert mock_urlopen.call_args[1]["timeout"] == 3


class TestNudgeHookQueueEmptyDedupe:
    """Test the once-per-transition dedupe for on_queue_empty."""

    def test_queue_empty_dedupe(self):
        """on_queue_empty only sends once while idle (prevents spam)."""
        hook = NudgeHook("http://example.com/nudge", events={"empty"})
        with patch.object(hook, "_send") as mock_send:
            hook.on_queue_empty()
            hook.on_queue_empty()
            hook.on_queue_empty()
        mock_send.assert_called_once()

    def test_queue_empty_fires_after_task(self):
        """After a task completes, on_queue_empty fires again (reset)."""
        hook = NudgeHook("http://example.com/nudge", events={"empty", "completed"})
        task = _make_perf_task()

        with patch.object(hook, "_send") as mock_send:
            # First idle period: one empty nudge, then deduped
            hook.on_queue_empty()
            hook.on_queue_empty()  # Deduped
            assert mock_send.call_count == 1  # Only the empty nudge

            # Task completes: task payload nudge + reset _empty_nudged
            hook.on_task_completed(task)
            assert mock_send.call_count == 2  # + task payload nudge

            # New idle period: one more empty nudge
            hook.on_queue_empty()
            hook.on_queue_empty()  # Deduped
            assert mock_send.call_count == 3  # + one more empty nudge

    def test_queue_empty_fires_after_failed_task(self):
        """After a task fails, on_queue_empty fires again (reset)."""
        hook = NudgeHook("http://example.com/nudge", events={"empty", "failed"})
        task = _make_perf_task()

        with patch.object(hook, "_send") as mock_send:
            hook.on_queue_empty()
            hook.on_queue_empty()  # Deduped
            assert mock_send.call_count == 1

            hook.on_task_failed(task)
            assert mock_send.call_count == 2

            hook.on_queue_empty()
            hook.on_queue_empty()  # Deduped
            assert mock_send.call_count == 3

    def test_empty_nudged_flag_tracking(self):
        """The _empty_nudged flag should be reset to False after task events."""
        hook = NudgeHook("http://example.com/nudge", events={"empty", "completed", "failed"})
        task = _make_perf_task()

        with patch.object(hook, "_send"):
            # First empty nudge sets the flag
            hook.on_queue_empty()
            assert hook._empty_nudged is True

            # Duplicate call is deduped
            hook.on_queue_empty()
            assert hook._empty_nudged is True  # Still True

            # Task completion resets the flag
            hook.on_task_completed(task)
            assert hook._empty_nudged is False

            # Next empty nudge fires
            hook.on_queue_empty()
            assert hook._empty_nudged is True

            # Task failure resets the flag
            hook.on_task_failed(task)
            assert hook._empty_nudged is False

    def test_empty_nudge_fires_twice_after_task_without_completed_subscription(self):
        """When nudge-on is 'empty' only, on_task_completed must still reset
        _empty_nudged so the second empty transition fires.

        This is the regression test for the logic bug where _empty_nudged reset
        was inside the 'completed' event-subscription guard. Without the fix,
        the flag would remain True after the first idle period, and the second
        on_queue_empty would be deduped (no send).
        """
        hook = NudgeHook("http://example.com/nudge", events={"empty"})
        sends = []
        with patch.object(hook, "_send", side_effect=lambda p: sends.append(p)):
            hook.on_queue_empty()  # fires: send #1
            hook.on_task_completed(_make_perf_task())  # no send (completed not subscribed)
            hook.on_queue_empty()  # fires again: send #2
        assert len(sends) == 2


class TestNudgeHookTimestamp:
    """Test timezone-aware timestamps."""

    def test_timestamp_is_timezone_aware(self):
        """Timestamps in nudges should be timezone-aware (ISO format with +00:00)."""
        hook = NudgeHook("http://example.com/nudge", events={"empty"})
        with patch.object(hook, "_send") as mock_send:
            hook.on_queue_empty()
        payload = mock_send.call_args[0][0]
        timestamp = payload["timestamp"]
        assert "+00:00" in timestamp

    def test_task_payload_timestamp_is_timezone_aware(self):
        """Timestamps in task payloads should also be timezone-aware."""
        hook = NudgeHook("http://example.com/nudge")
        task = _make_perf_task()

        with patch.object(hook, "_send") as mock_send:
            hook.on_task_completed(task)
        payload = mock_send.call_args[0][0]
        timestamp = payload["timestamp"]
        assert "+00:00" in timestamp


class TestPerfTaskDataCreation:
    """Smoke test: PerfTaskData can be instantiated with realistic fields."""

    def test_perf_task_data(self):
        task = _make_perf_task()
        assert task.source == "valkey"
        assert task.specifier == "unstable"
        assert task.test == "get"
        assert task.val_size == 16
        assert task.io_threads == 9
        assert task.pipelining == 10
        assert task.warmup == 30
        assert task.duration == 30
        assert task.task_type == "PerfTaskData"
        assert task.task_id != ""

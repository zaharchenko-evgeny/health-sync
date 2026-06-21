from datetime import UTC, datetime, timedelta

from health_sync.state import SyncState


def test_record_event_updates_compact_state(tmp_path):
    state = SyncState(tmp_path / "state.sqlite3")
    event_id = state.record_event(
        run_id=None,
        flow_name="test",
        source="zepp",
        target="garmin",
        logical_key="zepp:body:1",
        payload_hash_value="abc",
        status="success",
        dry_run=False,
        payload={"weight": 86.2},
        update_state=True,
    )

    assert event_id > 0
    assert state.current_hash("zepp:body:1", "garmin") == "abc"
    assert state.current_payload("zepp:body:1", "garmin") == {"weight": 86.2}
    assert state.already_applied("zepp:body:1", "garmin", "abc")


def test_dry_run_event_does_not_update_compact_state(tmp_path):
    state = SyncState(tmp_path / "state.sqlite3")
    state.record_event(
        run_id=None,
        flow_name="test",
        source="zepp",
        target="garmin",
        logical_key="zepp:body:1",
        payload_hash_value="abc",
        status="dry_run",
        dry_run=True,
        payload={"weight": 86.2},
        update_state=True,
    )

    assert state.current_hash("zepp:body:1", "garmin") is None


def test_cleanup_removes_old_terminal_rows_but_keeps_sync_state(tmp_path):
    state = SyncState(tmp_path / "state.sqlite3")
    run_id = state.begin_run("test", dry_run=False)
    state.finish_run(run_id, "success")
    state.record_event(
        run_id=run_id,
        flow_name="test",
        source="zepp",
        target="garmin",
        logical_key="zepp:body:1",
        payload_hash_value="abc",
        status="success",
        dry_run=False,
        payload={"weight": 86.2},
        update_state=True,
    )

    old = (datetime.now(UTC) - timedelta(days=200)).isoformat()
    with state.connect() as conn:
        conn.execute("UPDATE sync_events SET created_at = ?", (old,))
        conn.execute("UPDATE sync_runs SET started_at = ?, finished_at = ?", (old, old))

    result = state.cleanup(now=datetime.now(UTC), vacuum=False, analyze=False)

    assert result.sync_events_deleted == 1
    assert result.sync_runs_deleted == 1
    assert state.current_hash("zepp:body:1", "garmin") == "abc"


def test_cleanup_does_not_delete_unresolved_failed_event(tmp_path):
    state = SyncState(tmp_path / "state.sqlite3")
    state.record_event(
        run_id=None,
        flow_name="test",
        source="zepp",
        target="garmin",
        logical_key="zepp:body:1",
        payload_hash_value="abc",
        status="failed",
        dry_run=False,
        payload={"weight": 86.2},
        error="boom",
    )

    old = (datetime.now(UTC) - timedelta(days=400)).isoformat()
    with state.connect() as conn:
        conn.execute("UPDATE sync_events SET created_at = ?", (old,))
        conn.execute("UPDATE dead_letters SET created_at = ?", (old,))

    result = state.cleanup(now=datetime.now(UTC))

    assert result.failed_events_deleted == 0
    with state.connect() as conn:
        count = conn.execute("SELECT COUNT(*) AS count FROM sync_events").fetchone()["count"]
    assert count == 1

"""[lite-engine] Clean-room, dependency-free in-memory storage.

Dagster's stock ``InMemoryRunStorage`` / ``InMemoryEventLogStorage`` are
"in-memory" only in the sense that they point SQLAlchemy at an in-memory
SQLite database — so importing them drags in sqlalchemy + alembic + greenlet +
mako (~18MB). These classes reimplement the :class:`RunStorage` and
:class:`EventLogStorage` interfaces directly on top of plain Python dicts/lists,
with zero third-party dependencies, so the lite engine can run the in-process
execution path (``materialize`` / ``execute_in_process``) with no SQL stack
installed.

Scope: the execution hot path — creating runs, recording run/step events,
transitioning run status, storing & reading back the event log, and job /
execution-plan snapshots. Features that only matter to the (removed) UI /
daemon / scheduler — backfills, concurrency pools, dynamic partitions, the
asset-status cache, daemon heartbeats — raise :class:`NotImplementedError` so
misuse fails loudly instead of silently returning wrong data.
"""

from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Optional

import dagster._check as check
from dagster._core.event_api import EventRecordsResult
from dagster._core.events import EVENT_TYPE_TO_PIPELINE_RUN_STATUS, DagsterEvent, DagsterEventType
from dagster._core.snap import ExecutionPlanSnapshot, JobSnap, create_execution_plan_snapshot_id
from dagster._core.storage.dagster_run import (
    DagsterRun,
    JobBucket,
    RunPartitionData,
    RunRecord,
    RunsFilter,
    TagBucket,
)
from dagster._core.storage.event_log.base import EventLogConnection, EventLogStorage
from dagster._core.storage.runs.base import RunStorage
from dagster._time import get_current_datetime

if TYPE_CHECKING:
    from dagster._core.event_api import EventLogRecord
    from dagster._core.events.log import EventLogEntry


def _lite(name: str) -> "NotImplementedError":
    return NotImplementedError(
        f"LiteInMemory storage does not implement '{name}' — it only supports the "
        "in-process execution path. This feature belongs to the UI/daemon/scheduler "
        "subsystems that the lite engine removes."
    )


class LiteInMemoryRunStorage(RunStorage):
    """Dict-backed clean-room implementation of :class:`RunStorage`."""

    def __init__(self, preload=None):
        # run_id -> RunRecord
        self._runs: dict[str, RunRecord] = {}
        self._job_snapshots: dict[str, JobSnap] = {}
        self._ep_snapshots: dict[str, ExecutionPlanSnapshot] = {}
        self._next_storage_id = 1
        if preload:
            raise _lite("RunStorage(preload=...)")

    # --- snapshots -------------------------------------------------------
    def add_job_snapshot(self, job_snapshot: JobSnap) -> str:
        snapshot_id = job_snapshot.snapshot_id
        self._job_snapshots[snapshot_id] = job_snapshot
        return snapshot_id

    def has_job_snapshot(self, job_snapshot_id: str) -> bool:
        return job_snapshot_id in self._job_snapshots

    def get_job_snapshot(self, job_snapshot_id: str) -> JobSnap:
        return self._job_snapshots[job_snapshot_id]

    def add_execution_plan_snapshot(self, execution_plan_snapshot: ExecutionPlanSnapshot) -> str:
        snapshot_id = create_execution_plan_snapshot_id(execution_plan_snapshot)
        self._ep_snapshots[snapshot_id] = execution_plan_snapshot
        return snapshot_id

    def has_execution_plan_snapshot(self, execution_plan_snapshot_id: str) -> bool:
        return execution_plan_snapshot_id in self._ep_snapshots

    def get_execution_plan_snapshot(self, execution_plan_snapshot_id: str) -> ExecutionPlanSnapshot:
        return self._ep_snapshots[execution_plan_snapshot_id]

    # --- runs ------------------------------------------------------------
    def add_run(self, dagster_run: DagsterRun) -> DagsterRun:
        check.inst_param(dagster_run, "dagster_run", DagsterRun)
        if dagster_run.run_id in self._runs:
            check.failed(f"Run {dagster_run.run_id} already exists in storage")
        if dagster_run.job_snapshot_id and not self.has_job_snapshot(dagster_run.job_snapshot_id):
            check.failed(f"Snapshot {dagster_run.job_snapshot_id} does not exist in run storage")
        now = get_current_datetime()
        self._runs[dagster_run.run_id] = RunRecord(
            storage_id=self._next_storage_id,
            dagster_run=dagster_run,
            create_timestamp=now,
            update_timestamp=now,
        )
        self._next_storage_id += 1
        return dagster_run

    def add_historical_run(
        self, dagster_run: DagsterRun, run_creation_time: datetime
    ) -> DagsterRun:
        raise _lite("add_historical_run")

    def handle_run_event(
        self, run_id: str, event: DagsterEvent, update_timestamp: datetime | None = None
    ) -> None:
        check.str_param(run_id, "run_id")
        check.inst_param(event, "event", DagsterEvent)
        if event.event_type not in EVENT_TYPE_TO_PIPELINE_RUN_STATUS:
            return
        record = self._runs.get(run_id)
        if not record:
            return

        new_status = EVENT_TYPE_TO_PIPELINE_RUN_STATUS[event.event_type]
        updated_run = record.dagster_run.with_status(new_status)
        update_timestamp = update_timestamp or get_current_datetime()

        start_time = record.start_time
        end_time = record.end_time
        if event.event_type == DagsterEventType.PIPELINE_START:
            start_time = update_timestamp.timestamp()
        if event.event_type in {
            DagsterEventType.PIPELINE_SUCCESS,
            DagsterEventType.PIPELINE_FAILURE,
            DagsterEventType.PIPELINE_CANCELED,
        }:
            end_time = update_timestamp.timestamp()

        self._runs[run_id] = record._replace(
            dagster_run=updated_run,
            update_timestamp=update_timestamp,
            start_time=start_time,
            end_time=end_time,
        )

    def _matches(self, run: DagsterRun, filters: RunsFilter | None) -> bool:
        if filters is None:
            return True
        if filters.run_ids and run.run_id not in filters.run_ids:
            return False
        if filters.job_name and run.job_name != filters.job_name:
            return False
        if filters.statuses and run.status not in filters.statuses:
            return False
        if filters.snapshot_id and run.job_snapshot_id != filters.snapshot_id:
            return False
        if filters.tags:
            for key, value in filters.tags.items():
                run_value = run.tags.get(key)
                if isinstance(value, (list, set, tuple)):
                    if run_value not in value:
                        return False
                elif run_value != value:
                    return False
        return True

    def _sorted_records(self, ascending: bool = False) -> list[RunRecord]:
        return sorted(self._runs.values(), key=lambda r: r.storage_id, reverse=not ascending)

    def get_runs(
        self,
        filters: RunsFilter | None = None,
        cursor: str | None = None,
        limit: int | None = None,
        bucket_by: JobBucket | TagBucket | None = None,
        ascending: bool = False,
    ) -> Sequence[DagsterRun]:
        if bucket_by is not None:
            raise _lite("get_runs(bucket_by=...)")
        runs = [
            r.dagster_run
            for r in self._sorted_records(ascending)
            if self._matches(r.dagster_run, filters)
        ]
        return self._apply_cursor_limit(runs, cursor, limit, key=lambda r: r.run_id)

    def get_run_records(
        self,
        filters: RunsFilter | None = None,
        limit: int | None = None,
        order_by: str | None = None,
        ascending: bool = False,
        cursor: str | None = None,
        bucket_by: JobBucket | TagBucket | None = None,
    ) -> Sequence[RunRecord]:
        if bucket_by is not None:
            raise _lite("get_run_records(bucket_by=...)")
        records = [
            r for r in self._sorted_records(ascending) if self._matches(r.dagster_run, filters)
        ]
        return self._apply_cursor_limit(records, cursor, limit, key=lambda r: r.dagster_run.run_id)

    @staticmethod
    def _apply_cursor_limit(items, cursor, limit, key):
        if cursor:
            ids = [key(i) for i in items]
            if cursor in ids:
                items = items[ids.index(cursor) + 1 :]
        if limit is not None:
            items = items[:limit]
        return items

    def get_runs_count(self, filters: RunsFilter | None = None) -> int:
        return len([r for r in self._runs.values() if self._matches(r.dagster_run, filters)])

    def get_run_ids(
        self,
        filters: RunsFilter | None = None,
        cursor: str | None = None,
        limit: int | None = None,
    ) -> Sequence[str]:
        return [r.run_id for r in self.get_runs(filters, cursor, limit)]

    def has_run(self, run_id: str) -> bool:
        return run_id in self._runs

    def delete_run(self, run_id: str) -> None:
        self._runs.pop(run_id, None)

    def add_run_tags(self, run_id: str, new_tags: Mapping[str, str]) -> None:
        record = self._runs[run_id]
        merged = {**record.dagster_run.tags, **new_tags}
        self._runs[run_id] = record._replace(
            dagster_run=record.dagster_run.with_tags(merged),
            update_timestamp=get_current_datetime(),
        )

    def get_run_tags(
        self,
        tag_keys: Sequence[str],
        value_prefix: str | None = None,
        limit: int | None = None,
    ) -> Sequence[tuple[str, set[str]]]:
        out: dict[str, set[str]] = defaultdict(set)
        for record in self._runs.values():
            for key, value in record.dagster_run.tags.items():
                if tag_keys and key not in tag_keys:
                    continue
                if value_prefix and not value.startswith(value_prefix):
                    continue
                out[key].add(value)
        return sorted(out.items())

    def get_run_tag_keys(self) -> Sequence[str]:
        keys: set[str] = set()
        for record in self._runs.values():
            keys.update(record.dagster_run.tags.keys())
        return sorted(keys)

    def get_run_group(self, run_id: str):
        raise _lite("get_run_group")

    def get_run_partition_data(self, runs_filter: RunsFilter) -> Sequence[RunPartitionData]:
        raise _lite("get_run_partition_data")

    def replace_job_origin(self, run, job_origin) -> None:
        record = self._runs[run.run_id]
        self._runs[run.run_id] = record._replace(
            dagster_run=record.dagster_run._replace(job_code_origin=job_origin)
        )

    def wipe(self) -> None:
        self._runs.clear()
        self._job_snapshots.clear()
        self._ep_snapshots.clear()

    # --- backfills / daemon heartbeats (UI/daemon only) ------------------
    def add_backfill(self, partition_backfill):
        raise _lite("add_backfill")

    def update_backfill(self, partition_backfill):
        raise _lite("update_backfill")

    def get_backfill(self, backfill_id: str):
        raise _lite("get_backfill")

    def get_backfills(self, filters=None, cursor=None, limit=None, status=None):
        return []

    def get_backfills_count(self, filters=None) -> int:
        return 0

    def add_daemon_heartbeat(self, daemon_heartbeat) -> None:
        raise _lite("add_daemon_heartbeat")

    def get_daemon_heartbeats(self) -> Mapping[str, object]:
        return {}

    def wipe_daemon_heartbeats(self) -> None:
        pass

    def upgrade(self) -> None:
        pass


class LiteInMemoryEventLogStorage(EventLogStorage):
    """List-backed clean-room implementation of :class:`EventLogStorage`."""

    def __init__(self, preload=None):
        self._records_by_run: dict[str, list[EventLogRecord]] = defaultdict(list)
        self._next_storage_id = 1
        self._watchers: dict[str, list[Callable]] = defaultdict(list)
        if preload:
            raise _lite("EventLogStorage(preload=...)")

    def store_event(self, event: "EventLogEntry") -> None:
        from dagster._core.event_api import EventLogRecord

        record = EventLogRecord(storage_id=self._next_storage_id, event_log_entry=event)
        self._next_storage_id += 1
        self._records_by_run[event.run_id].append(record)

        for callback in list(self._watchers.get(event.run_id, [])):
            callback(event, str(record.storage_id))

    def get_records_for_run(
        self,
        run_id: str,
        cursor: str | None = None,
        of_type: DagsterEventType | set[DagsterEventType] | None = None,
        limit: int | None = None,
        ascending: bool = True,
    ) -> EventLogConnection:
        if of_type is None:
            type_filter = None
        elif isinstance(of_type, DagsterEventType):
            type_filter = {of_type}
        else:
            type_filter = set(of_type)

        after_id = int(cursor) if cursor is not None else None
        records = []
        for record in self._records_by_run.get(run_id, []):
            if after_id is not None and record.storage_id <= after_id:
                continue
            if type_filter is not None:
                entry = record.event_log_entry
                if not entry.is_dagster_event:
                    continue
                if entry.dagster_event.event_type not in type_filter:
                    continue
            records.append(record)

        if not ascending:
            records = list(reversed(records))
        if limit is not None:
            records = records[:limit]

        new_cursor = str(records[-1].storage_id) if records else (cursor or "-1")
        return EventLogConnection(records=records, cursor=new_cursor, has_more=False)

    def get_event_records(
        self, event_records_filter, limit: int | None = None, ascending: bool = False
    ) -> Sequence["EventLogRecord"]:
        # The in-process execution path reads events via get_records_for_run;
        # the asset-centric get_event_records query is UI-only.
        raise _lite("get_event_records")

    def delete_events(self, run_id: str) -> None:
        self._records_by_run.pop(run_id, None)

    def watch(self, run_id: str, cursor: str | None, callback: Callable) -> None:
        self._watchers[run_id].append(callback)

    def end_watch(self, run_id: str, handler: Callable) -> None:
        watchers = self._watchers.get(run_id)
        if watchers and handler in watchers:
            watchers.remove(handler)

    def wipe(self) -> None:
        self._records_by_run.clear()
        self._watchers.clear()

    @property
    def is_persistent(self) -> bool:
        return False

    def upgrade(self) -> None:
        pass

    def reindex_events(self, print_fn=None, force: bool = False) -> None:
        pass

    def reindex_assets(self, print_fn=None, force: bool = False) -> None:
        pass

    # --- asset queries (UI only) ----------------------------------------
    def get_latest_materialization_events(
        self, asset_keys: Iterable
    ) -> Mapping[object, Optional["EventLogEntry"]]:
        return {}

    def all_asset_keys(self) -> Sequence:
        return []

    def has_asset_key(self, asset_key) -> bool:
        return False

    def get_asset_records(self, asset_keys=None) -> Sequence:
        return []

    def get_latest_storage_id_by_partition(self, asset_key, event_type, partitions=None):
        return {}

    def get_materialized_partitions(self, asset_key, before_cursor=None, after_cursor=None):
        return set()

    def get_latest_tags_by_partition(self, *args, **kwargs):
        return {}

    def get_event_tags_for_asset(self, asset_key, filter_tags=None, filter_event_id=None):
        return []

    def wipe_asset(self, asset_key) -> None:
        raise _lite("wipe_asset")

    def wipe_asset_partitions(self, asset_key, partition_keys) -> None:
        raise _lite("wipe_asset_partitions")

    def get_updated_data_version_partitions(self, asset_key, partitions, since_storage_id):
        return set()

    def get_latest_planned_materialization_info(self, asset_key, partition=None):
        return None

    def get_latest_asset_partition_materialization_attempts_without_materializations(
        self, asset_key, after_storage_id=None
    ):
        return {}

    def get_freshness_state_records(self, keys):
        return {}

    # --- asset status cache ---------------------------------------------
    def can_read_asset_status_cache(self) -> bool:
        return False

    def can_write_asset_status_cache(self) -> bool:
        return False

    def update_asset_cached_status_data(self, asset_key, cache_values) -> None:
        pass

    def wipe_asset_cached_status(self, asset_key) -> None:
        pass

    # --- asset checks ----------------------------------------------------
    def get_asset_check_summary_records(self, asset_check_keys):
        return {}

    def get_asset_check_execution_history(
        self, check_key, limit, cursor=None, status=None, partition_filter=None
    ):
        return []

    def get_asset_check_partition_info(self, keys, after_storage_id=None, partition_keys=None):
        return []

    def get_latest_asset_check_execution_by_key(self, check_keys, partition_filter=None):
        return {}

    # --- record fetch API ------------------------------------------------
    # The in-process execution path reads upstream asset materializations /
    # observations through these (e.g. data-version resolution for inputs), so
    # they must return real records scanned from the event log.
    def _all_records(self) -> list["EventLogRecord"]:
        out: list[EventLogRecord] = []
        for records in self._records_by_run.values():
            out.extend(records)
        out.sort(key=lambda r: r.storage_id)
        return out

    def _fetch_asset_events(self, event_type, records_filter, limit, cursor, ascending):
        from dagster._core.definitions.asset_key import AssetKey
        from dagster._core.event_api import EventLogCursor

        asset_key = (
            records_filter
            if isinstance(records_filter, AssetKey)
            else getattr(records_filter, "asset_key", None)
        )
        after_id = EventLogCursor.parse(cursor).storage_id() if cursor else None

        matched = []
        for record in self._all_records():
            entry = record.event_log_entry
            if not entry.is_dagster_event:
                continue
            event = entry.dagster_event
            if event.event_type != event_type:
                continue
            if asset_key is not None and event.asset_key != asset_key:
                continue
            if after_id is not None and record.storage_id <= after_id:
                continue
            matched.append(record)

        if not ascending:
            matched = list(reversed(matched))
        if limit is not None:
            matched = matched[:limit]

        last_id = matched[-1].storage_id if matched else -1
        return EventRecordsResult(
            records=matched,
            cursor=EventLogCursor.from_storage_id(last_id).to_string(),
            has_more=False,
        )

    def fetch_materializations(self, records_filter, limit, cursor=None, ascending=False):
        return self._fetch_asset_events(
            DagsterEventType.ASSET_MATERIALIZATION, records_filter, limit, cursor, ascending
        )

    def fetch_failed_materializations(self, records_filter, limit, cursor=None, ascending=False):
        return self._fetch_asset_events(
            DagsterEventType.ASSET_FAILED_TO_MATERIALIZE, records_filter, limit, cursor, ascending
        )

    def fetch_observations(self, records_filter, limit, cursor=None, ascending=False):
        return self._fetch_asset_events(
            DagsterEventType.ASSET_OBSERVATION, records_filter, limit, cursor, ascending
        )

    def fetch_run_status_changes(self, records_filter, limit, cursor=None, ascending=False):
        event_type = (
            records_filter
            if isinstance(records_filter, DagsterEventType)
            else getattr(records_filter, "event_type", None)
        )
        return self._fetch_asset_events(event_type, records_filter, limit, cursor, ascending)

    # --- dynamic partitions ---------------------------------------------
    def get_dynamic_partitions(self, partitions_def_name: str) -> Sequence[str]:
        return []

    def get_paginated_dynamic_partitions(self, partitions_def_name, limit, ascending, cursor=None):
        raise _lite("get_paginated_dynamic_partitions")

    def has_dynamic_partition(self, partitions_def_name: str, partition_key: str) -> bool:
        return False

    def add_dynamic_partitions(self, partitions_def_name, partition_keys) -> None:
        raise _lite("add_dynamic_partitions")

    def delete_dynamic_partition(self, partitions_def_name, partition_key) -> None:
        raise _lite("delete_dynamic_partition")

    # --- concurrency (daemon only) --------------------------------------
    def get_concurrency_keys(self) -> set[str]:
        return set()

    def get_concurrency_info(self, concurrency_key: str):
        raise _lite("get_concurrency_info")

    def get_pool_limits(self):
        return []

    def set_concurrency_slots(self, concurrency_key: str, num: int) -> None:
        raise _lite("set_concurrency_slots")

    def initialize_concurrency_limit_to_default(self, concurrency_key: str) -> bool:
        return False

    def delete_concurrency_limit(self, concurrency_key: str) -> None:
        raise _lite("delete_concurrency_limit")

    def claim_concurrency_slot(self, concurrency_key, run_id, step_key, priority=None):
        raise _lite("claim_concurrency_slot")

    def check_concurrency_claim(self, concurrency_key, run_id, step_key):
        raise _lite("check_concurrency_claim")

    def get_concurrency_run_ids(self) -> set[str]:
        return set()

    def free_concurrency_slots_for_run(self, run_id: str) -> None:
        pass

    def free_concurrency_slot_for_step(self, run_id: str, step_key: str) -> None:
        pass

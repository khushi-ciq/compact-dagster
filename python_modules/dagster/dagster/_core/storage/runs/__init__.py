from dagster._core.storage.runs.base import RunStorage as RunStorage

# [lite-engine] SQL-backed run storages import sqlalchemy; optional for the
# in-process lite engine (uses the dependency-free LiteInMemoryRunStorage).
try:
    from dagster._core.storage.runs.in_memory import InMemoryRunStorage as InMemoryRunStorage
    from dagster._core.storage.runs.schema import (
        DaemonHeartbeatsTable as DaemonHeartbeatsTable,
        InstanceInfo as InstanceInfo,
        RunStorageSqlMetadata as RunStorageSqlMetadata,
    )
    from dagster._core.storage.runs.sql_run_storage import SqlRunStorage as SqlRunStorage
    from dagster._core.storage.runs.sqlite import SqliteRunStorage as SqliteRunStorage
except ImportError:
    pass

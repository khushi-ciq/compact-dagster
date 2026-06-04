"""[lite-engine] Dependency-free storage migration types.

``AlembicVersion`` is just a tuple type, but it historically lived in
``dagster._core.storage.sql`` — a module that eagerly imports sqlalchemy +
alembic. The storage *interfaces* (RunStorage / EventLogStorage /
ScheduleStorage base classes) only need the type alias, so hosting it here lets
those bases — and the clean-room in-memory stores — load without dragging in
the SQL stack.
"""

from typing import TypeAlias

AlembicVersion: TypeAlias = tuple[str | None, str | tuple[str, ...] | None]

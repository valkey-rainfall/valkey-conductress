"""Task queue for benchmark tasks"""

import json
import logging
import os
import re
import tempfile
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from dataclasses import fields as dataclasses_fields
from datetime import datetime
from importlib import import_module
from pathlib import Path
from typing import Callable, ClassVar, Dict, Optional, Type

from . import config
from .file_protocol import FileProtocol
from .topology import TopologySpec
from .utility import datetime_to_task_id

_ENVELOPE_TASK_ID_KEY = "__envelope_task_id"
_ENVELOPE_TASK_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]*$")
_ENVELOPE_TASK_ID_MAX_LENGTH = 200


def _validate_envelope_task_id(task_id: str) -> str:
    """Validate an authenticated or locally persisted envelope task ID."""
    if not isinstance(task_id, str) or not task_id:
        raise ValueError("envelope task ID must be a non-empty string")
    if len(task_id) > _ENVELOPE_TASK_ID_MAX_LENGTH:
        raise ValueError(f"envelope task ID exceeds {_ENVELOPE_TASK_ID_MAX_LENGTH} characters")
    if _ENVELOPE_TASK_ID_PATTERN.fullmatch(task_id) is None:
        raise ValueError("envelope task ID contains unsafe characters")
    return task_id


logger = logging.getLogger(__name__)


def _upgrade_topology_keys(data: dict) -> None:
    """Rewrite a task document's layout keys in place to the current ``topology`` form.

    Documents written before the ``topology`` field carried ``replicas: N``,
    meaning N replicas on the next N configured hosts (``N <= 0``: none). A
    document carrying both keeps ``topology`` and drops ``replicas``. A
    document with neither is the single-instance layout every task has always
    meant by default.
    """
    replicas = data.pop("replicas", None)
    if "topology" in data:
        data["topology"] = TopologySpec.from_dict(data["topology"])
    elif replicas is not None:
        try:
            data["topology"] = TopologySpec.replication_hosts(int(replicas))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Invalid task data: replicas must be an integer, got {replicas!r}") from exc
    else:
        data["topology"] = TopologySpec.standalone()


def _drop_unknown_task_fields(task_class: Type["BaseTaskData"], task_type: str, data: dict) -> dict:
    """Return ``data`` with any keys the task constructor does not accept removed.

    A newer submitter may add envelope-body fields that a task dataclass on an
    older deployment does not define. Passing them straight to the constructor
    raises ``TypeError`` and wedges the queue. Instead, keep only the fields the
    dataclass declares as constructor arguments and warn about the rest, so an
    unrecognised field degrades to being ignored rather than fatal.
    """
    accepted = {field_def.name for field_def in dataclasses_fields(task_class) if field_def.init}
    unknown = [key for key in data if key not in accepted]
    if unknown:
        logger.warning(
            "task type %s: ignoring unrecognized field(s): %s",
            task_type,
            ", ".join(sorted(unknown)),
        )
        data = {key: value for key, value in data.items() if key in accepted}
    return data


@dataclass
class BaseTaskData(ABC):
    """Task for benchmarking"""

    source: str
    specifier: str
    # The server layout the task runs against: which instances, on which
    # hosts and ports, in which roles. ``TopologySpec.standalone()`` is one
    # instance on the runner with the legacy working directory and logfile,
    # the shape every sweep cell measures. Documents from before this field
    # carried ``replicas: N`` (N extra configured hosts); ``from_dict`` maps
    # that to ``TopologySpec.replication_hosts(N)``.
    topology: TopologySpec
    note: str
    requirements: dict
    make_args: str
    task_type: str = field(init=False)
    timestamp: datetime = field(
        default_factory=datetime.now,
        init=False,  # This prevents it from being a constructor argument
    )

    __task_registry: ClassVar[Dict[str, Type["BaseTaskData"]]] = {}

    # Fields left out of the saved and submitted document while they hold their default, so a task that does not
    # use an optional feature keeps the document it always had.
    _OMIT_WHEN_DEFAULT: ClassVar[tuple[str, ...]] = ()

    @classmethod
    def register_tasks(cls):
        """Dynamically import all task modules to register them."""
        tasks_dir = Path(__file__).parent / "tasks"
        for task_file in tasks_dir.glob("task_*.py"):
            module_name = task_file.stem
            logger.info("Importing task module: %s", module_name)
            import_module(f"conductress.tasks.{module_name}")

    def __init_subclass__(cls, **kwargs):
        """Register subclasses in the task registry."""
        super().__init_subclass__(**kwargs)
        if cls.__name__ not in BaseTaskData.__task_registry:
            BaseTaskData.__task_registry[cls.__name__] = cls

    def __post_init__(self):
        self.task_type = self.__class__.__name__
        # Explicit task-ID override set by the trusted remote envelope.
        # Initialised here (not as a dataclass field) so it never appears in
        # __init__(), asdict(), or subclass constructors.
        if not hasattr(self, "_override_task_id"):
            self._override_task_id: Optional[str] = None
        self.topology = TopologySpec.from_dict(self.topology)
        extra = list(self._extra_sources())
        if self.source != config.MANUALLY_UPLOADED and self.source not in config.REPO_NAMES + extra:
            raise ValueError(
                f"Unknown source: {self.source}. Valid: {config.REPO_NAMES + [config.MANUALLY_UPLOADED] + extra}"
            )

    def __eq__(self, other):
        if not isinstance(other, BaseTaskData):
            return False
        return self.timestamp == other.timestamp

    @property
    def task_id(self) -> str:
        """Return the canonical task_id.

        When a trusted remote envelope supplies an explicit ID (e.g. canary
        scheduler's deterministic ``canary:<runner>:<profile>:<date>``), that
        override takes precedence over the timestamp-derived default.
        """
        if self._override_task_id is not None:
            return self._override_task_id
        return datetime_to_task_id(self.timestamp)

    @abstractmethod
    def short_description(self) -> str:
        """Return a short description of the task."""
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    def prepare_task_runner(self, server_infos: list[config.ServerInfo]) -> "BaseTaskRunner":
        """Return the task runner for this task."""
        raise NotImplementedError("Subclasses must implement this method.")

    def _extra_sources(self) -> tuple[str, ...]:
        """Sources this task type accepts besides repositories and manually_uploaded."""
        return ()

    def to_document(self) -> dict:
        """The task as a plain dict (``asdict``), minus the ``_OMIT_WHEN_DEFAULT`` fields that hold their default."""
        data = asdict(self)
        for field_def in dataclasses_fields(self):
            if field_def.name in self._OMIT_WHEN_DEFAULT and data.get(field_def.name) == field_def.default:
                del data[field_def.name]
        return data

    def save_to_file(self, filepath: Path):
        """Save the task to a JSON file"""
        data = self.to_document()
        data["timestamp"] = self.timestamp.isoformat()
        # Persist the envelope-supplied override alongside the task body so it
        # survives restart / from_file reload.  The key is prefixed with "__"
        # to avoid collisions with task-type fields.
        if self._override_task_id is not None:
            data[_ENVELOPE_TASK_ID_KEY] = self._override_task_id

        with filepath.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

    @classmethod
    def from_dict(cls, document: dict, *, envelope_task_id: Optional[str] = None) -> "BaseTaskData":
        """Deserialize a task from an in-memory task document.

        ``envelope_task_id`` is the authenticated identity supplied by the
        caller. The reserved persistence sidecar in ``document`` is always
        ignored here; only :meth:`from_file` may restore it from trusted local
        queue storage.
        """
        if not isinstance(document, dict):
            raise ValueError("Invalid task data: expected object")
        data = dict(document)
        data.pop(_ENVELOPE_TASK_ID_KEY, None)
        try:
            timestamp = datetime.fromisoformat(data.pop("timestamp"))
            task_type = data.pop("task_type")
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"Invalid task data: {exc}") from exc
        if task_type not in BaseTaskData.__task_registry:
            raise ValueError(f"Unknown task type: {task_type}")
        _upgrade_topology_keys(data)
        task_class = BaseTaskData.__task_registry[task_type]
        data = _drop_unknown_task_fields(task_class, task_type, data)
        result = task_class(**data)
        result.timestamp = timestamp
        if envelope_task_id is not None:
            result._override_task_id = _validate_envelope_task_id(envelope_task_id)
        return result

    @classmethod
    def from_file(cls, filepath: Path) -> "BaseTaskData":
        """Load a task from a JSON file"""
        try:
            with filepath.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except FileNotFoundError as exc:
            raise FileNotFoundError(f"Task file not found: {filepath}") from exc
        except json.decoder.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSON in file: {filepath}") from exc
        if not isinstance(data, dict):
            raise ValueError(f"Invalid task data in file: {filepath}")

        persisted_override = data.get(_ENVELOPE_TASK_ID_KEY)
        return cls.from_dict(data, envelope_task_id=persisted_override)


class TaskCancelled(Exception):
    """A task stopped because the control service asked it to (an operator cancelled it)."""


class BaseTaskRunner(ABC):
    """Base class for task runners"""

    def __init__(self, task_name: str):
        self.task_name = task_name
        # Set by TaskRunner for a remote task: returns True when the control service
        # wants the task stopped. None for a local task, which cannot be cancelled remotely.
        self.cancel_check: Optional[Callable[[], bool]] = None
        self.file_protocol = FileProtocol(task_name, role_id="client")
        # Cores the runner process is pinned to for this task ("" if unpinned);
        # set by TaskRunner before run(). A task that checks for foreign CPU
        # activity treats them as claimed.
        self.management_cpus: str = ""

    def check_cancel(self) -> None:
        """Stop here if the task was cancelled. Call it between repetitions, never inside a measurement."""
        check = self.cancel_check  # TaskRunner assigns a function; pylint only sees the None default
        if check is not None and check():  # pylint: disable=not-callable
            raise TaskCancelled("cancelled by the operator")

    @abstractmethod
    async def run(self) -> None:
        """Run the task"""
        raise NotImplementedError("Subclasses must implement this method.")


class TaskQueue:
    """Task queue for benchmark tasks"""

    def __init__(self, queue_dir=config.CONDUCTRESS_QUEUE):
        self.queue_dir = Path(queue_dir)
        self.queue_dir.mkdir(parents=True, exist_ok=True)

    def submit_task(self, task: BaseTaskData) -> None:
        """Add a new task to the queue"""
        task_file = self.task_path(task.task_id)
        task.save_to_file(task_file)

    def task_path(self, task_id: str) -> Path:
        return self.queue_dir / f"task_{task_id}.json"

    def has_task(self, task_id: str) -> bool:
        return self.task_path(task_id).exists()

    def import_task(self, document: dict, *, envelope_task_id: Optional[str] = None) -> BaseTaskData:
        """Validate and atomically install a serialized task document.

        ``envelope_task_id`` is the **trusted** identity from the remote
        envelope.  When supplied, the task is filed under that ID (both
        in-memory and on disk) regardless of the timestamp-derived ID
        inside the untrusted document body.  It is persisted as a sidecar
        ``__envelope_task_id`` key so that :meth:`from_file` restores it
        after a restart.
        """
        if _ENVELOPE_TASK_ID_KEY in document:
            raise ValueError(f"reserved task field is not allowed: {_ENVELOPE_TASK_ID_KEY}")
        task = BaseTaskData.from_dict(document, envelope_task_id=envelope_task_id)
        # Build the on-disk document: original body + authenticated sidecar.
        persisted = dict(document)
        if envelope_task_id is not None:
            persisted[_ENVELOPE_TASK_ID_KEY] = task.task_id
        task_file = self.queue_dir / f"task_{task.task_id}.json"
        serialized = json.dumps(persisted, indent=2)
        if task_file.exists():
            existing = json.loads(task_file.read_text(encoding="utf-8"))
            if existing != persisted:
                raise ValueError(f"task file already exists with different content: {task.task_id}")
            return task

        temporary_path = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=self.queue_dir,
                prefix=f".task_{task.task_id}.",
                suffix=".tmp",
                delete=False,
            ) as temporary:
                temporary.write(serialized)
                temporary.flush()
                os.fsync(temporary.fileno())
                temporary_path = Path(temporary.name)
            os.replace(temporary_path, task_file)
            directory_fd = os.open(self.queue_dir, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)
        return task

    def get_next_task(self) -> Optional[BaseTaskData]:
        """Get the next task from the queue"""
        tasks = sorted(self.queue_dir.glob("task_*.json"))
        if not tasks:
            return None

        task_file = tasks[0]
        try:
            task = BaseTaskData.from_file(task_file)
            return task
        except (json.JSONDecodeError, FileNotFoundError):
            # Handle corrupted task files
            if task_file.exists():
                logger.error("unable to read - skipping %s", task_file)
                task_file.unlink()
            return None
        except ValueError as exc:
            # The file is well-formed but this code cannot run it: a task type
            # that no longer exists, or a source this runner does not know.
            # Left in place it would head the queue forever, so set it aside
            # under a name the queue does not scan and let the queue proceed.
            if task_file.exists():
                parked = task_file.with_name(task_file.name + ".unsupported")
                logger.error("cannot load %s (%s) - setting aside as %s", task_file.name, exc, parked.name)
                task_file.rename(parked)
            return None

    def finish_task(self, task: BaseTaskData) -> None:
        """Delete a task from the queue, indicating it has been completed"""
        task_file = self.queue_dir / f"task_{task.task_id}.json"
        if task_file.exists():
            task_file.unlink()
        else:
            logger.error(
                "Task file not found (task_id=%s, expected=%s). " "This is a bug — task_id does not match filename.",
                task.task_id,
                task_file,
            )
            # Attempt to find and remove the file by matching timestamp content
            for candidate in self.queue_dir.glob("task_*.json"):
                try:
                    data = json.loads(candidate.read_text())
                    if data.get("timestamp") == task.timestamp.isoformat():
                        logger.error("Found matching file by timestamp: %s — removing", candidate)
                        candidate.unlink()
                        return
                except (json.JSONDecodeError, OSError):
                    continue
            logger.error("Could not find any matching task file to remove")

    def get_all_tasks(self) -> list[BaseTaskData]:
        """Returns list of (timestamp, task) tuples, sorted by timestamp"""
        tasks = []
        for task_file in self.queue_dir.glob("task_*.json"):
            try:
                task = BaseTaskData.from_file(task_file)
                tasks.append(task)
            except (ValueError, json.JSONDecodeError, FileNotFoundError):
                continue

        return sorted(tasks, key=lambda x: x.timestamp)

    def get_queue_length(self) -> int:
        """Get the number of tasks in the queue"""
        return len(list(self.queue_dir.glob("task_*.json")))

    def remove_task(self, task_id: str) -> bool:
        """Remove a task from the queue by task_id.

        Returns True if the task was removed, False otherwise.
        """
        task_file = self.queue_dir / f"task_{task_id}.json"
        if task_file.exists():
            task_file.unlink()
            return True
        return False


BaseTaskData.register_tasks()

"""Task queue for benchmark tasks"""

import json
import logging
import os
import tempfile
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from datetime import datetime
from importlib import import_module
from pathlib import Path
from typing import ClassVar, Dict, Optional, Type

from . import config
from .file_protocol import FileProtocol
from .utility import datetime_to_task_id

logger = logging.getLogger(__name__)


@dataclass
class BaseTaskData(ABC):
    """Task for benchmarking"""

    source: str
    specifier: str
    replicas: int
    note: str
    requirements: dict
    make_args: str
    task_type: str = field(init=False)
    timestamp: datetime = field(
        default_factory=datetime.now,
        init=False,  # This prevents it from being a constructor argument
    )

    __task_registry: ClassVar[Dict[str, Type["BaseTaskData"]]] = {}

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
        if self.source != config.MANUALLY_UPLOADED and self.source not in config.REPO_NAMES:
            raise ValueError(f"Unknown source: {self.source}. Valid: {config.REPO_NAMES + [config.MANUALLY_UPLOADED]}")

    def __eq__(self, other):
        if not isinstance(other, BaseTaskData):
            return False
        return self.timestamp == other.timestamp

    @property
    def task_id(self) -> str:
        """Return the canonical task_id (timestamp in standard format)."""
        return datetime_to_task_id(self.timestamp)

    @abstractmethod
    def short_description(self) -> str:
        """Return a short description of the task."""
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    def prepare_task_runner(self, server_infos: list[config.ServerInfo]) -> "BaseTaskRunner":
        """Return the task runner for this task."""
        raise NotImplementedError("Subclasses must implement this method.")

    def save_to_file(self, filepath: Path):
        """Save the task to a JSON file"""
        data = asdict(self)
        data["timestamp"] = self.timestamp.isoformat()

        with filepath.open("w") as f:
            json.dump(data, f, indent=2)

    @classmethod
    def from_dict(cls, document: dict) -> "BaseTaskData":
        """Deserialize a task from an in-memory task document."""
        if not isinstance(document, dict):
            raise ValueError("Invalid task data: expected object")
        data = dict(document)
        try:
            timestamp = datetime.fromisoformat(data.pop("timestamp"))
            task_type = data.pop("task_type")
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"Invalid task data: {exc}") from exc
        if task_type not in BaseTaskData.__task_registry:
            raise ValueError(f"Unknown task type: {task_type}")
        result = BaseTaskData.__task_registry[task_type](**data)
        result.timestamp = timestamp
        return result

    @classmethod
    def from_file(cls, filepath: Path) -> "BaseTaskData":
        """Load a task from a JSON file"""
        try:
            with filepath.open("r") as f:
                data = json.load(f)
        except FileNotFoundError as exc:
            raise FileNotFoundError(f"Task file not found: {filepath}") from exc
        except json.decoder.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSON in file: {filepath}") from exc
        if not isinstance(data, dict):
            raise ValueError(f"Invalid task data in file: {filepath}")

        return cls.from_dict(data)


class BaseTaskRunner(ABC):
    """Base class for task runners"""

    def __init__(self, task_name: str):
        self.task_name = task_name
        self.file_protocol = FileProtocol(task_name, role_id="client")

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

    def import_task(self, document: dict) -> BaseTaskData:
        """Validate and atomically install a serialized task document."""
        task = BaseTaskData.from_dict(document)
        task_file = self.queue_dir / f"task_{task.task_id}.json"
        serialized = json.dumps(document, indent=2)
        if task_file.exists():
            existing = json.loads(task_file.read_text(encoding="utf-8"))
            if existing != document:
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

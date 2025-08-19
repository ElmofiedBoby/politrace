import uuid
from enum import Enum
from datetime import datetime, timezone
from pydantic import BaseModel
from pydantic import Field
import json
from typing import Iterable, Optional
import redis

class JobType(int, Enum):
    INGEST = 0

class JobStatus(int, Enum):
    PENDING = 0
    RUNNING = 1
    COMPLETED = 2
    FAILED = 3
    CANCELLED = 4

class Job(BaseModel):
    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    type: JobType
    status: JobStatus = JobStatus.PENDING
    status_desc: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = None
    percent: float = 0.0
    content: Optional[dict] = None

    def update(self, status: Optional[JobStatus] = None, status_desc: Optional[str] = None, percent: Optional[float] = None, content: Optional[dict] = None):
        if status is None and status_desc is None and percent is None:
            raise ValueError("At least one of status, status_desc, or percent must be provided.")
        if status is not None:
            self.status = status
        if status_desc is not None and status_desc != "":
            self.status_desc = status_desc
        if percent is not None:
            if not (0.0 <= percent <= 100.0):
                raise ValueError("Percent must be between 0.0 and 100.0.")
            self.percent = percent
        if content is not None:
            self.content = content

        self.updated_at = datetime.now(timezone.utc)

    # -------- Serialization helpers for Redis --------
    def to_redis_json(self) -> str:
        data = self.model_dump()
        # Convert complex types for JSON storage
        data["id"] = str(self.id)
        data["type"] = int(self.type)
        data["status"] = int(self.status)
        data["created_at"] = self.created_at.isoformat()
        data["updated_at"] = self.updated_at.isoformat() if self.updated_at else None
        try:
            data["content"] = self.content if self.content is not None else {}
            # validate JSON-serializable
            json.dumps(data["content"])
        except Exception:
            data["content"] = str(self.content)
        return json.dumps(data)

    @classmethod
    def from_redis_json(cls, json_str: str) -> "Job":
        raw = json.loads(json_str)
        raw["id"] = uuid.UUID(raw["id"]) if isinstance(raw.get("id"), str) else raw.get("id")
        raw["type"] = JobType(int(raw["type"]))
        raw["status"] = JobStatus(int(raw["status"]))
        raw["created_at"] = datetime.fromisoformat(raw["created_at"]) if raw.get("created_at") else None
        raw["updated_at"] = datetime.fromisoformat(raw["updated_at"]) if raw.get("updated_at") else None
        raw["content"] = raw.get("content", {})
        return cls(**raw)

class JobSummary(BaseModel):
    id: str
    type: int
    status: int
    status_desc: Optional[str] = None
    percent: float

    @classmethod
    def from_job(cls, j: Job) -> "JobSummary":
        return cls(
            id=str(j.id),
            type=int(j.type),
            status=int(j.status),
            status_desc=j.status_desc,
            percent=j.percent,
        )

# ---------------- Redis-backed job store ----------------
class RedisJobStore:
    """Lightweight Redis persistence for Job objects.

    Keys:
      - job:<id> -> JSON blob for the Job
      - jobs:index -> a Set of all job IDs (strings)
    Optionally, you can set a TTL per job via `ttl_seconds` when saving.
    """

    def __init__(self, redis_client: Optional[redis.Redis] = None, *, namespace: str = "jobs"):
        self.r = redis_client or redis.Redis(host="localhost", port=6379, decode_responses=True)
        self.ns = namespace.rstrip(":")

    def _job_key(self, job_id: uuid.UUID) -> str:
        return f"{self.ns}:job:{job_id}"

    def _index_key(self) -> str:
        return f"{self.ns}:index"

    # ---- CRUD ----
    def create(self, job: Job, *, ttl_seconds: Optional[int] = None) -> Job:
        key = self._job_key(job.id)
        self.r.set(key, job.to_redis_json())
        if ttl_seconds:
            self.r.expire(key, ttl_seconds)
        self.r.sadd(self._index_key(), str(job.id))
        return job

    def get(self, job_id: uuid.UUID) -> Optional[Job]:
        raw = self.r.get(self._job_key(job_id))
        return Job.from_redis_json(raw) if raw else None

    def update(self, job_id: uuid.UUID, *, status: Optional[JobStatus] = None, status_desc: Optional[str] = None, percent: Optional[float] = None) -> Job:
        job = self.get(job_id)
        if not job:
            raise KeyError(f"Job {job_id} not found")
        job.update(status=status, status_desc=status_desc, percent=percent)
        self.r.set(self._job_key(job.id), job.to_redis_json())
        return job

    def delete(self, job_id: uuid.UUID) -> None:
        self.r.delete(self._job_key(job_id))
        self.r.srem(self._index_key(), str(job_id))

    def list(self) -> Iterable[Job]:
        for id_str in self.r.smembers(self._index_key()):
            raw = self.r.get(self._job_key(uuid.UUID(id_str)))
            if raw:
                yield Job.from_redis_json(raw)

    # Convenience helpers
    def start(self, job_id: uuid.UUID, desc: Optional[str] = None) -> Job:
        return self.update(job_id, status=JobStatus.RUNNING, status_desc=desc)

    def complete(self, job_id: uuid.UUID, desc: Optional[str] = None) -> Job:
        return self.update(job_id, status=JobStatus.COMPLETED, status_desc=desc, percent=100.0)

    def fail(self, job_id: uuid.UUID, desc: Optional[str] = None) -> Job:
        return self.update(job_id, status=JobStatus.FAILED, status_desc=desc)

    def cancel(self, job_id: uuid.UUID, desc: Optional[str] = None) -> Job:
        return self.update(job_id, status=JobStatus.CANCELLED, status_desc=desc)

    def progress(self, job_id: uuid.UUID, percent: float, desc: Optional[str] = None) -> Job:
        return self.update(job_id, percent=percent, status_desc=desc)
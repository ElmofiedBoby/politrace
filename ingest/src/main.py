import time
import uuid
from typing import List
from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel

from graph import insert_many, wipe_neo4j_database
from jobs import Job, JobStatus, JobSummary, JobType, RedisJobStore
from parsers.legislators import LegislatorParser

app = FastAPI()
store = RedisJobStore()

@app.get("/")
def read_root():
    return {"message": "Welcome to the FastAPI app!"}

@app.get("/jobs", response_model=List[JobSummary])
def get_jobs():
    return [JobSummary.from_job(j) for j in store.list()]

@app.get("/jobs/{job_id}", response_model=JobSummary)
def get_job(job_id: str):
    try:
        jid = uuid.UUID(job_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid job id")
    job = store.get(jid)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return JobSummary.from_job(job)

# ---- Background task that performs the ingest and updates the Redis-backed job ----
def _ingest_legislators_task(job_id: uuid.UUID):
    try:
        store.start(job_id, "Loading legislators into memory...")
        parser = LegislatorParser(include_current=True, include_historical=True)
        parser.load()
        total = len(parser.legislators)
        store.progress(job_id, 10.0, f"Loaded {total} legislators into memory")

        store.progress(job_id, 20.0, "Wiping Neo4j database...")
        wipe_neo4j_database(batch_size=20000, use_apoc=True)

        store.progress(job_id, 30.0, "Inserting legislators into Neo4j...")
        start = time.time()
        insert_many(
            entries=parser.legislators,
            src=parser.get_source(),
            render_visual=False,
            workers=12,
            db_uri="bolt://localhost:7687",
            db_user="neo4j",
            db_pass="your_password",
        )
        elapsed = time.time() - start
        store.complete(job_id, f"Inserted {total} legislators in {elapsed:.2f}s")
    except Exception as e:
        store.fail(job_id, f"Ingest failed: {e}")

@app.post("/ingest/legislators")
def ingest_legislators(background_tasks: BackgroundTasks):
    # Create a pending job and enqueue background work
    job = Job(type=JobType.INGEST, status_desc="Queued", percent=0.0)
    store.create(job)
    background_tasks.add_task(_ingest_legislators_task, job.id)
    return {"job_id": str(job.id)}
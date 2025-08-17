import time
from fastapi import FastAPI

from graph import insert_many, wipe_neo4j_database
from parsers.legislators import LegislatorParser

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "Welcome to the FastAPI app!"}

@app.get("/jobs")
def get_jobs():
    return {"jobs": []}

@app.post("/ingest/legislators")
def ingest_legislators():
    print("Starting to load legislators into memory...")
    parser = LegislatorParser(include_current=True, include_historical=True)
    parser.load()
    print("Loaded ", len(parser.legislators), " legislators.")

    print("Wiping Neo4j database...")
    wipe_neo4j_database(batch_size=20000, use_apoc=True)
    print("Wiped Neo4j database. Inserting legislators...")
    start = time.time()
    summary = insert_many(
        entries=parser.legislators,
        src=parser.get_source(),
        render_visual=False,
        workers=12,
        db_uri="bolt://localhost:7687",
        db_user="neo4j",
        db_pass="your_password",
    )

    return {"message": f"Inserted {len(parser.legislators)} legislators in {time.time() - start} seconds."}
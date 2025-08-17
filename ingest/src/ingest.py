from parsers.legislators import LegislatorParser
from graph import insert_many, wipe_neo4j_database
import logging
import time

#logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

print("Starting to load legislators into memory...")
parser = LegislatorParser(include_current=True, include_historical=True)
parser.load()
print("Loaded ", len(parser.legislators), " legislators.")

logger.info("Wiping Neo4j database...")
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
print(f"Inserted {len(parser.legislators)} legislators in", time.time() - start, "seconds.")

# print(summary["count"], "processed")
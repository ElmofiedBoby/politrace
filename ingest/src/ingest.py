from parsers.legislators import LegislatorParser
from graph import insert_data, wipe_neo4j_database
import logging

#logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

parser = LegislatorParser(include_current=True, include_historical=True)
parser.load()
length = len(parser.legislators)

logger.info("Wiping Neo4j database and inserting legislator data...")
wipe_neo4j_database()
for idx, legislator in enumerate(parser.legislators):
    print(f"Inserted {idx + 1}/{length} legislator(s) into Neo4j database.")
    insert_data(
        entry=legislator,
        src=parser.get_source(),
        render_visual=False
    )
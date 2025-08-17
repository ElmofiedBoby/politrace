from parsers.legislators import LegislatorParser
from graph import insert_data, wipe_neo4j_database

parser = LegislatorParser()
parser.load()

wipe_neo4j_database()
for legislator in parser.legislators:
    insert_data(
        entry=legislator,
        src=parser.get_source(),
        render_visual=False
    )
    
print(parser.legislators[0])


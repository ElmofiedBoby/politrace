"""
Automatic graph generator for congress-legislators entries → Neo4j (temporal/event model).

- Creates/updates anchor nodes: Person, Organization (Party), Geography
- Creates temporal event nodes per term: OfficeTerm, PartyAffiliation
- Attaches provenance via Source nodes

Idempotent: event nodes have deterministic `key` hashes so re-running won’t duplicate.

Requirements:
    pip install neo4j python-dateutil
    # For visualization:
    pip install networkx matplotlib

Usage example (see bottom):
    from neo4j import GraphDatabase
    driver = GraphDatabase.driver(NEO4J_URI, auth=(USER, PASS))
    with driver.session() as s:
        install_constraints(s)
        ingest_legislator(s, entry, source_meta)

Tested on Neo4j 5.x. For 4.x, replace composite constraints with single-property `key`.
"""
from __future__ import annotations

import json
from neo4j import GraphDatabase
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha1
from typing import Any, Dict, Iterable, Optional
import logging
# Concurrency-related imports
from concurrent.futures import ThreadPoolExecutor, as_completed
from neo4j.exceptions import TransientError, ServiceUnavailable
import threading
import time
#logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Thread-safe, install-once guard for Neo4j constraints
_CONSTRAINTS_INSTALLED = False
_CONSTRAINTS_LOCK = threading.Lock()

def _ensure_constraints(driver) -> None:
    """Install constraints once per-process in a thread-safe way."""
    global _CONSTRAINTS_INSTALLED
    if _CONSTRAINTS_INSTALLED:
        return
    with _CONSTRAINTS_LOCK:
        if _CONSTRAINTS_INSTALLED:
            return
        with driver.session() as s:
            install_constraints(s)
        _CONSTRAINTS_INSTALLED = True

def _retry(fn, retries: int = 3, base_sleep: float = 0.25):
    """Exponential backoff retry for transient Neo4j errors."""
    attempt = 0
    while True:
        try:
            return fn()
        except (TransientError, ServiceUnavailable) as e:
            attempt += 1
            if attempt > retries:
                raise
            time.sleep(base_sleep * (2 ** (attempt - 1)))

# -----------------------
# Helpers & normalization
# -----------------------

PARTY_MAP = {
    "Democrat": ("DEM", "Democratic Party"),
    "Republican": ("REP", "Republican Party"),
    "Independent": ("IND", "Independent"),
}


def norm_party(party: Optional[str]) -> tuple[str, str]:
    if not party:
        return ("UNK", "Unknown")
    if party in PARTY_MAP:
        return PARTY_MAP[party]
    # Fallbacks (e.g., "Democratic-Farmer-Labor")
    up = party.strip().upper()
    if up.startswith("DEM"):
        return ("DEM", party)
    if up.startswith("REP"):
        return ("REP", party)
    if up.startswith("IND"):
        return ("IND", party)
    return (up[:3], party)


def fact_key(*parts: Any) -> str:
    """Stable SHA1 key for idempotent MERGE of event nodes."""
    s = "|".join("" if p is None else str(p) for p in parts)
    return sha1(s.encode("utf-8")).hexdigest()


@dataclass
class SourceMeta:
    source_id: str
    url: str
    publisher: str = "congress-legislators"
    retrieved_at: str = datetime.utcnow().isoformat()  # ISO8601


# -----------------------
# Schema setup (constraints/indexes)
# -----------------------

def install_constraints(session) -> None:
    """Create uniqueness constraints (Neo4j 5.x). Safe to run repeatedly."""
    constraint_queries = [
        "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Person) REQUIRE p.bioguide_id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (s:Source) REQUIRE s.source_id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (o:Organization) REQUIRE (o.org_type, o.short) IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (g:Geography) REQUIRE (g.kind, g.code) IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (e:OfficeTerm) REQUIRE e.key IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (e:PartyAffiliation) REQUIRE e.key IS UNIQUE"
    ]
    for query in constraint_queries:
        session.run(query)


# -----------------------
# Ingest functions
# -----------------------

def get_all_ids(entry: Dict[str, Any]) -> Dict[str, Any]:
    """Helper to consistently resolve all ids from an entry."""
    ids = entry.get("ids", {}).copy()
    if "bioguide_id" in entry:
        ids["bioguide"] = entry["bioguide_id"]
    return ids

def get_bioguide(entry: Dict[str, Any]) -> Optional[str]:
    """Helper to consistently resolve the bioguide id from an entry."""
    ids = get_all_ids(entry)
    return ids.get("bioguide")

def upsert_source(session, src: SourceMeta) -> None:
    session.run(
        """
        MERGE (s:Source {source_id:$source_id})
        ON CREATE SET s.url=$url, s.publisher=$publisher, s.retrieved_at=datetime($retrieved_at)
        """,
        {
            "source_id": src.source_id,
            "url": src.url,
            "publisher": src.publisher,
            "retrieved_at": src.retrieved_at,
        },
    )


def upsert_person(session, entry: Dict[str, Any]) -> None:
    ids = entry.get("ids", {})
    name = entry.get("name", {})
    bio = entry.get("bio", {})

    session.run(
        """
        MERGE (p:Person {bioguide_id:$bioguide_id})
        ON CREATE SET p.name_first=$first, p.name_last=$last, p.name_full=$full,
                      p.birthdate = CASE WHEN $birthday IS NULL THEN NULL ELSE date($birthday) END,
                      p.gender=$gender
        SET p.identifiers = $identifiers
        """,
        {
            "bioguide_id": entry.get("bioguide_id") or ids.get("bioguide"),
            "first": name.get("first"),
            "last": name.get("last"),
            "full": name.get("official_full") or f"{name.get('first', '')} {name.get('last', '')}".strip(),
            "birthday": bio.get("birthday"),
            "gender": bio.get("gender"),
            "identifiers": json.dumps(ids)
        },
    )


def upsert_term_events(session, person_bioguide: str, term: Dict[str, Any], src: SourceMeta) -> None:
    """Create OfficeTerm + PartyAffiliation for a single term block."""
    term_type = term.get("type")  # 'rep' or 'sen'
    chamber = "HOUSE" if term_type == "rep" else "SENATE"
    state = term.get("state")
    district_val = int(term["district"]) if term_type == "rep" and "district" in term else None
    sen_class = term.get("class") if term_type == "sen" else None
    start = term.get("start")
    end = term.get("end")

    party_short, party_full = norm_party(term.get("party"))

    # Geography node
    if chamber == "HOUSE":
        geo_kind = "DISTRICT"
        d_code = "AL" if (district_val in (None, -1, 0)) else str(district_val)
        geo_code = f"{state}-{d_code}"
        geo_name = geo_code
    else:
        geo_kind = "STATE"
        geo_code = state
        geo_name = state

    now_iso = datetime.utcnow().isoformat()

    office_key = fact_key("OfficeTerm", person_bioguide, chamber, state, district_val, sen_class, start, end)
    party_key = fact_key("PartyAffiliation", person_bioguide, party_short, start, end)

    params = {
        "source_id": src.source_id,
        "source_url": src.url,
        "source_publisher": src.publisher,
        "retrieved_at": src.retrieved_at,

        "bioguide_id": person_bioguide,
        "geo_kind": geo_kind,
        "geo_code": geo_code,
        "geo_name": geo_name,
        "party_short": party_short,
        "party_full": party_full,

        "office_key": office_key,
        "party_key": party_key,
        "chamber": chamber,
        "state": state,
        "district": district_val,
        "class": sen_class,
        "term_start": start,
        "term_end": end,
        "now": now_iso,
    }

    cypher = """
    // Ensure Source
    MERGE (src:Source {source_id:$source_id})
      ON CREATE SET src.url=$source_url, src.publisher=$source_publisher, src.retrieved_at=datetime($retrieved_at)

    // Person anchor
    WITH *
    MATCH (p:Person {bioguide_id:$bioguide_id})

    // Geography
    MERGE (g:Geography {kind:$geo_kind, code:$geo_code})
      ON CREATE SET g.name=$geo_name

    // Party org
    MERGE (party:Organization {org_type:'PARTY', short:$party_short})
      ON CREATE SET party.name=$party_full

    // OfficeTerm event
    MERGE (ot:OfficeTerm {key:$office_key})
      ON CREATE SET ot.recorded_at=datetime($now)
    SET ot.chamber=$chamber,
        ot.state=$state,
        ot.district=$district,
        ot.class=$class,
        ot.valid_from = date($term_start),
        ot.valid_to = CASE WHEN $term_end IS NULL THEN NULL ELSE date($term_end) END

    MERGE (p)-[:REPRESENTS]->(ot)
    MERGE (ot)-[:OF]->(g)
    MERGE (ot)-[:SUPPORTED_BY]->(src)

    // PartyAffiliation event (paired to term window)
    MERGE (pa:PartyAffiliation {key:$party_key})
      ON CREATE SET pa.recorded_at=datetime($now)
    SET pa.party=$party_short,
        pa.valid_from = date($term_start),
        pa.valid_to = CASE WHEN $term_end IS NULL THEN NULL ELSE date($term_end) END

    MERGE (p)-[:AFFILIATED_WITH]->(pa)
    MERGE (pa)-[:OF]->(party)
    MERGE (pa)-[:SUPPORTED_BY]->(src)
    """

    session.run(cypher, params)


def ingest_legislator(session, entry: Dict[str, Any], source: SourceMeta) -> None:
    """Top-level: upsert person, then iterate terms → events."""
    upsert_source(session, source)
    upsert_person(session, entry)

    bioguide = entry.get("bioguide_id") or entry.get("ids", {}).get("bioguide")
    terms: Iterable[Dict[str, Any]] = entry.get("terms", [])

    # Optionally combine contiguous terms with same party/chamber/state/district/class windows.
    # (MVP: emit as-is; downstream queries can coalesce.)
    for term in terms:
        upsert_term_events(session, bioguide, term, source)

# -----------------------
# Bulk ingestion helpers
# -----------------------

def _ingest_one(entry: Dict[str, Any], src: SourceMeta, driver) -> None:
    """Single-entry ingestion with its own Neo4j session."""
    _ensure_constraints(driver)
    with driver.session() as sess:
        ingest_legislator(sess, entry, src)


# -----------------------
# Bulk concurrent API
# -----------------------

def insert_many(
    entries: Iterable[Dict[str, Any]],
    src: SourceMeta,
    render_visual: bool = False,
    workers: int = 8,
    db_uri: str = "neo4j://localhost:7687",
    db_user: str = "neo4j",
    db_pass: str = "your_password",
) -> Dict[str, Any]:
    """
    Concurrently ingest many legislator entries into Neo4j.
    - Uses a shared Neo4j driver (thread-safe) and a new session per task (not shared).
    - Constraints are installed once per process.
    - Visualizations are rendered *after* DB ingest to avoid matplotlib thread issues.
    Returns a summary dict with per-entry status.
    """
    entries_list = list(entries)
    total = len(entries_list)
    if total == 0:
        return {"count": 0, "results": []}

    out_dir = Path("./data/out")
    out_dir.mkdir(exist_ok=True, parents=True)

    driver = GraphDatabase.driver(db_uri, auth=(db_user, db_pass))
    results: list[Dict[str, Any]] = []
    try:
        _ensure_constraints(driver)
        logger.info(f"Starting ingest of {total} entries with {workers} workers...")

        def task(entry: Dict[str, Any]) -> tuple[str, Optional[Exception]]:
            bioguide = entry.get("bioguide_id") or (entry.get("ids", {}) or {}).get("bioguide")
            try:
                _retry(lambda: _ingest_one(entry, src, driver))
                return (str(bioguide), None)
            except Exception as e:
                return (str(bioguide), e)

        with ThreadPoolExecutor(max_workers=max(1, int(workers))) as pool:
            futures = {pool.submit(task, e): e for e in entries_list}
            for f in as_completed(futures):
                bid, err = f.result()
                if err is None:
                    logger.info(f"Ingested {bid}")
                    results.append({"bioguide_id": bid, "status": "ok"})
                else:
                    logger.error(f"Ingest failed for {bid}: {err}")
                    results.append({"bioguide_id": bid, "status": "error", "error": repr(err)})

        # Render visuals sequentially to avoid matplotlib global-state issues
        if render_visual:
            for e in entries_list:
                bid = e.get("bioguide_id") or (e.get("ids", {}) or {}).get("bioguide")
                png_path = out_dir / f"{bid}_graph.png"
                gml_path = out_dir / f"{bid}.graphml"
                try:
                    render_person_png(e, src, str(png_path), str(gml_path))
                    logger.info(f"Wrote visualization to {png_path} and {gml_path}")
                except Exception as viz_err:
                    logger.error(f"Visualization failed for {bid}: {viz_err}")
                    results.append({"bioguide_id": bid, "status": "ok", "visualization_error": repr(viz_err)})

        return {"count": total, "results": results}
    finally:
        driver.close()


# -----------------------
# Visualization & Usage
# -----------------------

def wipe_neo4j_database(batch_size: int = 20000, use_apoc: bool = True, sleep_between: float = 0.0):
    """
    Wipes all nodes and relationships from the Neo4j database with low memory pressure.
    Strategy:
      1) Prefer APOC periodic iterate (fast & low memory) if available.
      2) Fallback to batched loops:
         a) Delete relationships in batches
         b) Delete nodes in batches
    Args:
        batch_size: number of rows per batch (tune based on hardware)
        use_apoc: attempt to use APOC if available
        sleep_between: optional sleep between batches to reduce load
    """
    driver = None
    try:
        driver = GraphDatabase.driver("neo4j://localhost:7687", auth=("neo4j","your_password"))
        with driver.session() as session:
            # Try APOC first
            if use_apoc:
                try:
                    # Probe APOC availability
                    session.run("RETURN apoc.version() AS v").single()
                    # Use APOC periodic iterate for efficient batched delete
                    q = (
                        "CALL apoc.periodic.iterate("
                        "  'MATCH (n) RETURN n',"
                        "  'DETACH DELETE n',"
                        "  {batchSize:$batch, parallel:true}"
                        ") YIELD batches, total "
                        "RETURN batches, total"
                    )
                    res = session.run(q, {"batch": batch_size}).single()
                    print(f"APOC deletion completed. batches={res['batches']} total={res['total']}")
                    return
                except Exception as e:
                    print(f"APOC not available or failed ({e}); falling back to batched loop...")

            # ---- Fallback: two-phase batched deletion (low memory) ----
            total_rels = 0
            total_nodes = 0

            # Phase 1: relationships
            while True:
                r = session.run(
                    """
                    MATCH ()-[r]->()
                    WITH r LIMIT $batch
                    DELETE r
                    RETURN count(*) AS deleted
                    """,
                    {"batch": batch_size},
                ).single()
                deleted = r["deleted"] or 0
                total_rels += deleted
                print(f"Deleted relationships batch: {deleted} (total {total_rels})")
                if deleted == 0:
                    break
                if sleep_between > 0:
                    time.sleep(sleep_between)

            # Phase 2: nodes
            while True:
                r = session.run(
                    """
                    MATCH (n)
                    WITH n LIMIT $batch
                    DELETE n
                    RETURN count(*) AS deleted
                    """,
                    {"batch": batch_size},
                ).single()
                deleted = r["deleted"] or 0
                total_nodes += deleted
                print(f"Deleted nodes batch: {deleted} (total {total_nodes})")
                if deleted == 0:
                    break
                if sleep_between > 0:
                    time.sleep(sleep_between)

            print(f"Database wiped. Total rels={total_rels}, nodes={total_nodes}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if driver:
            driver.close()

def build_person_graph_from_entry(entry: Dict[str, Any], source: SourceMeta):
    """Build a NetworkX DiGraph representing the temporal schema for a single person entry."""
    import networkx as nx

    G = nx.DiGraph()

    L_PERSON, L_EVENT, L_ENTITY, L_SOURCE = 0, 1, 2, 3

    # Person anchor
    bioguide = entry.get("bioguide_id") or entry.get("ids", {}).get("bioguide")
    nm = entry.get("name", {})
    person_label = f"Person: {nm.get('official_full') or nm.get('first', '') + ' ' + nm.get('last', '')}\n({bioguide})"
    G.add_node(person_label, layer=L_PERSON, kind="Person")

    # One synthetic Source node for this dataset
    G.add_node(f"Source: {source.source_id}", layer=L_SOURCE, kind="Source")

    for term in entry.get("terms", []):
        term_type = term.get("type")
        chamber = "HOUSE" if term_type == "rep" else "SENATE"
        state = term.get("state")
        district = term.get("district") if term_type == "rep" else None
        klass = term.get("class") if term_type == "sen" else None
        start, end = term.get("start"), term.get("end")

        # Geography + Party anchors
        if chamber == "HOUSE":
            g_label = f"District: {state}-{district}"
        else:
            g_label = f"State: {state}"
        G.add_node(g_label, layer=L_ENTITY, kind="Geography")

        party_short, party_full = norm_party(term.get("party"))
        party_label = f"Party: {party_full}"
        G.add_node(party_label, layer=L_ENTITY, kind="Organization(PARTY)")

        # Event nodes
        d_for_label = None
        if chamber == "HOUSE":
            try:
                d_for_label = int(district) if district is not None else None
            except (TypeError, ValueError):
                d_for_label = None
        d_suffix = f"-{'AL' if (d_for_label in (None, -1, 0)) else str(d_for_label)}" if chamber == "HOUSE" else ""
        ot_label = (
            f"OfficeTerm: {chamber} {state}{d_suffix}"
            f"\n({start} → {end or ''})"
        )
        pa_label = f"PartyAffiliation: {party_short}\n({start} → {end or ''})"
        G.add_node(ot_label, layer=L_EVENT, kind="Event")
        G.add_node(pa_label, layer=L_EVENT, kind="Event")

        # Wire
        G.add_edge(person_label, ot_label, label="REPRESENTS")
        G.add_edge(ot_label, g_label, label="OF")
        G.add_edge(ot_label, f"Source: {source.source_id}", label="SUPPORTED_BY")

        G.add_edge(person_label, pa_label, label="AFFILIATED_WITH")
        G.add_edge(pa_label, party_label, label="OF")
        G.add_edge(pa_label, f"Source: {source.source_id}", label="SUPPORTED_BY")

    return G


def render_person_png(entry: Dict[str, Any], source: SourceMeta, out_png: str, out_graphml: Optional[str] = None) -> str:
    import networkx as nx
    import matplotlib.pyplot as plt

    G = build_person_graph_from_entry(entry, source)
    pos = nx.multipartite_layout(G, subset_key="layer")

    fig = plt.figure(figsize=(14, 9))
    nx.draw_networkx_nodes(G, pos, node_size=1400)
    nx.draw_networkx_labels(G, pos, font_size=8)
    nx.draw_networkx_edges(G, pos, arrows=True, arrowstyle="->", arrowsize=12, width=1.2)
    plt.axis("off")
    plt.tight_layout()
    fig.savefig(out_png, dpi=200)
    plt.close(fig)

    if out_graphml:
        nx.write_graphml(G, out_graphml)
    return out_png

def insert_data(entry: Dict[str, Any], src: SourceMeta, render_visual: bool = False):
    
    # Maintain the original single-entry API by delegating to the bulk inserter.
    summary = insert_many([entry], src, render_visual=render_visual, workers=1)
    # Mirror the original logging style for a single record:
    bioguide = entry.get("bioguide_id") or (entry.get("ids", {}) or {}).get("bioguide")
    if render_visual:
        logger.info(f"Wrote visualization to ./data/out/{bioguide}_graph.png and ./data/out/{bioguide}.graphml")
    if any(r.get("status") == "error" for r in summary["results"]):
        logger.error(f"Ingest encountered an error for {bioguide}.")
    else:
        logger.info(f"Successfully ingested Person {bioguide}.")
    return summary


# -----------------------
# Quick manual test hook
# -----------------------
if __name__ == "__main__":
    # Minimal demo using the sample entry from the user prompt
    SAMPLE_ENTRY = {'bioguide_id': 'C000127', 'name': {'first': 'Maria', 'last': 'Cantwell', 'official_full': 'Maria Cantwell'}, 'bio': {'birthday': '1958-10-13', 'gender': 'F'}, 'terms': [{'type': 'rep', 'start': '1993-01-05', 'end': '1995-01-03', 'state': 'WA', 'district': 1, 'party': 'Democrat'}, {'type': 'sen', 'start': '2001-01-03', 'end': '2007-01-03', 'state': 'WA', 'class': 1, 'party': 'Democrat', 'url': 'http://cantwell.senate.gov'}, {'type': 'sen', 'start': '2007-01-04', 'end': '2013-01-03', 'state': 'WA', 'class': 1, 'party': 'Democrat', 'url': 'http://cantwell.senate.gov', 'address': '311 HART SENATE OFFICE BUILDING WASHINGTON DC 20510', 'phone': '202-224-3441', 'fax': '202-228-0514', 'contact_form': 'http://www.cantwell.senate.gov/contact/', 'office': '311 Hart Senate Office Building'}, {'type': 'sen', 'start': '2013-01-03', 'end': '2019-01-03', 'state': 'WA', 'party': 'Democrat', 'class': 1, 'url': 'https://www.cantwell.senate.gov', 'address': '511 Hart Senate Office Building Washington DC 20510', 'phone': '202-224-3441', 'fax': '202-228-0514', 'contact_form': 'http://www.cantwell.senate.gov/public/index.cfm/email-maria', 'office': '511 Hart Senate Office Building', 'state_rank': 'junior', 'rss_url': 'http://www.cantwell.senate.gov/public/index.cfm/rss/feed'}, {'type': 'sen', 'start': '2019-01-03', 'end': '2025-01-03', 'state': 'WA', 'class': 1, 'party': 'Democrat', 'state_rank': 'junior', 'url': 'https://www.cantwell.senate.gov', 'rss_url': 'http://www.cantwell.senate.gov/public/index.cfm/rss/feed', 'contact_form': 'https://www.cantwell.senate.gov/public/index.cfm/email-maria', 'address': '511 Hart Senate Office Building Washington DC 20510', 'office': '511 Hart Senate Office Building', 'phone': '202-224-3441'}, {'type': 'sen', 'start': '2025-01-03', 'end': '2031-01-03', 'state': 'WA', 'class': 1, 'state_rank': 'junior', 'party': 'Democrat', 'url': 'https://www.cantwell.senate.gov', 'rss_url': 'http://www.cantwell.senate.gov/public/index.cfm/rss/feed', 'contact_form': 'https://www.cantwell.senate.gov/public/index.cfm/email-maria', 'address': '511 Hart Senate Office Building Washington DC 20510', 'office': '511 Hart Senate Office Building', 'phone': '202-224-3441'}], 'ids': {'bioguide': 'C000127', 'thomas': '00172', 'lis': 'S275', 'govtrack': 300018, 'opensecrets': 'N00007836', 'votesmart': 27122, 'fec': ['S8WA00194', 'H2WA01054'], 'cspan': 26137, 'wikipedia': 'Maria Cantwell', 'house_history': 10608, 'ballotpedia': 'Maria Cantwell', 'maplight': 544, 'icpsr': 39310, 'wikidata': 'Q22250', 'google_entity_id': 'kg:/m/01x68t', 'pictorial': 13398}, 'current_term': {'type': 'sen', 'start': '2025-01-03', 'end': '2031-01-03', 'state': 'WA', 'class': 1, 'state_rank': 'junior', 'party': 'Democrat', 'url': 'https://www.cantwell.senate.gov', 'rss_url': 'http://www.cantwell.senate.gov/public/index.cfm/rss/feed', 'contact_form': 'https://www.cantwell.senate.gov/public/index.cfm/email-maria', 'address': '511 Hart Senate Office Building Washington DC 20510', 'office': '511 Hart Senate Office Building', 'phone': '202-224-3441'}}
    src = SourceMeta(
        source_id="congress-legislators@sample",
        url="https://github.com/unitedstates/congress-legislators",
        publisher="unitedstates/congress-legislators",
    )

    insert_data(SAMPLE_ENTRY, src, render_visual=True)

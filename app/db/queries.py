import logging
import heapq
from typing import Optional

import asyncpg

from app.core.config import settings
from app.db.pool import get_pool
from app.schemas.paths import DijkstraFilter, ChildTreeResponse, ChildNode, ChildTreeItem

logger = logging.getLogger(__name__)

SYSTEM_ID_PATTERN = r"^[a-zA-Z0-9_-]{1,128}$"
DEFAULT_MAX_DEPTH = 4


def validate_system_id(system_id: str) -> bool:
    import re
    return bool(re.match(SYSTEM_ID_PATTERN, system_id))


def build_path_query(
    from_system_id: str, 
    to_system_id: str, 
    graph_name: str, 
    limit: int,
    max_depth: Optional[int] = None
) -> str:
    depth_pattern = f"0..{max_depth}" if max_depth is not None else "0.."
    
    return f"""
WITH paths AS (
SELECT *
FROM cypher('{graph_name}', $$
MATCH p = (a:SYSTEM)-[r1:EOTAR_INTERFACE]->(:SYSTEM)-[:EOTAR_INTERFACE*{depth_pattern}]->(b:SYSTEM)
WHERE a.system_rsm_id = "{from_system_id}"
AND b.system_rsm_id = "{to_system_id}"
RETURN
a.system_rsm_id,
a.system_rsm_name,
b.system_rsm_id,
b.system_rsm_name,
length(p),
nodes(p),
r1.eotar_rsm_id
LIMIT {limit}
$$) AS (
from_system_id text,
from_system_name text,
to_system_id text,
to_system_name text,
path_length int,
path_nodes agtype,
eotar_rsm_id text
)
)
SELECT
from_system_id,
from_system_name,
to_system_id,
to_system_name,
path_length,
path_nodes AS path,
COUNT(*) AS frequency,
MIN(eotar_rsm_id) AS example_eotar_rsm_id
FROM paths
GROUP BY
from_system_id,
from_system_name,
to_system_id,
to_system_name,
path_length,
path_nodes
ORDER BY frequency DESC, path_length ASC
LIMIT 1;
"""


class PathResult:
    def __init__(
        self,
        from_system_id: str,
        from_system_name: Optional[str],
        to_system_id: str,
        to_system_name: Optional[str],
        path_length: int,
        path: str,
        frequency: int,
        example_eotar_rsm_id: Optional[str],
    ):
        self.from_system_id = from_system_id
        self.from_system_name = from_system_name
        self.to_system_id = to_system_id
        self.to_system_name = to_system_name
        self.path_length = path_length
        self.path = path
        self.frequency = frequency
        self.example_eotar_rsm_id = example_eotar_rsm_id


async def execute_path_search(from_system_id: str, to_system_id: str) -> Optional[PathResult]:
    if not validate_system_id(from_system_id):
        raise ValueError(f"Invalid from_system_id format: {from_system_id}")
    if not validate_system_id(to_system_id):
        raise ValueError(f"Invalid to_system_id format: {to_system_id}")
    
    logger.info(f"Searching path from {from_system_id} to {to_system_id}")
    
    pool = get_pool()
    query = build_path_query(
        from_system_id=from_system_id,
        to_system_id=to_system_id,
        graph_name=settings.AGE_GRAPH_NAME,
        limit=settings.PATH_SEARCH_LIMIT,
    )
    
    timeout = settings.DB_STATEMENT_TIMEOUT_MS / 1000.0
    
    try:
        async with pool.acquire(timeout=timeout) as conn:
            await conn.execute('SET search_path = ag_catalog, "$user", public;')
            row = await conn.fetchrow(query)
    except asyncpg.PostgresConnectionError as e:
        logger.error(f"Database connection error: {e}")
        raise
    except asyncpg.QueryCanceledError as e:
        logger.error(f"Database query timeout: {e}")
        raise
    except Exception as e:
        logger.error(f"Database query error: {e}")
        raise
    
    if row is None:
        logger.info(f"Path not found from {from_system_id} to {to_system_id}")
        return None
    
    logger.info(f"Path found from {from_system_id} to {to_system_id}, length={row['path_length']}")
    
    return PathResult(
        from_system_id=row["from_system_id"],
        from_system_name=row["from_system_name"],
        to_system_id=row["to_system_id"],
        to_system_name=row["to_system_name"],
        path_length=row["path_length"],
        path=row["path"],
        frequency=row["frequency"],
        example_eotar_rsm_id=row["example_eotar_rsm_id"],
    )


async def execute_path_search_bounded(
    from_system_id: str, 
    to_system_id: str, 
    max_depth: int
) -> Optional[PathResult]:
    if not validate_system_id(from_system_id):
        raise ValueError(f"Invalid from_system_id format: {from_system_id}")
    if not validate_system_id(to_system_id):
        raise ValueError(f"Invalid to_system_id format: {to_system_id}")
    if max_depth < 1 or max_depth > 100:
        raise ValueError(f"max_depth must be between 1 and 100, got: {max_depth}")
    
    logger.info(f"Searching path from {from_system_id} to {to_system_id} with max_depth={max_depth}")
    
    pool = get_pool()
    query = build_path_query(
        from_system_id=from_system_id,
        to_system_id=to_system_id,
        graph_name=settings.AGE_GRAPH_NAME,
        limit=settings.PATH_SEARCH_LIMIT,
        max_depth=max_depth,
    )
    
    timeout = settings.DB_STATEMENT_TIMEOUT_MS / 1000.0
    
    try:
        async with pool.acquire(timeout=timeout) as conn:
            await conn.execute('SET search_path = ag_catalog, "$user", public;')
            row = await conn.fetchrow(query)
    except asyncpg.PostgresConnectionError as e:
        logger.error(f"Database connection error: {e}")
        raise
    except asyncpg.QueryCanceledError as e:
        logger.error(f"Database query timeout: {e}")
        raise
    except Exception as e:
        logger.error(f"Database query error: {e}")
        raise
    
    if row is None:
        logger.info(f"Path not found from {from_system_id} to {to_system_id} with max_depth={max_depth}")
        return None
    
    logger.info(f"Path found from {from_system_id} to {to_system_id}, length={row['path_length']}")
    
    return PathResult(
        from_system_id=row["from_system_id"],
        from_system_name=row["from_system_name"],
        to_system_id=row["to_system_id"],
        to_system_name=row["to_system_name"],
        path_length=row["path_length"],
        path=row["path"],
        frequency=row["frequency"],
        example_eotar_rsm_id=row["example_eotar_rsm_id"],
    )


class SystemResult:
    def __init__(self, system_id: str, system_name: str):
        self.system_id = system_id
        self.system_name = system_name


def build_system_search_query(graph_name: str, name_pattern: str) -> str:
    escaped_pattern = name_pattern.replace("'", "''")
    return f"""
SELECT DISTINCT
  properties ->> '"system_rsm_id"'   AS system_id,
  properties ->> '"system_rsm_name"' AS system_name
FROM {graph_name}."SYSTEM"
WHERE properties ->> '"system_rsm_name"' ILIKE '%{escaped_pattern}%';
"""


async def search_systems_by_name(name_pattern: str) -> list[SystemResult]:
    if not name_pattern or len(name_pattern.strip()) == 0:
        raise ValueError("name_pattern cannot be empty")
    
    if len(name_pattern) > 256:
        raise ValueError("name_pattern cannot exceed 256 characters")
    
    logger.info(f"Searching systems by name pattern: {name_pattern}")
    
    pool = get_pool()
    query = build_system_search_query(
        graph_name=settings.AGE_GRAPH_NAME,
        name_pattern=name_pattern.strip(),
    )
    
    timeout = settings.DB_STATEMENT_TIMEOUT_MS / 1000.0
    
    try:
        async with pool.acquire(timeout=timeout) as conn:
            await conn.execute(f'SET search_path = ag_catalog, "{settings.AGE_GRAPH_NAME}", "$user", public;')
            rows = await conn.fetch(query)
    except asyncpg.PostgresConnectionError as e:
        logger.error(f"Database connection error: {e}")
        raise
    except asyncpg.QueryCanceledError as e:
        logger.error(f"Database query timeout: {e}")
        raise
    except Exception as e:
        logger.error(f"Database query error: {e}")
        raise
    
    results = [
        SystemResult(system_id=row["system_id"], system_name=row["system_name"])
        for row in rows
    ]
    
    logger.info(f"Found {len(results)} systems matching pattern: {name_pattern}")
    
    return results


class GraphEdge:
    def __init__(
        self,
        from_system: str,
        from_module: Optional[str],
        from_component: Optional[str],
        to_system: str,
        to_module: Optional[str],
        to_component: Optional[str],
        weight: int,
        eotar_rsm_id: str,
        eotar_rsm_date_time: Optional[str],
    ):
        self.from_system = from_system
        self.from_module = from_module
        self.from_component = from_component
        self.to_system = to_system
        self.to_module = to_module
        self.to_component = to_component
        self.weight = weight
        self.eotar_rsm_id = eotar_rsm_id
        self.eotar_rsm_date_time = eotar_rsm_date_time


def build_dijkstra_graph_query(graph_name: str) -> str:
    return f"""
    SELECT * FROM cypher('{graph_name}', $$
      MATCH (a)-[r:EOTAR_INTERFACE]->(b)
      RETURN a.system_rsm_id as from_system, 
             r.consumer_module_rsm_id as from_module,
             r.consumer_component_rsm_id as from_component,
             b.system_rsm_id as to_system,  
             r.provider_module_rsm_id as to_module,
             r.provider_component_rsm_id as to_component,
             r.weight as weight, 
             r.eotar_rsm_id,
             r.eotar_rsm_date_time
    $$) as (
        from_system text, 
        from_module text,
        from_component text,
        to_system text, 
        to_module text,
        to_component text,
        weight int, 
        eotar_rsm_id text,
        eotar_rsm_date_time text
    )
    """


async def load_graph_edges() -> list[GraphEdge]:
    logger.info("Loading graph edges for Dijkstra algorithm")
    
    pool = get_pool()
    query = build_dijkstra_graph_query(settings.AGE_GRAPH_NAME)
    timeout = settings.DB_STATEMENT_TIMEOUT_MS / 1000.0
    
    try:
        async with pool.acquire(timeout=timeout) as conn:
            await conn.execute('SET search_path = ag_catalog, "$user", public;')
            rows = await conn.fetch(query)
    except asyncpg.PostgresConnectionError as e:
        logger.error(f"Database connection error: {e}")
        raise
    except asyncpg.QueryCanceledError as e:
        logger.error(f"Database query timeout: {e}")
        raise
    except Exception as e:
        logger.error(f"Database query error: {e}")
        raise
    
    edges = [
        GraphEdge(
            from_system=row["from_system"],
            from_module=row["from_module"],
            from_component=row["from_component"],
            to_system=row["to_system"],
            to_module=row["to_module"],
            to_component=row["to_component"],
            weight=row["weight"] or 1,
            eotar_rsm_id=row["eotar_rsm_id"],
            eotar_rsm_date_time=row["eotar_rsm_date_time"],
        )
        for row in rows
    ]
    
    logger.info(f"Loaded {len(edges)} edges from graph")
    return edges


def make_node_id(system_id: str, module_id: Optional[str], component_id: Optional[str]) -> str:
    return f"{system_id or ''}|{module_id or ''}|{component_id or ''}"


def matches_node(node_id: str, filter: DijkstraFilter) -> bool:
    parts = node_id.split("|")
    node = {
        "system_rsm_id": parts[0] if len(parts) > 0 else "",
        "module_rsm_id": parts[1] if len(parts) > 1 else "",
        "component_rsm_id": parts[2] if len(parts) > 2 else "",
    }
    return (
        (not filter.system_rsm_id or filter.system_rsm_id == node["system_rsm_id"]) and
        (not filter.module_rsm_id or filter.module_rsm_id == node["module_rsm_id"]) and
        (not filter.component_rsm_id or filter.component_rsm_id == node["component_rsm_id"])
    )


async def execute_dijkstra_search(
    start_filter: DijkstraFilter,
    finish_filter: DijkstraFilter,
) -> dict[str, dict[str, dict]]:
    edges = await load_graph_edges()
    
    graph: dict[str, dict[str, list[dict]]] = {}
    all_nodes: set[str] = set()
    
    for edge in edges:
        from_node = make_node_id(edge.from_system, edge.from_module, edge.from_component)
        to_node = make_node_id(edge.to_system, edge.to_module, edge.to_component)
        
        all_nodes.add(from_node)
        all_nodes.add(to_node)
        
        if from_node not in graph:
            graph[from_node] = {}
        if to_node not in graph[from_node]:
            graph[from_node][to_node] = []
        
        graph[from_node][to_node].append({
            "eotar_rsm_id": edge.eotar_rsm_id,
            "weight": edge.weight,
            "eotar_rsm_date_time": edge.eotar_rsm_date_time,
        })
    
    start_nodes = [n for n in all_nodes if matches_node(n, start_filter)]
    if not start_nodes:
        for from_node in graph:
            if matches_node(from_node, start_filter):
                start_nodes.append(from_node)
    start_nodes = list(set(start_nodes))
    
    finish_nodes: set[str] = set()
    for node in all_nodes:
        if matches_node(node, finish_filter):
            finish_nodes.add(node)
    for from_node in graph:
        for to_node in graph[from_node]:
            if matches_node(to_node, finish_filter):
                finish_nodes.add(to_node)
    finish_nodes = list(finish_nodes)
    
    logger.info(f"Dijkstra: {len(start_nodes)} start nodes, {len(finish_nodes)} finish nodes")
    
    if not start_nodes or not finish_nodes:
        return {}
    
    eotar_data: dict[str, dict] = {}
    for start_node in start_nodes:
        if start_node in graph:
            for neighbor in graph[start_node]:
                for edge in graph[start_node][neighbor]:
                    eotar_id = edge["eotar_rsm_id"]
                    if eotar_id:
                        existing = eotar_data.get(eotar_id)
                        new_date = edge.get("eotar_rsm_date_time")
                        if existing is None or (new_date and (existing.get("eotar_rsm_date_time") is None or new_date > existing.get("eotar_rsm_date_time", ""))):
                            eotar_data[eotar_id] = {
                                "eotar_rsm_id": eotar_id,
                                "eotar_rsm_date_time": new_date,
                            }
    
    eotar_ids: set[str] = set(eotar_data.keys())
    
    logger.info(f"Dijkstra: {len(eotar_ids)} unique eotar_rsm_ids to process")
    
    results: dict[str, dict[str, dict]] = {}
    
    for selected_eotar in eotar_ids:
        distances: dict[str, float] = {n: float('inf') for n in all_nodes}
        previous: dict[str, Optional[str]] = {n: None for n in all_nodes}
        visited: set[str] = set()
        queue: list[tuple[float, str]] = []
        
        for n in start_nodes:
            distances[n] = 0.0
            heapq.heappush(queue, (0.0, n))
        
        while queue:
            dist, current = heapq.heappop(queue)
            if current in visited:
                continue
            visited.add(current)
            
            if current not in graph:
                continue
            
            for neighbor, edge_list in graph[current].items():
                for edge in edge_list:
                    if edge["eotar_rsm_id"] != selected_eotar:
                        continue
                    weight = edge["weight"]
                    new_dist = distances[current] + weight
                    
                    if new_dist < distances[neighbor]:
                        distances[neighbor] = new_dist
                        previous[neighbor] = current
                        heapq.heappush(queue, (new_dist, neighbor))
        
        paths: dict[str, dict] = {}
        for finish_node in finish_nodes:
            if distances[finish_node] == float('inf'):
                continue
            path: list[str] = []
            current: Optional[str] = finish_node
            while current:
                path.insert(0, current)
                current = previous[current]
            paths[finish_node] = {
                "distance": int(distances[finish_node]),
                "path": path,
            }
        
        if paths:
            results[selected_eotar] = {
                "paths": paths,
                "eotar_rsm_date_time": eotar_data.get(selected_eotar, {}).get("eotar_rsm_date_time"),
            }
    
    logger.info(f"Dijkstra: found paths in {len(results)} eotar groups")
    return results


class NodeNames:
    def __init__(
        self,
        system_rsm_id: str,
        system_rsm_name: Optional[str],
        module_rsm_id: str,
        module_rsm_name: Optional[str],
        component_rsm_id: str,
        component_rsm_name: Optional[str],
    ):
        self.system_rsm_id = system_rsm_id
        self.system_rsm_name = system_rsm_name
        self.module_rsm_id = module_rsm_id
        self.module_rsm_name = module_rsm_name
        self.component_rsm_id = component_rsm_id
        self.component_rsm_name = component_rsm_name


async def fetch_node_names(
    nodes: list[tuple[str, str, str]]
) -> dict[tuple, NodeNames]:
    if not nodes:
        return {}
    
    unique_nodes = list(set(nodes))
    
    if not unique_nodes:
        return {}
    
    pool = get_pool()
    timeout = settings.DB_STATEMENT_TIMEOUT_MS / 1000.0
    
    union_parts = []
    for sys_id, mod_id, comp_id in unique_nodes:
        escaped_sys = (sys_id or "").replace('"', '\\"')
        escaped_mod = (mod_id or "").replace('"', '\\"')
        escaped_comp = (comp_id or "").replace('"', '\\"')
        
        union_parts.append(f"""
        SELECT 
            '{escaped_sys}' as system_rsm_id,
            '{escaped_mod}' as module_rsm_id, 
            '{escaped_comp}' as component_rsm_id,
            (SELECT system_rsm_name FROM ag_catalog.cypher('{settings.AGE_GRAPH_NAME}', $$
                MATCH (s:SYSTEM {{system_rsm_id: "{escaped_sys}"}})
                RETURN s.system_rsm_name
                LIMIT 1
            $$) AS (system_rsm_name ag_catalog.agtype)) as system_rsm_name,
            (SELECT module_rsm_name FROM ag_catalog.cypher('{settings.AGE_GRAPH_NAME}', $$
                OPTIONAL MATCH (m:MODULE {{module_rsm_id: "{escaped_mod}"}})
                RETURN m.module_rsm_name
                LIMIT 1
            $$) AS (module_rsm_name ag_catalog.agtype)) as module_rsm_name,
            (SELECT component_rsm_name FROM ag_catalog.cypher('{settings.AGE_GRAPH_NAME}', $$
                OPTIONAL MATCH (c:COMPONENT {{component_rsm_id: "{escaped_comp}"}})
                RETURN c.component_rsm_name
                LIMIT 1
            $$) AS (component_rsm_name ag_catalog.agtype)) as component_rsm_name
        """)
    
    query = " UNION ALL ".join(union_parts)
    
    result: dict[tuple, NodeNames] = {}
    
    try:
        async with pool.acquire(timeout=timeout) as conn:
            await conn.execute('SET search_path = ag_catalog, "$user", public;')
            rows = await conn.fetch(query)
    except asyncpg.PostgresConnectionError as e:
        logger.error(f"Database connection error: {e}")
        raise
    except asyncpg.QueryCanceledError as e:
        logger.error(f"Database query timeout: {e}")
        raise
    except Exception as e:
        logger.error(f"Error fetching node names: {e}")
        raise
    
    for row in rows:
        key = (row["system_rsm_id"], row["module_rsm_id"], row["component_rsm_id"])
        result[key] = NodeNames(
            system_rsm_id=row["system_rsm_id"],
            system_rsm_name=row["system_rsm_name"],
            module_rsm_id=row["module_rsm_id"],
            module_rsm_name=row["module_rsm_name"],
            component_rsm_id=row["component_rsm_id"],
            component_rsm_name=row["component_rsm_name"],
        )
    
    logger.info(f"Fetched names for {len(result)} nodes")
    return result


def parse_agtype(agtype_str: str) -> dict:
    import re
    if not agtype_str:
        return {}
    
    cleaned = agtype_str.strip()
    if cleaned.startswith('"') and cleaned.endswith('"'):
        cleaned = cleaned[1:-1]
    
    cleaned = re.sub(r'::[\w]+$', '', cleaned)
    
    if cleaned.startswith('{') and cleaned.endswith('}'):
        props = {}
        content = cleaned[1:-1]
        
        pattern = r'"([^"]+)"\s*:\s*(?:"([^"]*)"|(\d+(?:\.\d+)?)|(\w+))'
        for match in re.finditer(pattern, content):
            key = match.group(1)
            if match.group(2) is not None:
                props[key] = match.group(2)
            elif match.group(3) is not None:
                val = match.group(3)
                props[key] = float(val) if '.' in val else int(val)
            elif match.group(4) is not None:
                props[key] = match.group(4)
        
        return props
    
    return {}


def build_system_find_query(graph_name: str, rsm_id: str) -> str:
    return f"""
    SELECT *
    FROM ag_catalog.cypher('{graph_name}', $$
        MATCH (r:SYSTEM {{system_rsm_id: "{rsm_id}"}})
        RETURN r, id(r)
    $$) AS (r ag_catalog.agtype, r_id bigint)
    """


def build_module_find_query(graph_name: str, rsm_id: str) -> str:
    return f"""
    SELECT *
    FROM ag_catalog.cypher('{graph_name}', $$
        MATCH (r:MODULE {{module_rsm_id: "{rsm_id}"}})
        RETURN r, id(r)
    $$) AS (r ag_catalog.agtype, r_id bigint)
    """


def build_system_query(graph_name: str, rsm_id: str) -> str:
    return f"""
    SELECT *
    FROM ag_catalog.cypher('{graph_name}', $$
        MATCH (r:SYSTEM {{system_rsm_id: "{rsm_id}"}})
        MATCH p = (r)-[:SYSTEM_HIERARCHY*1..5]->(n)
        RETURN p, n, id(n)
    $$) AS (p ag_catalog.agtype, n ag_catalog.agtype, n_id bigint)
    """


def build_module_query(graph_name: str, rsm_id: str) -> str:
    return f"""
    SELECT *
    FROM ag_catalog.cypher('{graph_name}', $$
        MATCH (r:MODULE {{module_rsm_id: "{rsm_id}"}})
        MATCH p = (r)-[:SYSTEM_HIERARCHY*1..5]->(n)
        RETURN p, n, id(n)
    $$) AS (p ag_catalog.agtype, n ag_catalog.agtype, n_id bigint)
    """


def build_children_query(graph_name: str, vertex_id: int) -> str:
    return f"""
    SELECT *
    FROM ag_catalog.cypher('{graph_name}', $$
        MATCH (a)-[:SYSTEM_HIERARCHY*]->(b)
        WHERE id(a) = {vertex_id}
        RETURN b, id(b)
    $$) AS (b ag_catalog.agtype, b_id bigint)
    """


async def find_vertex_by_rsm_id(rsm_id: str) -> Optional[tuple[int, dict]]:
    logger.info(f"Searching for vertex with rsm_id={rsm_id}")
    
    pool = get_pool()
    timeout = settings.DB_STATEMENT_TIMEOUT_MS / 1000.0
    
    system_query = build_system_find_query(settings.AGE_GRAPH_NAME, rsm_id)
    try:
        async with pool.acquire(timeout=timeout) as conn:
            await conn.execute('SET search_path = ag_catalog, "$user", public;')
            row = await conn.fetchrow(system_query)
    except asyncpg.PostgresConnectionError as e:
        logger.error(f"Database connection error: {e}")
        raise
    except asyncpg.QueryCanceledError as e:
        logger.error(f"Database query timeout: {e}")
        raise
    except Exception as e:
        logger.error(f"Database query error: {e}")
        raise
    
    if row and row["r"]:
        node_data = parse_agtype(str(row["r"]))
        logger.info(f"Found SYSTEM vertex with rsm_id={rsm_id}, id={row['r_id']}")
        return (row["r_id"], node_data)
    
    module_query = build_module_find_query(settings.AGE_GRAPH_NAME, rsm_id)
    try:
        async with pool.acquire(timeout=timeout) as conn:
            await conn.execute('SET search_path = ag_catalog, "$user", public;')
            row = await conn.fetchrow(module_query)
    except asyncpg.PostgresConnectionError as e:
        logger.error(f"Database connection error: {e}")
        raise
    except asyncpg.QueryCanceledError as e:
        logger.error(f"Database query timeout: {e}")
        raise
    except Exception as e:
        logger.error(f"Database query error: {e}")
        raise
    
    if row and row["r"]:
        node_data = parse_agtype(str(row["r"]))
        logger.info(f"Found MODULE vertex with rsm_id={rsm_id}, id={row['r_id']}")
        return (row["r_id"], node_data)
    
    logger.info(f"Vertex with rsm_id={rsm_id} not found")
    return None


async def get_hierarchy_tree(vertex_id: int) -> list[dict]:
    logger.info(f"Fetching hierarchy tree for vertex id={vertex_id}")
    
    pool = get_pool()
    query = build_children_query(settings.AGE_GRAPH_NAME, vertex_id)
    timeout = settings.DB_STATEMENT_TIMEOUT_MS / 1000.0
    
    try:
        async with pool.acquire(timeout=timeout) as conn:
            await conn.execute('SET search_path = ag_catalog, "$user", public;')
            rows = await conn.fetch(query)
    except asyncpg.PostgresConnectionError as e:
        logger.error(f"Database connection error: {e}")
        raise
    except asyncpg.QueryCanceledError as e:
        logger.error(f"Database query timeout: {e}")
        raise
    except Exception as e:
        logger.error(f"Database query error: {e}")
        raise
    
    children = []
    for row in rows:
        if row["b"]:
            node_data = parse_agtype(str(row["b"]))
            children.append({
                "id": row["b_id"],
                "data": node_data,
            })
    
    logger.info(f"Found {len(children)} children in hierarchy tree")
    return children


async def get_direct_children(vertex_id: int) -> list[dict]:
    """Получить только прямых детей (один уровень)"""
    logger.info(f"Fetching direct children for vertex id={vertex_id}")
    
    pool = get_pool()
    query = f"""
    SELECT *
    FROM ag_catalog.cypher('{settings.AGE_GRAPH_NAME}', $$
        MATCH (a)-[:SYSTEM_HIERARCHY]->(b)
        WHERE id(a) = {vertex_id}
        RETURN b, id(b)
    $$) AS (b ag_catalog.agtype, b_id bigint)
    """
    timeout = settings.DB_STATEMENT_TIMEOUT_MS / 1000.0
    
    try:
        async with pool.acquire(timeout=timeout) as conn:
            await conn.execute('SET search_path = ag_catalog, "$user", public;')
            rows = await conn.fetch(query)
    except asyncpg.PostgresConnectionError as e:
        logger.error(f"Database connection error: {e}")
        raise
    except asyncpg.QueryCanceledError as e:
        logger.error(f"Database query timeout: {e}")
        raise
    except Exception as e:
        logger.error(f"Database query error: {e}")
        raise
    # Убираем дубли по rsm_id в свойствах
    seen_rsm_ids = set()
    children = []
    for row in rows:
        if row["b"]:
            node_data = parse_agtype(str(row["b"]))
            rsm_id = node_data.get("module_rsm_id") or node_data.get("component_rsm_id") or node_data.get("system_rsm_id")
            if rsm_id and rsm_id in seen_rsm_ids:
                continue
            if rsm_id:
                seen_rsm_ids.add(rsm_id)
            children.append({
                "id": row["b_id"],
                "data": node_data,
            })
    
    logger.info(f"Found {len(children)} direct children for vertex id={vertex_id}")
    return children





async def build_tree_recursive(vertex_id: int, visited: set) -> list[ChildTreeItem]:
    """Рекурсивно строит дерево детей"""
    children_data = await get_direct_children(vertex_id)
    result = []
    
    for child in children_data:
        child_id = child["id"]
        
        # Если узел уже посещён — пропускаем (не дублируем)
        if child_id in visited:
            continue
        
        visited.add(child_id)
        child_properties = {k: v for k, v in child["data"].items() if k != "id"}
        
        # Рекурсивно получаем детей этого узла
        nested_children = await build_tree_recursive(child_id, visited)
        
        result.append(ChildTreeItem(
            node=ChildNode(
                properties=child_properties,
            ),
            children=nested_children,
        ))
    
    return result


async def build_child_tree_by_rsm_id(rsm_id: str) -> Optional[ChildTreeResponse]:
    vertex_data = await find_vertex_by_rsm_id(rsm_id)
    
    if vertex_data is None:
        return None
    
    vertex_id, node_data = vertex_data
    
    properties = {k: v for k, v in node_data.items() if k != "id"}
    
    child_node = ChildNode(
        properties=properties,
    )
    
    # Строим дерево рекурсивно
    visited: set = set()
    visited.add(vertex_id)  # Добавляем корневой узел
    children = await build_tree_recursive(vertex_id, visited)
    
    return ChildTreeResponse(
        node=child_node,
        children=children,
    )

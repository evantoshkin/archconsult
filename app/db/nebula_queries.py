import logging
from typing import Optional

from nebula3.gclient.net import ConnectionPool

from app.core.config import settings
from app.db.nebula_pool import get_nebula_pool
from app.schemas.paths import TraverseFilter

logger = logging.getLogger(__name__)


async def execute_nebula_experiment_search(
    start_filter: TraverseFilter,
    finish_filter: TraverseFilter,
    depth_days: int,
    source_type: str = "vision",
) -> dict[str, dict[str, dict]]:
    from datetime import datetime, timedelta
    
    cutoff_date = (datetime.now() - timedelta(days=depth_days)).strftime("%Y-%m-%dT%H:%M:%S")
    logger.info(f"Searching paths with depth_days={depth_days}, cutoff_date={cutoff_date}")
    
    edge_type = "VISION_INTERFACE_SYSTEM_LEVEL"
    if source_type == "interface_registry":
        edge_type = "INTERFACE_REGISTRY_INTERFACE_SYSTEM_LEVEL"
    logger.info(f"Using edge type: {edge_type} (source_type={source_type})")
    pool = get_nebula_pool()
    
    session = pool.get_session(settings.NEBULA_USER, settings.NEBULA_PASSWORD)
    
    try:
        result = session.execute(f'USE {settings.NEBULA_SPACE};')
        if not result.is_succeeded():
            logger.error(f"Failed to use space {settings.NEBULA_SPACE}: {result.error_msg()}")
            return {}
        
        if not start_filter.system_rsm_id:
            logger.info("No start system_rsm_id provided, returning empty results")
            return {}
        
        # Build filter conditions
        start_edge_conditions = f'{edge_type}.rsm_document_date > "{cutoff_date}"'
        if start_filter.module_rsm_id:
            start_edge_conditions += f' AND {edge_type}.consumer_module_id == "{start_filter.module_rsm_id}"'
        if start_filter.component_rsm_id:
            start_edge_conditions += f' AND {edge_type}.consumer_component_id == "{start_filter.component_rsm_id}"'
        
        system_query = f"""
        FETCH PROP ON SYSTEM "{start_filter.system_rsm_id}"
        YIELD vertex AS v
        """
        
        logger.info(f"Executing system query: {system_query}")
        system_result = session.execute(system_query)
        
        if not system_result.is_succeeded():
            logger.error(f"System query failed: {system_result.error_msg()}")
            return {}
        
        if system_result.row_size() == 0:
            logger.info(f"System {start_filter.system_rsm_id} not found")
            return {}
        
        def collect_document_data(
            result,
            target_dict: dict[str, dict],
            label: str
        ):
            if not result.is_succeeded():
                logger.error(f"{label} query failed: {result.error_msg()}")
                return
            for row_index in range(result.row_size()):
                row = result.row_values(row_index)
                if len(row) < 6:
                    continue
                document_id = str(row[0]) if row[0] else None
                if not document_id or document_id in ("None", "__EMPTY__"):
                    continue
                document_date = str(row[1]) if row[1] else None
                consumer_module_id = str(row[2]) if row[2] else None
                provider_module_id = str(row[3]) if row[3] else None
                consumer_component_id = str(row[4]) if row[4] else None
                provider_component_id = str(row[5]) if row[5] else None
                document_date_clean = None
                if document_date and document_date not in ("None", "__EMPTY__"):
                    document_date_clean = document_date.strip('"').strip()
                consumer_module_id_clean = None
                if consumer_module_id and consumer_module_id not in ("None", "__EMPTY__"):
                    consumer_module_id_clean = consumer_module_id.strip('"').strip()
                provider_module_id_clean = None
                if provider_module_id and provider_module_id not in ("None", "__EMPTY__"):
                    provider_module_id_clean = provider_module_id.strip('"').strip()
                consumer_component_id_clean = None
                if consumer_component_id and consumer_component_id not in ("None", "__EMPTY__"):
                    consumer_component_id_clean = consumer_component_id.strip('"').strip()
                provider_component_id_clean = None
                if provider_component_id and provider_component_id not in ("None", "__EMPTY__"):
                    provider_component_id_clean = provider_component_id.strip('"').strip()
                if document_id not in target_dict:
                    target_dict[document_id] = {
                        "document_date": document_date_clean,
                        "consumer_module_id": consumer_module_id_clean,
                        "provider_module_id": provider_module_id_clean,
                        "consumer_component_id": consumer_component_id_clean,
                        "provider_component_id": provider_component_id_clean,
                    }
                elif document_date_clean:
                    existing = target_dict[document_id].get("document_date")
                    if existing is None or document_date_clean > existing:
                        target_dict[document_id]["document_date"] = document_date_clean
                        target_dict[document_id]["consumer_module_id"] = consumer_module_id_clean
                        target_dict[document_id]["provider_module_id"] = provider_module_id_clean
                        target_dict[document_id]["consumer_component_id"] = consumer_component_id_clean
                        target_dict[document_id]["provider_component_id"] = provider_component_id_clean
        
        outgoing_document_data: dict[str, dict] = {}
        
        edges_query = f"""
        GO FROM "{start_filter.system_rsm_id}" OVER {edge_type} BIDIRECT
        WHERE {start_edge_conditions}
        YIELD 
            {edge_type}.rsm_document_id AS document_id,
            {edge_type}.rsm_document_date AS document_date,
            {edge_type}.consumer_module_id AS consumer_module_id,
            {edge_type}.provider_module_id AS provider_module_id,
            {edge_type}.consumer_component_id AS consumer_component_id,
            {edge_type}.provider_component_id AS provider_component_id
        """
        
        logger.info(f"Executing start edges query (BIDIRECT): {edges_query}")
        collect_document_data(session.execute(edges_query), outgoing_document_data, "start_bidirect")
        
        logger.info(f"Found {len(outgoing_document_data)} document_ids from start system (BIDIRECT)")
        
        incoming_document_data: dict[str, dict] = {}
        
        if finish_filter.system_rsm_id:
            incoming_query = f"""
            GO FROM "{finish_filter.system_rsm_id}" OVER {edge_type} BIDIRECT
            WHERE {edge_type}.rsm_document_date > "{cutoff_date}"
            YIELD 
                {edge_type}.rsm_document_id AS document_id,
                {edge_type}.rsm_document_date AS document_date,
                {edge_type}.consumer_module_id AS consumer_module_id,
                {edge_type}.provider_module_id AS provider_module_id,
                {edge_type}.consumer_component_id AS consumer_component_id,
                {edge_type}.provider_component_id AS provider_component_id
            """
            
            logger.info(f"Executing finish edges query (BIDIRECT): {incoming_query}")
            collect_document_data(session.execute(incoming_query), incoming_document_data, "finish_bidirect")
            
            logger.info(f"Found {len(incoming_document_data)} document_ids to finish system (BIDIRECT)")
        
        outgoing_document_ids = set(outgoing_document_data.keys())
        incoming_document_ids = set(incoming_document_data.keys())
        matching_document_ids = outgoing_document_ids & incoming_document_ids
        
        logger.info(f"Found {len(matching_document_ids)} matching document_ids between start and finish systems")
        
        results: dict[str, dict[str, dict]] = {}
        
        for document_id in matching_document_ids:
            clean_document_id = document_id.strip('"')
            
            paths_for_document: dict[str, dict] = {}
            
            def process_path_result(path_result):
                if not path_result.is_succeeded():
                    logger.error(f"Path query failed for document_id {document_id}: {path_result.error_msg()}")
                    return
                
                for row_index in range(path_result.row_size()):
                    row = path_result.row_values(row_index)
                    
                    if len(row) < 1:
                        continue
                    
                    path = row[0] if row[0] else None
                    if path:
                        raw_path = str(path)
                        temp_nodes = []
                        temp_dirs = []
                        idx = 0
                        while idx < len(raw_path):
                            if raw_path[idx] == '(' and idx + 1 < len(raw_path) and raw_path[idx + 1] == '"':
                                start = idx + 2
                                end = raw_path.index('"', start)
                                temp_nodes.append(raw_path[start:end])
                                idx = end + 2
                            elif raw_path[idx:idx+1] == "<" and raw_path[idx:idx+2] == "<-":
                                temp_dirs.append("reverse")
                                idx += 2
                            elif raw_path[idx:idx+2] == "->":
                                temp_dirs.append("forward")
                                idx += 2
                            else:
                                idx += 1
                        
                        nodes = temp_nodes
                        edge_directions = temp_dirs[:len(nodes)-1] if len(temp_dirs) >= len(nodes) - 1 else ["forward"] * (len(nodes) - 1)
                        
                        path_key = tuple(nodes)
                        
                        if path_key not in paths_for_document:
                            edge_data_list = []
                            edge_cache: dict[tuple, dict] = {}
                            for i in range(len(nodes) - 1):
                                from_node = nodes[i]
                                to_node = nodes[i + 1]
                                is_reverse = (edge_directions[i] == "reverse") if i < len(edge_directions) else False

                                if from_node == to_node:
                                    logger.info(f"Skipping self-loop edge query: {from_node} == {to_node}")
                                    edge_cache_key = (from_node, to_node, is_reverse)
                                    edge_cache[edge_cache_key] = [{
                                        "consumer_module_id": "",
                                        "provider_module_id": "",
                                        "consumer_component_id": "",
                                        "provider_component_id": "",
                                    }]
                                    edge_data_list.append(edge_cache[edge_cache_key])
                                    continue

                                edge_cache_key = (from_node, to_node, is_reverse)
                                if edge_cache_key in edge_cache:
                                    logger.info(f"Reusing cached edge data for {from_node} -> {to_node} ({'reverse' if is_reverse else 'forward'})")
                                    edge_data_list.append(edge_cache[edge_cache_key])
                                    continue

                                if not is_reverse:
                                    edge_query = f'GO FROM "{from_node}" OVER {edge_type} WHERE {edge_type}.rsm_document_id == "{clean_document_id}" AND {edge_type}.rsm_document_date > "{cutoff_date}" AND id($$) == "{to_node}" YIELD {edge_type}.consumer_module_id, {edge_type}.provider_module_id, {edge_type}.consumer_component_id, {edge_type}.provider_component_id'
                                else:
                                    edge_query = f'GO FROM "{from_node}" OVER {edge_type} REVERSELY WHERE {edge_type}.rsm_document_id == "{clean_document_id}" AND {edge_type}.rsm_document_date > "{cutoff_date}" AND id($$) == "{to_node}" YIELD {edge_type}.consumer_module_id, {edge_type}.provider_module_id, {edge_type}.consumer_component_id, {edge_type}.provider_component_id'
                                
                                logger.info(f"Executing edge query ({'forward' if not is_reverse else 'reverse'}): {edge_query}")
                                edge_result = session.execute(edge_query)
                                
                                combos: list[dict] = []
                                seen: set[tuple] = set()
                                if edge_result.is_succeeded():
                                    for edge_row_idx in range(edge_result.row_size()):
                                        edge_row = edge_result.row_values(edge_row_idx)
                                        if len(edge_row) < 4:
                                            continue
                                        combo = {
                                            "consumer_module_id": str(edge_row[0]).strip('"') if edge_row[0] and str(edge_row[0]) not in ["None", "__EMPTY__", '"NULL"'] else "",
                                            "provider_module_id": str(edge_row[1]).strip('"') if edge_row[1] and str(edge_row[1]) not in ["None", "__EMPTY__", '"NULL"'] else "",
                                            "consumer_component_id": str(edge_row[2]).strip('"') if edge_row[2] and str(edge_row[2]) not in ["None", "__EMPTY__", '"NULL"'] else "",
                                            "provider_component_id": str(edge_row[3]).strip('"') if edge_row[3] and str(edge_row[3]) not in ["None", "__EMPTY__", '"NULL"'] else "",
                                        }
                                        combo_key = (
                                            combo["consumer_module_id"],
                                            combo["consumer_component_id"],
                                            combo["provider_module_id"],
                                            combo["provider_component_id"],
                                        )
                                        if combo_key not in seen:
                                            seen.add(combo_key)
                                            combos.append(combo)
                                
                                if not combos:
                                    combos = [{
                                        "consumer_module_id": "",
                                        "provider_module_id": "",
                                        "consumer_component_id": "",
                                        "provider_component_id": "",
                                    }]
                                
                                edge_data_list.append(combos)
                                edge_cache[edge_cache_key] = combos
                            
                            paths_for_document[path_key] = {
                                "path": nodes,
                                "distance": len(nodes),
                                "edge_data": edge_data_list,
                                "edge_directions": edge_directions,
                            }
            
            # Query 1: Direct edges (forward direction)
            path_query_forward = f'FIND NOLOOP PATH FROM "{start_filter.system_rsm_id}" TO "{finish_filter.system_rsm_id}" OVER {edge_type} BIDIRECT WHERE {edge_type}.rsm_document_id == "{clean_document_id}" AND {edge_type}.rsm_document_date > "{cutoff_date}" UPTO {settings.MAX_PATH_DEPTH} STEPS YIELD path AS p'
            
            logger.info(f"Executing forward path query for document_id {document_id}: {path_query_forward}")
            path_result_forward = session.execute(path_query_forward)
            process_path_result(path_result_forward)
            
            
            
            if paths_for_document:
                out_data = outgoing_document_data.get(document_id, {})
                in_data = incoming_document_data.get(document_id, {})
                
                out_date = out_data.get("document_date")
                in_date = in_data.get("document_date")
                dates = [d for d in [out_date, in_date] if d]
                latest_date = max(dates) if dates else None
                
                consumer_module_id = out_data.get("consumer_module_id") or in_data.get("consumer_module_id")
                provider_module_id = out_data.get("provider_module_id") or in_data.get("provider_module_id")
                consumer_component_id = out_data.get("consumer_component_id") or in_data.get("consumer_component_id")
                provider_component_id = out_data.get("provider_component_id") or in_data.get("provider_component_id")
                
                results[document_id] = {
                    "paths": paths_for_document,
                    "document_rsm_date_time": latest_date,
                    "consumer_module_id": consumer_module_id or "",
                    "provider_module_id": provider_module_id or "",
                    "consumer_component_id": consumer_component_id or "",
                    "provider_component_id": provider_component_id or "",
                }
        
        logger.info(f"NebulaGraph experiment search returned {len(results)} matching document groups with paths")
        return results
        
    except Exception as e:
        logger.error(f"NebulaGraph query error: {e}")
        return {}
    finally:
        session.release()


async def fetch_one_hop_neighbors(
    start_filter: TraverseFilter,
    depth_days: int,
    source_type: str = "vision",
) -> dict[str, dict[str, dict]]:
    from datetime import datetime, timedelta

    cutoff_date = (datetime.now() - timedelta(days=depth_days)).strftime("%Y-%m-%dT%H:%M:%S")

    edge_type = "VISION_INTERFACE_SYSTEM_LEVEL"
    if source_type == "interface_registry":
        edge_type = "INTERFACE_REGISTRY_INTERFACE_SYSTEM_LEVEL"

    pool = get_nebula_pool()
    session = pool.get_session(settings.NEBULA_USER, settings.NEBULA_PASSWORD)

    try:
        result = session.execute(f'USE {settings.NEBULA_SPACE};')
        if not result.is_succeeded():
            return {}

        if not start_filter.system_rsm_id:
            return {}

        edge_date_conditions = '{et}.rsm_document_date > "{cd}"'.format(et=edge_type, cd=cutoff_date)
        edge_filter_conditions = ""
        if start_filter.module_rsm_id:
            edge_filter_conditions += ' AND {et}.consumer_module_id == "{mid}"'.format(et=edge_type, mid=start_filter.module_rsm_id)
        if start_filter.component_rsm_id:
            edge_filter_conditions += ' AND {et}.consumer_component_id == "{cid}"'.format(et=edge_type, cid=start_filter.component_rsm_id)

        results: dict[str, dict[str, dict]] = {}

        def _normalize(val: str | None) -> str | None:
            if not val:
                return None
            stripped = val.strip('"').strip()
            if stripped in ("", "None", "__EMPTY__", "NULL"):
                return None
            return stripped

        def _process_rows(result, direction: str):
            if not result.is_succeeded():
                return
            for row_index in range(result.row_size()):
                row = result.row_values(row_index)
                if len(row) < 7:
                    continue

                document_id = str(row[0]) if row[0] else None
                if not document_id or document_id in ("None", "__EMPTY__"):
                    continue

                document_date = str(row[1]) if row[1] else None
                consumer_module_id = _normalize(str(row[2]) if row[2] else None)
                provider_module_id = _normalize(str(row[3]) if row[3] else None)
                consumer_component_id = _normalize(str(row[4]) if row[4] else None)
                provider_component_id = _normalize(str(row[5]) if row[5] else None)
                neighbor_id = str(row[6]).strip('"') if row[6] else None

                if not neighbor_id or neighbor_id == start_filter.system_rsm_id:
                    continue

                document_date_clean = None
                if document_date and document_date not in ("None", "__EMPTY__"):
                    document_date_clean = document_date.strip('"').strip()

                if direction == "forward":
                    nodes = [start_filter.system_rsm_id, neighbor_id]
                    edge_data = {
                        "consumer_module_id": consumer_module_id or "",
                        "provider_module_id": provider_module_id or "",
                        "consumer_component_id": consumer_component_id or "",
                        "provider_component_id": provider_component_id or "",
                    }
                    edge_directions = ["forward"]
                else:
                    nodes = [start_filter.system_rsm_id, neighbor_id]
                    edge_data = {
                        "consumer_module_id": consumer_module_id or "",
                        "provider_module_id": provider_module_id or "",
                        "consumer_component_id": consumer_component_id or "",
                        "provider_component_id": provider_component_id or "",
                    }
                    edge_directions = ["reverse"]

                path_key = tuple(nodes)

                if document_id not in results:
                    results[document_id] = {
                        "paths": {},
                        "document_rsm_date_time": None,
                    }

                if path_key not in results[document_id]["paths"]:
                    results[document_id]["paths"][path_key] = {
                        "path": nodes,
                        "distance": 2,
                        "edge_data": [edge_data],
                        "edge_directions": edge_directions,
                    }

                existing_date = results[document_id].get("document_rsm_date_time")
                if document_date_clean and (not existing_date or document_date_clean > existing_date):
                    results[document_id]["document_rsm_date_time"] = document_date_clean

        # Forward edges: start=consumer, neighbor=provider
        forward_query = (
            'GO FROM "' + start_filter.system_rsm_id + '" OVER ' + edge_type + ' '
            'WHERE ' + edge_date_conditions + edge_filter_conditions + ' '
            'YIELD ' + edge_type + '.rsm_document_id AS document_id, '
            + edge_type + '.rsm_document_date AS document_date, '
            + edge_type + '.consumer_module_id AS consumer_module_id, '
            + edge_type + '.provider_module_id AS provider_module_id, '
            + edge_type + '.consumer_component_id AS consumer_component_id, '
            + edge_type + '.provider_component_id AS provider_component_id, '
            'id($$) AS neighbor_id'
        )
        logger.info(f"One-hop forward query: {forward_query}")
        _process_rows(session.execute(forward_query), "forward")

        # Reverse edges: start=provider, neighbor=consumer
        reverse_query = (
            'GO FROM "' + start_filter.system_rsm_id + '" OVER ' + edge_type + ' REVERSELY '
            'WHERE ' + edge_date_conditions + edge_filter_conditions + ' '
            'YIELD ' + edge_type + '.rsm_document_id AS document_id, '
            + edge_type + '.rsm_document_date AS document_date, '
            + edge_type + '.consumer_module_id AS consumer_module_id, '
            + edge_type + '.provider_module_id AS provider_module_id, '
            + edge_type + '.consumer_component_id AS consumer_component_id, '
            + edge_type + '.provider_component_id AS provider_component_id, '
            'id($$) AS neighbor_id'
        )
        logger.info(f"One-hop reverse query: {reverse_query}")
        _process_rows(session.execute(reverse_query), "reverse")

        return results

    except Exception as e:
        logger.error(f"NebulaGraph one-hop query error: {e}")
        return {}
    finally:
        session.release()


async def fetch_one_hop_neighbors_to_finish(
    finish_filter: TraverseFilter,
    depth_days: int,
    source_type: str = "vision",
) -> dict[str, dict[str, dict]]:
    from datetime import datetime, timedelta

    cutoff_date = (datetime.now() - timedelta(days=depth_days)).strftime("%Y-%m-%dT%H:%M:%S")

    edge_type = "VISION_INTERFACE_SYSTEM_LEVEL"
    if source_type == "interface_registry":
        edge_type = "INTERFACE_REGISTRY_INTERFACE_SYSTEM_LEVEL"

    pool = get_nebula_pool()
    session = pool.get_session(settings.NEBULA_USER, settings.NEBULA_PASSWORD)

    try:
        result = session.execute(f'USE {settings.NEBULA_SPACE};')
        if not result.is_succeeded():
            return {}

        if not finish_filter.system_rsm_id:
            return {}

        edge_date_conditions = '{et}.rsm_document_date > "{cd}"'.format(et=edge_type, cd=cutoff_date)
        edge_filter_conditions = ""
        if finish_filter.module_rsm_id:
            edge_filter_conditions += ' AND {et}.provider_module_id == "{mid}"'.format(et=edge_type, mid=finish_filter.module_rsm_id)
        if finish_filter.component_rsm_id:
            edge_filter_conditions += ' AND {et}.provider_component_id == "{cid}"'.format(et=edge_type, cid=finish_filter.component_rsm_id)

        results: dict[str, dict[str, dict]] = {}

        def _normalize(val: str | None) -> str | None:
            if not val:
                return None
            stripped = val.strip('"').strip()
            if stripped in ("", "None", "__EMPTY__", "NULL"):
                return None
            return stripped

        def _process_rows(result, direction: str):
            if not result.is_succeeded():
                return
            for row_index in range(result.row_size()):
                row = result.row_values(row_index)
                if len(row) < 7:
                    continue

                document_id = str(row[0]) if row[0] else None
                if not document_id or document_id in ("None", "__EMPTY__"):
                    continue

                document_date = str(row[1]) if row[1] else None
                consumer_module_id = _normalize(str(row[2]) if row[2] else None)
                provider_module_id = _normalize(str(row[3]) if row[3] else None)
                consumer_component_id = _normalize(str(row[4]) if row[4] else None)
                provider_component_id = _normalize(str(row[5]) if row[5] else None)
                neighbor_id = str(row[6]).strip('"') if row[6] else None

                if not neighbor_id or neighbor_id == finish_filter.system_rsm_id:
                    continue

                document_date_clean = None
                if document_date and document_date not in ("None", "__EMPTY__"):
                    document_date_clean = document_date.strip('"').strip()

                if direction == "forward":
                    # Forward edge: finish is provider/destination, neighbor is consumer/source.
                    nodes = [neighbor_id, finish_filter.system_rsm_id]
                    edge_data = {
                        "consumer_module_id": consumer_module_id or "",
                        "provider_module_id": provider_module_id or "",
                        "consumer_component_id": consumer_component_id or "",
                        "provider_component_id": provider_component_id or "",
                    }
                    edge_directions = ["forward"]
                else:
                    # Reverse edge: finish is consumer/destination, neighbor is provider/source.
                    nodes = [neighbor_id, finish_filter.system_rsm_id]
                    edge_data = {
                        "consumer_module_id": consumer_module_id or "",
                        "provider_module_id": provider_module_id or "",
                        "consumer_component_id": consumer_component_id or "",
                        "provider_component_id": provider_component_id or "",
                    }
                    edge_directions = ["reverse"]

                path_key = tuple(nodes)

                if document_id not in results:
                    results[document_id] = {
                        "paths": {},
                        "document_rsm_date_time": None,
                    }

                if path_key not in results[document_id]["paths"]:
                    results[document_id]["paths"][path_key] = {
                        "path": nodes,
                        "distance": 2,
                        "edge_data": [edge_data],
                        "edge_directions": edge_directions,
                    }

                existing_date = results[document_id].get("document_rsm_date_time")
                if document_date_clean and (not existing_date or document_date_clean > existing_date):
                    results[document_id]["document_rsm_date_time"] = document_date_clean

        # Forward edges: finish=provider/destination, neighbor=consumer/source
        forward_query = (
            'GO FROM "' + finish_filter.system_rsm_id + '" OVER ' + edge_type + ' REVERSELY '
            'WHERE ' + edge_date_conditions + edge_filter_conditions + ' '
            'YIELD ' + edge_type + '.rsm_document_id AS document_id, '
            + edge_type + '.rsm_document_date AS document_date, '
            + edge_type + '.consumer_module_id AS consumer_module_id, '
            + edge_type + '.provider_module_id AS provider_module_id, '
            + edge_type + '.consumer_component_id AS consumer_component_id, '
            + edge_type + '.provider_component_id AS provider_component_id, '
            'id($$) AS neighbor_id'
        )
        logger.info(f"Finish-anchored forward query: {forward_query}")
        _process_rows(session.execute(forward_query), "forward")

        # Reverse edges: finish=consumer/destination, neighbor=provider/source
        reverse_query = (
            'GO FROM "' + finish_filter.system_rsm_id + '" OVER ' + edge_type + ' '
            'WHERE ' + edge_date_conditions + edge_filter_conditions + ' '
            'YIELD ' + edge_type + '.rsm_document_id AS document_id, '
            + edge_type + '.rsm_document_date AS document_date, '
            + edge_type + '.consumer_module_id AS consumer_module_id, '
            + edge_type + '.provider_module_id AS provider_module_id, '
            + edge_type + '.consumer_component_id AS consumer_component_id, '
            + edge_type + '.provider_component_id AS provider_component_id, '
            'id($$) AS neighbor_id'
        )
        logger.info(f"Finish-anchored reverse query: {reverse_query}")
        _process_rows(session.execute(reverse_query), "reverse")

        return results

    except Exception as e:
        logger.error(f"NebulaGraph finish-anchored one-hop query error: {e}")
        return {}
    finally:
        session.release()


async def fetch_nebula_node_names(
    nodes: list[tuple[str, str, str]]
) -> dict[tuple, dict]:
    if not nodes:
        return {}
    
    pool = get_nebula_pool()
    session = pool.get_session(settings.NEBULA_USER, settings.NEBULA_PASSWORD)
    
    try:
        result = session.execute(f'USE {settings.NEBULA_SPACE};')
        if not result.is_succeeded():
            logger.error(f"Failed to use space: {result.error_msg()}")
            return {}
        
        names: dict[tuple, dict] = {}
        
        for sys_id, mod_id, comp_id in set(nodes):
            if sys_id:
                query = f'FETCH PROP ON SYSTEM "{sys_id}" YIELD vertex as v'
                result = session.execute(query)
                if result.is_succeeded() and result.row_size() > 0:
                    row = result.row_values(0)
                    vertex_str = str(row[0]) if row[0] else ""
                    
                    name = None
                    if "name:" in vertex_str or "rsm_name:" in vertex_str:
                        import re
                        match = re.search(r'(?:name|rsm_name):\s*"([^"]*)"', vertex_str)
                        if match:
                            name = match.group(1)
                    
                    key = (sys_id, "", "")
                    if key not in names:
                        names[key] = {}
                    if name and name != "None":
                        names[key]["system_rsm_name"] = name
            
            if mod_id:
                query = f'FETCH PROP ON MODULE "{mod_id}" YIELD vertex as v'
                result = session.execute(query)
                if result.is_succeeded() and result.row_size() > 0:
                    row = result.row_values(0)
                    vertex_str = str(row[0]) if row[0] else ""
                    
                    name = None
                    import re
                    if "module_rsm_name:" in vertex_str:
                        match = re.search(r'module_rsm_name:\s*"([^"]*)"', vertex_str)
                        if match:
                            name = match.group(1)
                    elif "name:" in vertex_str:
                        match = re.search(r'name:\s*"([^"]*)"', vertex_str)
                        if match:
                            name = match.group(1)
                    
                    key = (sys_id, mod_id, comp_id)
                    if key not in names:
                        names[key] = {}
                    if name and name != "None":
                        names[key]["module_rsm_name"] = name
            
            if comp_id:
                query = f'FETCH PROP ON COMPONENT "{comp_id}" YIELD vertex as v'
                result = session.execute(query)
                if result.is_succeeded() and result.row_size() > 0:
                    row = result.row_values(0)
                    vertex_str = str(row[0]) if row[0] else ""
                    
                    name = None
                    import re
                    if "component_rsm_name:" in vertex_str:
                        match = re.search(r'component_rsm_name:\s*"([^"]*)"', vertex_str)
                        if match:
                            name = match.group(1)
                    elif "name:" in vertex_str:
                        match = re.search(r'name:\s*"([^"]*)"', vertex_str)
                        if match:
                            name = match.group(1)
                    
                    key = (sys_id, mod_id, comp_id)
                    if key not in names:
                        names[key] = {}
                    if name and name != "None":
                        names[key]["component_rsm_name"] = name
        
        logger.info(f"Fetched names for {len(names)} nodes from NebulaGraph")
        return names
        
    except Exception as e:
        logger.error(f"NebulaGraph node names fetch error: {e}")
        return {}
    finally:
        session.release()


async def fetch_child_tree_from_nebula(rsm_id: str) -> dict:
    """
    Fetch child tree from NebulaGraph using hierarchy edges.
    Returns a tree structure with node and children.
    """
    pool = get_nebula_pool()
    session = pool.get_session(settings.NEBULA_USER, settings.NEBULA_PASSWORD)
    
    def parse_vertex(vertex_str: str) -> dict:
        """Parse vertex string and extract label, name and description."""
        import re
        
        label = ""
        name = None
        description = None
        
        # Extract label from vertex string (e.g., :SYSTEM, :MODULE, :COMPONENT)
        label_match = re.search(r':([A-Za-z]+)[{]', vertex_str)
        if label_match:
            label = label_match.group(1)
        
        # Extract name property
        name_match = re.search(r'name:\s*"([^"]*)"', vertex_str)
        if name_match:
            name = name_match.group(1)
        
        # Extract description property
        description_match = re.search(r'(?:description|rsm_description):\s*"([^"]*)"', vertex_str)
        if description_match:
            description = description_match.group(1)
        
        return {
            "label": label,
            "rsm_name": name,
            "description": description,
        }
    
    def fetch_children_recursive(node_id: str, visited: set) -> list:
        """Recursively fetch children for a node."""
        if node_id in visited:
            return []
        visited.add(node_id)
        
        children_query = f'GO FROM "{node_id}" OVER HIERARCHY REVERSELY YIELD id($$) AS child_id, $$ AS child_vertex'
        children_result = session.execute(children_query)
        
        children = []
        if children_result.is_succeeded():
            for row_idx in range(children_result.row_size()):
                row = children_result.row_values(row_idx)
                child_id = str(row[0]).strip('"') if row[0] else None
                child_vertex = str(row[1]) if row[1] else ""
                
                if child_id and child_id not in visited:
                    parsed = parse_vertex(child_vertex)
                    children.append({
                        "node": {
                            "label": parsed["label"],
                            "rsm_id": child_id,
                            "rsm_name": parsed["rsm_name"],
                        },
                        "children": fetch_children_recursive(child_id, visited)
                    })
        
        return children
    
    try:
        result = session.execute(f'USE {settings.NEBULA_SPACE};')
        if not result.is_succeeded():
            logger.error(f"Failed to use space {settings.NEBULA_SPACE}: {result.error_msg()}")
            return None
        
        # Check if node exists and get its properties
        check_query = f'FETCH PROP ON * "{rsm_id}" YIELD vertex AS v'
        check_result = session.execute(check_query)
        
        if not check_result.is_succeeded() or check_result.row_size() == 0:
            logger.info(f"Node {rsm_id} not found")
            return None
        
        # Get node properties
        row = check_result.row_values(0)
        vertex_str = str(row[0]) if row[0] else ""
        parsed = parse_vertex(vertex_str)
        
        # Fetch children recursively
        visited = set()
        children = fetch_children_recursive(rsm_id, visited)
        
        logger.info(f"Found {len(children)} direct children for node {rsm_id}")
        
        return {
            "node": {
                "label": parsed["label"],
                "rsm_id": rsm_id,
                "rsm_name": parsed["rsm_name"],
                "description": parsed["description"],
            },
            "children": children
        }
        
    except Exception as e:
        logger.error(f"NebulaGraph query error in fetch_child_tree_from_nebula: {e}")
        return None
    finally:
        session.release()

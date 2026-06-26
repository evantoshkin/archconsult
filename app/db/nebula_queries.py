import logging
from typing import Optional

from nebula3.gclient.net import ConnectionPool

from app.core.config import settings
from app.db.nebula_pool import get_nebula_pool
from app.schemas.paths import TraverseFilter

logger = logging.getLogger(__name__)


async def fetch_node_paths_with_document_id(
    session,
    node_id: str,
    document_id: str,
    depth: int = 2,
) -> dict:
    result_data = {
        "incoming": [],
        "outgoing": [],
    }
    
    outgoing_query = f'GO FROM "{node_id}" OVER VISION_INTERFACE_SYSTEM_LEVEL WHERE VISION_INTERFACE_SYSTEM_LEVEL.rsm_document_id == "{document_id}" YIELD $$.SYSTEM.system_rsm_id, $$.SYSTEM.system_rsm_name'
    logger.info(f"Executing outgoing query for node {node_id}: {outgoing_query}")
    outgoing_result = session.execute(outgoing_query)
    
    if outgoing_result.is_succeeded():
        for row_idx in range(outgoing_result.row_size()):
            row = outgoing_result.row_values(row_idx)
            system_id = str(row[0]).strip('"') if row[0] else None
            system_name = str(row[1]).strip('"') if row[1] and str(row[1]) not in ["None", "__EMPTY__", '"NULL"'] else None
            
            if system_id and system_id != node_id:
                result_data["outgoing"].append({
                    "system_rsm_id": system_id,
                    "system_rsm_name": system_name,
                })
    
    incoming_query = f'GO FROM "{node_id}" OVER VISION_INTERFACE_SYSTEM_LEVEL REVERSELY WHERE VISION_INTERFACE_SYSTEM_LEVEL.rsm_document_id == "{document_id}" YIELD $$.SYSTEM.system_rsm_id, $$.SYSTEM.system_rsm_name'
    logger.info(f"Executing incoming query for node {node_id}: {incoming_query}")
    incoming_result = session.execute(incoming_query)
    
    if incoming_result.is_succeeded():
        for row_idx in range(incoming_result.row_size()):
            row = incoming_result.row_values(row_idx)
            system_id = str(row[0]).strip('"') if row[0] else None
            system_name = str(row[1]).strip('"') if row[1] and str(row[1]) not in ["None", "__EMPTY__", '"NULL"'] else None
            
            if system_id and system_id != node_id:
                result_data["incoming"].append({
                    "system_rsm_id": system_id,
                    "system_rsm_name": system_name,
                })
    
    return result_data


async def execute_nebula_experiment_search(
    start_filter: TraverseFilter,
    finish_filter: TraverseFilter,
    depth_days: int,
) -> dict[str, dict[str, dict]]:
    from datetime import datetime, timedelta
    
    cutoff_date = (datetime.now() - timedelta(days=depth_days)).strftime("%Y-%m-%dT%H:%M:%S")
    logger.info(f"Searching paths with depth_days={depth_days}, cutoff_date={cutoff_date}")
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
        
        edges_query = f"""
        GO FROM "{start_filter.system_rsm_id}" OVER VISION_INTERFACE_SYSTEM_LEVEL
        WHERE VISION_INTERFACE_SYSTEM_LEVEL.rsm_document_date > "{cutoff_date}"
        YIELD 
            VISION_INTERFACE_SYSTEM_LEVEL.rsm_document_id AS document_id,
            VISION_INTERFACE_SYSTEM_LEVEL.rsm_document_date AS document_date,
            VISION_INTERFACE_SYSTEM_LEVEL.consumer_module_id AS consumer_module_id,
            VISION_INTERFACE_SYSTEM_LEVEL.provider_module_id AS provider_module_id,
            VISION_INTERFACE_SYSTEM_LEVEL.consumer_component_id AS consumer_component_id,
            VISION_INTERFACE_SYSTEM_LEVEL.provider_component_id AS provider_component_id
        """
        
        logger.info(f"Executing edges query: {edges_query}")
        edges_result = session.execute(edges_query)
        
        if not edges_result.is_succeeded():
            logger.error(f"Edges query failed: {edges_result.error_msg()}")
            return {}
        
        outgoing_document_data: dict[str, dict] = {}
        
        for row_index in range(edges_result.row_size()):
            row = edges_result.row_values(row_index)
            
            if len(row) < 6:
                continue
            
            document_id = str(row[0]) if row[0] else None
            document_date = str(row[1]) if row[1] else None
            consumer_module_id = str(row[2]) if row[2] else None
            provider_module_id = str(row[3]) if row[3] else None
            consumer_component_id = str(row[4]) if row[4] else None
            provider_component_id = str(row[5]) if row[5] else None
            
            if document_id and document_id != "None" and document_id != "__EMPTY__":
                document_date_clean = None
                if document_date and document_date != "None" and document_date != "__EMPTY__":
                    document_date_clean = document_date.strip('"').strip()
                
                consumer_module_id_clean = None
                if consumer_module_id and consumer_module_id != "None" and consumer_module_id != "__EMPTY__":
                    consumer_module_id_clean = consumer_module_id.strip('"').strip()
                
                provider_module_id_clean = None
                if provider_module_id and provider_module_id != "None" and provider_module_id != "__EMPTY__":
                    provider_module_id_clean = provider_module_id.strip('"').strip()
                
                consumer_component_id_clean = None
                if consumer_component_id and consumer_component_id != "None" and consumer_component_id != "__EMPTY__":
                    consumer_component_id_clean = consumer_component_id.strip('"').strip()
                
                provider_component_id_clean = None
                if provider_component_id and provider_component_id != "None" and provider_component_id != "__EMPTY__":
                    provider_component_id_clean = provider_component_id.strip('"').strip()
                
                if document_id not in outgoing_document_data:
                    outgoing_document_data[document_id] = {
                        "document_date": document_date_clean,
                        "consumer_module_id": consumer_module_id_clean,
                        "provider_module_id": provider_module_id_clean,
                        "consumer_component_id": consumer_component_id_clean,
                        "provider_component_id": provider_component_id_clean,
                    }
                elif document_date_clean:
                    existing = outgoing_document_data[document_id].get("document_date")
                    if existing is None or document_date_clean > existing:
                        outgoing_document_data[document_id]["document_date"] = document_date_clean
                        outgoing_document_data[document_id]["consumer_module_id"] = consumer_module_id_clean
                        outgoing_document_data[document_id]["provider_module_id"] = provider_module_id_clean
                        outgoing_document_data[document_id]["consumer_component_id"] = consumer_component_id_clean
                        outgoing_document_data[document_id]["provider_component_id"] = provider_component_id_clean
        
        logger.info(f"Found {len(outgoing_document_data)} outgoing document_ids from start system")
        
        incoming_document_data: dict[str, dict] = {}
        
        if finish_filter.system_rsm_id:
            incoming_query = f"""
            GO FROM "{finish_filter.system_rsm_id}" OVER VISION_INTERFACE_SYSTEM_LEVEL REVERSELY
            WHERE VISION_INTERFACE_SYSTEM_LEVEL.rsm_document_date > "{cutoff_date}"
            YIELD 
                VISION_INTERFACE_SYSTEM_LEVEL.rsm_document_id AS document_id,
                VISION_INTERFACE_SYSTEM_LEVEL.rsm_document_date AS document_date,
                VISION_INTERFACE_SYSTEM_LEVEL.consumer_module_id AS consumer_module_id,
                VISION_INTERFACE_SYSTEM_LEVEL.provider_module_id AS provider_module_id,
                VISION_INTERFACE_SYSTEM_LEVEL.consumer_component_id AS consumer_component_id,
                VISION_INTERFACE_SYSTEM_LEVEL.provider_component_id AS provider_component_id
            """
            
            logger.info(f"Executing incoming edges query: {incoming_query}")
            incoming_result = session.execute(incoming_query)
            
            if incoming_result.is_succeeded():
                for row_index in range(incoming_result.row_size()):
                    row = incoming_result.row_values(row_index)
                    
                    if len(row) < 6:
                        continue
                    
                    document_id = str(row[0]) if row[0] else None
                    document_date = str(row[1]) if row[1] else None
                    consumer_module_id = str(row[2]) if row[2] else None
                    provider_module_id = str(row[3]) if row[3] else None
                    consumer_component_id = str(row[4]) if row[4] else None
                    provider_component_id = str(row[5]) if row[5] else None
                    
                    if document_id and document_id != "None" and document_id != "__EMPTY__":
                        document_date_clean = None
                        if document_date and document_date != "None" and document_date != "__EMPTY__":
                            document_date_clean = document_date.strip('"').strip()
                        
                        consumer_module_id_clean = None
                        if consumer_module_id and consumer_module_id != "None" and consumer_module_id != "__EMPTY__":
                            consumer_module_id_clean = consumer_module_id.strip('"').strip()
                        
                        provider_module_id_clean = None
                        if provider_module_id and provider_module_id != "None" and provider_module_id != "__EMPTY__":
                            provider_module_id_clean = provider_module_id.strip('"').strip()
                        
                        consumer_component_id_clean = None
                        if consumer_component_id and consumer_component_id != "None" and consumer_component_id != "__EMPTY__":
                            consumer_component_id_clean = consumer_component_id.strip('"').strip()
                        
                        provider_component_id_clean = None
                        if provider_component_id and provider_component_id != "None" and provider_component_id != "__EMPTY__":
                            provider_component_id_clean = provider_component_id.strip('"').strip()
                        
                        if document_id not in incoming_document_data:
                            incoming_document_data[document_id] = {
                                "document_date": document_date_clean,
                                "consumer_module_id": consumer_module_id_clean,
                                "provider_module_id": provider_module_id_clean,
                                "consumer_component_id": consumer_component_id_clean,
                                "provider_component_id": provider_component_id_clean,
                            }
                        elif document_date_clean:
                            existing = incoming_document_data[document_id].get("document_date")
                            if existing is None or document_date_clean > existing:
                                incoming_document_data[document_id]["document_date"] = document_date_clean
                                incoming_document_data[document_id]["consumer_module_id"] = consumer_module_id_clean
                                incoming_document_data[document_id]["provider_module_id"] = provider_module_id_clean
                                incoming_document_data[document_id]["consumer_component_id"] = consumer_component_id_clean
                                incoming_document_data[document_id]["provider_component_id"] = provider_component_id_clean
                
                logger.info(f"Found {len(incoming_document_data)} incoming document_ids to finish system")
            else:
                logger.error(f"Incoming edges query failed: {incoming_result.error_msg()}")
        
        outgoing_document_ids = set(outgoing_document_data.keys())
        incoming_document_ids = set(incoming_document_data.keys())
        matching_document_ids = outgoing_document_ids & incoming_document_ids
        
        logger.info(f"Found {len(matching_document_ids)} matching document_ids between start and finish systems")
        
        results: dict[str, dict[str, dict]] = {}
        
        for document_id in matching_document_ids:
            clean_document_id = document_id.strip('"')
            path_query = f'FIND ALL PATH FROM "{start_filter.system_rsm_id}" TO "{finish_filter.system_rsm_id}" OVER VISION_INTERFACE_SYSTEM_LEVEL WHERE VISION_INTERFACE_SYSTEM_LEVEL.rsm_document_id == "{clean_document_id}" AND VISION_INTERFACE_SYSTEM_LEVEL.rsm_document_date > "{cutoff_date}" YIELD path AS p'
            
            logger.info(f"Executing path query for document_id {document_id}: {path_query}")
            path_result = session.execute(path_query)
            
            if not path_result.is_succeeded():
                logger.error(f"Path query failed for document_id {document_id}: {path_result.error_msg()}")
                continue
            
            paths_for_document: dict[str, dict] = {}
            
            for row_index in range(path_result.row_size()):
                row = path_result.row_values(row_index)
                
                if len(row) < 1:
                    continue
                
                path = row[0] if row[0] else None
                if path:
                    path_str = str(path)
                    
                    nodes = []
                    if "->" in path_str:
                        parts = path_str.split("->")
                        for part in parts:
                            if '"' in part:
                                node_id = part.split('"')[1] if '"' in part else ""
                                if node_id:
                                    nodes.append(node_id)
                    
                    path_key = tuple(nodes)
                    
                    if path_key not in paths_for_document:
                        edge_data_list = []
                        for i in range(len(nodes) - 1):
                            from_node = nodes[i]
                            to_node = nodes[i + 1]
                            edge_query = f'GO FROM "{from_node}" OVER VISION_INTERFACE_SYSTEM_LEVEL WHERE VISION_INTERFACE_SYSTEM_LEVEL.rsm_document_id == "{clean_document_id}" AND VISION_INTERFACE_SYSTEM_LEVEL.rsm_document_date > "{cutoff_date}" YIELD VISION_INTERFACE_SYSTEM_LEVEL.consumer_module_id, VISION_INTERFACE_SYSTEM_LEVEL.provider_module_id, VISION_INTERFACE_SYSTEM_LEVEL.consumer_component_id, VISION_INTERFACE_SYSTEM_LEVEL.provider_component_id, $$ AS target_vertex'
                            
                            logger.info(f"Executing edge query: {edge_query}")
                            edge_result = session.execute(edge_query)
                            
                            found = False
                            if edge_result.is_succeeded():
                                for row_idx in range(edge_result.row_size()):
                                    edge_row = edge_result.row_values(row_idx)
                                    target_vertex = str(edge_row[4]) if edge_row[4] else ""
                                    
                                    if to_node in target_vertex:
                                        edge_data_list.append({
                                            "consumer_module_id": str(edge_row[0]).strip('"') if edge_row[0] and str(edge_row[0]) not in ["None", "__EMPTY__", '"NULL"'] else "",
                                            "provider_module_id": str(edge_row[1]).strip('"') if edge_row[1] and str(edge_row[1]) not in ["None", "__EMPTY__", '"NULL"'] else "",
                                            "consumer_component_id": str(edge_row[2]).strip('"') if edge_row[2] and str(edge_row[2]) not in ["None", "__EMPTY__", '"NULL"'] else "",
                                            "provider_component_id": str(edge_row[3]).strip('"') if edge_row[3] and str(edge_row[3]) not in ["None", "__EMPTY__", '"NULL"'] else "",
                                        })
                                        found = True
                                        break
                            
                            if not found:
                                edge_data_list.append({
                                    "consumer_module_id": "",
                                    "provider_module_id": "",
                                    "consumer_component_id": "",
                                    "provider_component_id": "",
                                })
                        
                        paths_for_document[path_key] = {
                            "path": nodes,
                            "distance": len(nodes),
                            "edge_data": edge_data_list,
                        }
            
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


async def execute_nebula_traverse_search(
    start_filter: TraverseFilter,
    finish_filter: TraverseFilter,
) -> dict[str, dict[str, dict]]:
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
        
        system_row = system_result.row_values(0)
        start_vertex_id = str(system_row[0]) if system_row[0] else None
        start_system_name = str(system_row[1]) if len(system_row) > 1 and system_row[1] else None
        
        logger.info(f"Found system: vertex_id={start_vertex_id}, name={start_system_name}")
        
        edges_query = f"""
        GO FROM "{start_filter.system_rsm_id}" OVER VISION_INTERFACE_SYSTEM_LEVEL
        WHERE VISION_INTERFACE_SYSTEM_LEVEL.rsm_document_date > "{cutoff_date}"
        YIELD 
            VISION_INTERFACE_SYSTEM_LEVEL.rsm_document_id AS eotar_id,
            VISION_INTERFACE_SYSTEM_LEVEL.rsm_document_date AS eotar_date,
            VISION_INTERFACE_SYSTEM_LEVEL.consumer_module_id AS consumer_module_id,
            VISION_INTERFACE_SYSTEM_LEVEL.provider_module_id AS provider_module_id,
            VISION_INTERFACE_SYSTEM_LEVEL.consumer_component_id AS consumer_component_id,
            VISION_INTERFACE_SYSTEM_LEVEL.provider_component_id AS provider_component_id
        """
        
        logger.info(f"Executing edges query: {edges_query}")
        edges_result = session.execute(edges_query)
        
        if not edges_result.is_succeeded():
            logger.error(f"Edges query failed: {edges_result.error_msg()}")
            return {}
        
        outgoing_eotar_data: dict[str, dict] = {}
        
        for row_index in range(edges_result.row_size()):
            row = edges_result.row_values(row_index)
            
            if len(row) < 6:
                continue
            
            eotar_id = str(row[0]) if row[0] else None
            eotar_date = str(row[1]) if row[1] else None
            consumer_module_id = str(row[2]) if row[2] else None
            provider_module_id = str(row[3]) if row[3] else None
            consumer_component_id = str(row[4]) if row[4] else None
            provider_component_id = str(row[5]) if row[5] else None
            
            if eotar_id and eotar_id != "None" and eotar_id != "__EMPTY__":
                eotar_date_clean = None
                if eotar_date and eotar_date != "None" and eotar_date != "__EMPTY__":
                    eotar_date_clean = eotar_date.strip('"').strip()
                
                consumer_module_id_clean = None
                if consumer_module_id and consumer_module_id != "None" and consumer_module_id != "__EMPTY__":
                    consumer_module_id_clean = consumer_module_id.strip('"').strip()
                
                provider_module_id_clean = None
                if provider_module_id and provider_module_id != "None" and provider_module_id != "__EMPTY__":
                    provider_module_id_clean = provider_module_id.strip('"').strip()
                
                consumer_component_id_clean = None
                if consumer_component_id and consumer_component_id != "None" and consumer_component_id != "__EMPTY__":
                    consumer_component_id_clean = consumer_component_id.strip('"').strip()
                
                provider_component_id_clean = None
                if provider_component_id and provider_component_id != "None" and provider_component_id != "__EMPTY__":
                    provider_component_id_clean = provider_component_id.strip('"').strip()
                
                if eotar_id not in outgoing_eotar_data:
                    outgoing_eotar_data[eotar_id] = {
                        "eotar_date": eotar_date_clean,
                        "consumer_module_id": consumer_module_id_clean,
                        "provider_module_id": provider_module_id_clean,
                        "consumer_component_id": consumer_component_id_clean,
                        "provider_component_id": provider_component_id_clean,
                    }
                elif eotar_date_clean:
                    existing = outgoing_eotar_data[eotar_id].get("eotar_date")
                    if existing is None or eotar_date_clean > existing:
                        outgoing_eotar_data[eotar_id]["eotar_date"] = eotar_date_clean
                        outgoing_eotar_data[eotar_id]["consumer_module_id"] = consumer_module_id_clean
                        outgoing_eotar_data[eotar_id]["provider_module_id"] = provider_module_id_clean
                        outgoing_eotar_data[eotar_id]["consumer_component_id"] = consumer_component_id_clean
                        outgoing_eotar_data[eotar_id]["provider_component_id"] = provider_component_id_clean
        
        logger.info(f"Found {len(outgoing_eotar_data)} outgoing eotar_ids from start system")
        
        incoming_eotar_data: dict[str, dict] = {}
        
        if finish_filter.system_rsm_id:
            incoming_query = f"""
            GO FROM "{finish_filter.system_rsm_id}" OVER VISION_INTERFACE_SYSTEM_LEVEL REVERSELY
            WHERE VISION_INTERFACE_SYSTEM_LEVEL.rsm_document_date > "{cutoff_date}"
            YIELD 
                VISION_INTERFACE_SYSTEM_LEVEL.rsm_document_id AS eotar_id,
                VISION_INTERFACE_SYSTEM_LEVEL.rsm_document_date AS eotar_date,
                VISION_INTERFACE_SYSTEM_LEVEL.consumer_module_id AS consumer_module_id,
                VISION_INTERFACE_SYSTEM_LEVEL.provider_module_id AS provider_module_id,
                VISION_INTERFACE_SYSTEM_LEVEL.consumer_component_id AS consumer_component_id,
                VISION_INTERFACE_SYSTEM_LEVEL.provider_component_id AS provider_component_id
            """
            
            logger.info(f"Executing incoming edges query: {incoming_query}")
            incoming_result = session.execute(incoming_query)
            
            if incoming_result.is_succeeded():
                for row_index in range(incoming_result.row_size()):
                    row = incoming_result.row_values(row_index)
                    
                    if len(row) < 6:
                        continue
                    
                    eotar_id = str(row[0]) if row[0] else None
                    eotar_date = str(row[1]) if row[1] else None
                    consumer_module_id = str(row[2]) if row[2] else None
                    provider_module_id = str(row[3]) if row[3] else None
                    consumer_component_id = str(row[4]) if row[4] else None
                    provider_component_id = str(row[5]) if row[5] else None
                    
                    if eotar_id and eotar_id != "None" and eotar_id != "__EMPTY__":
                        eotar_date_clean = None
                        if eotar_date and eotar_date != "None" and eotar_date != "__EMPTY__":
                            eotar_date_clean = eotar_date.strip('"').strip()
                        
                        consumer_module_id_clean = None
                        if consumer_module_id and consumer_module_id != "None" and consumer_module_id != "__EMPTY__":
                            consumer_module_id_clean = consumer_module_id.strip('"').strip()
                        
                        provider_module_id_clean = None
                        if provider_module_id and provider_module_id != "None" and provider_module_id != "__EMPTY__":
                            provider_module_id_clean = provider_module_id.strip('"').strip()
                        
                        consumer_component_id_clean = None
                        if consumer_component_id and consumer_component_id != "None" and consumer_component_id != "__EMPTY__":
                            consumer_component_id_clean = consumer_component_id.strip('"').strip()
                        
                        provider_component_id_clean = None
                        if provider_component_id and provider_component_id != "None" and provider_component_id != "__EMPTY__":
                            provider_component_id_clean = provider_component_id.strip('"').strip()
                        
                        if eotar_id not in incoming_eotar_data:
                            incoming_eotar_data[eotar_id] = {
                                "eotar_date": eotar_date_clean,
                                "consumer_module_id": consumer_module_id_clean,
                                "provider_module_id": provider_module_id_clean,
                                "consumer_component_id": consumer_component_id_clean,
                                "provider_component_id": provider_component_id_clean,
                            }
                        elif eotar_date_clean:
                            existing = incoming_eotar_data[eotar_id].get("eotar_date")
                            if existing is None or eotar_date_clean > existing:
                                incoming_eotar_data[eotar_id]["eotar_date"] = eotar_date_clean
                                incoming_eotar_data[eotar_id]["consumer_module_id"] = consumer_module_id_clean
                                incoming_eotar_data[eotar_id]["provider_module_id"] = provider_module_id_clean
                                incoming_eotar_data[eotar_id]["consumer_component_id"] = consumer_component_id_clean
                                incoming_eotar_data[eotar_id]["provider_component_id"] = provider_component_id_clean
                
                logger.info(f"Found {len(incoming_eotar_data)} incoming eotar_ids to finish system")
            else:
                logger.error(f"Incoming edges query failed: {incoming_result.error_msg()}")
        
        outgoing_eotar_ids = set(outgoing_eotar_data.keys())
        incoming_eotar_ids = set(incoming_eotar_data.keys())
        matching_eotar_ids = outgoing_eotar_ids & incoming_eotar_ids
        
        logger.info(f"Found {len(matching_eotar_ids)} matching eotar_ids between start and finish systems")
        
        results: dict[str, dict[str, dict]] = {}
        
        for eotar_id in matching_eotar_ids:
            clean_eotar_id = eotar_id.strip('"')
            path_query = f'FIND ALL PATH FROM "{start_filter.system_rsm_id}" TO "{finish_filter.system_rsm_id}" OVER VISION_INTERFACE_SYSTEM_LEVEL WHERE VISION_INTERFACE_SYSTEM_LEVEL.rsm_document_id == "{clean_eotar_id}" YIELD path AS p'
            
            logger.info(f"Executing path query for eotar_id {eotar_id}: {path_query}")
            path_result = session.execute(path_query)
            
            if not path_result.is_succeeded():
                logger.error(f"Path query failed for eotar_id {eotar_id}: {path_result.error_msg()}")
                continue
            
            paths_for_eotar: dict[str, dict] = {}
            
            for row_index in range(path_result.row_size()):
                row = path_result.row_values(row_index)
                
                if len(row) < 1:
                    continue
                
                path = row[0] if row[0] else None
                if path:
                    path_str = str(path)
                    
                    nodes = []
                    if "->" in path_str:
                        parts = path_str.split("->")
                        for part in parts:
                            if '"' in part:
                                node_id = part.split('"')[1] if '"' in part else ""
                                if node_id:
                                    nodes.append(node_id)
                    
                    path_key = tuple(nodes)
                    
                    if path_key not in paths_for_eotar:
                        edge_data_list = []
                        for i in range(len(nodes) - 1):
                            from_node = nodes[i]
                            to_node = nodes[i + 1]
                            edge_query = f'GO FROM "{from_node}" OVER VISION_INTERFACE_SYSTEM_LEVEL WHERE VISION_INTERFACE_SYSTEM_LEVEL.rsm_document_id == "{clean_eotar_id}" YIELD VISION_INTERFACE_SYSTEM_LEVEL.consumer_module_id, VISION_INTERFACE_SYSTEM_LEVEL.provider_module_id, VISION_INTERFACE_SYSTEM_LEVEL.consumer_component_id, VISION_INTERFACE_SYSTEM_LEVEL.provider_component_id, $$ AS target_vertex'
                            
                            logger.info(f"Executing edge query: {edge_query}")
                            edge_result = session.execute(edge_query)
                            
                            found = False
                            if edge_result.is_succeeded():
                                for row_idx in range(edge_result.row_size()):
                                    edge_row = edge_result.row_values(row_idx)
                                    target_vertex = str(edge_row[4]) if edge_row[4] else ""
                                    
                                    if to_node in target_vertex:
                                        edge_data_list.append({
                                            "consumer_module_id": str(edge_row[0]).strip('"') if edge_row[0] and str(edge_row[0]) not in ["None", "__EMPTY__", '"NULL"'] else "",
                                            "provider_module_id": str(edge_row[1]).strip('"') if edge_row[1] and str(edge_row[1]) not in ["None", "__EMPTY__", '"NULL"'] else "",
                                            "consumer_component_id": str(edge_row[2]).strip('"') if edge_row[2] and str(edge_row[2]) not in ["None", "__EMPTY__", '"NULL"'] else "",
                                            "provider_component_id": str(edge_row[3]).strip('"') if edge_row[3] and str(edge_row[3]) not in ["None", "__EMPTY__", '"NULL"'] else "",
                                        })
                                        found = True
                                        break
                            
                            if not found:
                                edge_data_list.append({
                                    "consumer_module_id": "",
                                    "provider_module_id": "",
                                    "consumer_component_id": "",
                                    "provider_component_id": "",
                                })
                        
                        paths_for_eotar[path_key] = {
                            "path": nodes,
                            "distance": len(nodes),
                            "edge_data": edge_data_list,
                        }
            
            if paths_for_eotar:
                out_data = outgoing_eotar_data.get(eotar_id, {})
                in_data = incoming_eotar_data.get(eotar_id, {})
                
                out_date = out_data.get("eotar_date")
                in_date = in_data.get("eotar_date")
                dates = [d for d in [out_date, in_date] if d]
                latest_date = max(dates) if dates else None
                
                consumer_module_id = out_data.get("consumer_module_id") or in_data.get("consumer_module_id")
                provider_module_id = out_data.get("provider_module_id") or in_data.get("provider_module_id")
                consumer_component_id = out_data.get("consumer_component_id") or in_data.get("consumer_component_id")
                provider_component_id = out_data.get("provider_component_id") or in_data.get("provider_component_id")
                
                results[eotar_id] = {
                    "paths": paths_for_eotar,
                    "document_rsm_date_time": latest_date,
                    "consumer_module_id": consumer_module_id or "",
                    "provider_module_id": provider_module_id or "",
                    "consumer_component_id": consumer_component_id or "",
                    "provider_component_id": provider_component_id or "",
                }
        
        logger.info(f"NebulaGraph search returned {len(results)} matching eotar groups with paths")
        return results
        
    except Exception as e:
        logger.error(f"NebulaGraph query error: {e}")
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
                query = f'FETCH PROP ON * "{sys_id}" YIELD vertex as v'
                result = session.execute(query)
                if result.is_succeeded() and result.row_size() > 0:
                    row = result.row_values(0)
                    vertex_str = str(row[0]) if row[0] else ""
                    
                    name = None
                    if "system_rsm_name:" in vertex_str:
                        import re
                        match = re.search(r'system_rsm_name:\s*"([^"]*)"', vertex_str)
                        if match:
                            name = match.group(1)
                    elif "name:" in vertex_str:
                        import re
                        match = re.search(r'name:\s*"([^"]*)"', vertex_str)
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
        
        import re
        
        # Get node properties
        row = check_result.row_values(0)
        vertex_str = str(row[0]) if row[0] else ""
        
        node_label = ""
        node_rsm_name = None
        
        # Extract label from vertex string (e.g., :SYSTEM, :MODULE, :COMPONENT)
        label_match = re.search(r':([A-Za-z]+)\{', vertex_str)
        if label_match:
            node_label = label_match.group(1)
        
        # Extract name property
        name_match = re.search(r'name:\s*"([^"]*)"', vertex_str)
        if name_match:
            node_rsm_name = name_match.group(1)
        
        # Extract all properties for other_info
        other_info = {}
        props_match = re.findall(r'([A-Za-z_]+):\s*"([^"]*)"', vertex_str)
        for prop_name, prop_value in props_match:
            other_info[prop_name] = prop_value
        
        # Get children via hierarchy edge (REVERSELY - nodes that point TO rsm_id via hierarchy)
        children_query = f'GO FROM "{rsm_id}" OVER HIERARCHY REVERSELY YIELD id($$) AS child_id, $$ AS child_vertex'
        children_result = session.execute(children_query)
        
        children = []
        if children_result.is_succeeded():
            for row_idx in range(children_result.row_size()):
                row = children_result.row_values(row_idx)
                child_id = str(row[0]).strip('"') if row[0] else None
                child_vertex = str(row[1]) if row[1] else ""
                
                if child_id:
                    # Extract label
                    child_label = ""
                    label_match = re.search(r':([A-Za-z]+)\{', child_vertex)
                    if label_match:
                        child_label = label_match.group(1)
                    
                    # Extract name
                    child_name = None
                    name_match = re.search(r'name:\s*"([^"]*)"', child_vertex)
                    if name_match:
                        child_name = name_match.group(1)
                    
                    # Extract all properties for other_info
                    child_other_info = {}
                    props_match = re.findall(r'([A-Za-z_]+):\s*"([^"]*)"', child_vertex)
                    for prop_name, prop_value in props_match:
                        child_other_info[prop_name] = prop_value
                    
                    children.append({
                        "node": {
                            "label": child_label,
                            "other_info": child_other_info,
                            "rsm_id": child_id,
                            "rsm_name": child_name,
                        },
                        "children": []
                    })
        
        logger.info(f"Found {len(children)} children for node {rsm_id}")
        
        return {
            "node": {
                "label": node_label,
                "other_info": other_info,
                "rsm_id": rsm_id,
                "rsm_name": node_rsm_name,
            },
            "children": children
        }
        
    except Exception as e:
        logger.error(f"NebulaGraph query error in fetch_child_tree_from_nebula: {e}")
        return None
    finally:
        session.release()

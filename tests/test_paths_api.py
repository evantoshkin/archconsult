import pytest
from httpx import AsyncClient
import asyncpg

from app.main import app
from app.db.queries import PathResult


@pytest.fixture
def mock_execute_path_search(mocker):
    return mocker.patch("app.api.v2.paths.execute_path_search")


@pytest.mark.asyncio
async def test_get_path_success(mock_execute_path_search):
    mock_execute_path_search.return_value = PathResult(
        from_system_id="sys1",
        from_system_name="System One",
        to_system_id="sys2",
        to_system_name="System Two",
        path_length=3,
        path='[{"id": 1, "label": "SYSTEM"}]',
        frequency=5,
        example_eotar_rsm_id="eotar123",
    )
    
    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.get("/api/v2/paths?from_system_id=sys1&to_system_id=sys2")
    
    assert response.status_code == 200
    data = response.json()
    assert data["from_system_id"] == "sys1"
    assert data["to_system_id"] == "sys2"
    assert data["path_length"] == 3
    assert data["frequency"] == 5


@pytest.mark.asyncio
async def test_get_path_not_found(mock_execute_path_search):
    mock_execute_path_search.return_value = None
    
    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.get("/api/v2/paths?from_system_id=sys1&to_system_id=sys2")
    
    assert response.status_code == 404
    data = response.json()
    assert data["detail"]["code"] == "PATH_NOT_FOUND"
    assert data["detail"]["from_system_id"] == "sys1"
    assert data["detail"]["to_system_id"] == "sys2"


@pytest.mark.asyncio
async def test_get_path_missing_params():
    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.get("/api/v2/paths")
    
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_get_path_invalid_id():
    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.get("/api/v2/paths?from_system_id=invalid!@#&to_system_id=sys2")
    
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_get_path_empty_params():
    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.get("/api/v2/paths?from_system_id=&to_system_id=sys2")
    
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_get_path_db_unavailable(mock_execute_path_search):
    mock_execute_path_search.side_effect = asyncpg.PostgresConnectionError()
    
    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.get("/api/v2/paths?from_system_id=sys1&to_system_id=sys2")
    
    assert response.status_code == 503
    data = response.json()
    assert data["detail"]["code"] == "DATABASE_UNAVAILABLE"


@pytest.mark.asyncio
async def test_get_path_db_timeout(mock_execute_path_search):
    mock_execute_path_search.side_effect = asyncpg.QueryCanceledError()
    
    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.get("/api/v2/paths?from_system_id=sys1&to_system_id=sys2")
    
    assert response.status_code == 504
    data = response.json()
    assert data["detail"]["code"] == "DATABASE_TIMEOUT"


@pytest.mark.asyncio
async def test_get_path_db_internal_error(mock_execute_path_search):
    mock_execute_path_search.side_effect = Exception("Unexpected error")
    
    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.get("/api/v2/paths?from_system_id=sys1&to_system_id=sys2")
    
    assert response.status_code == 500
    data = response.json()
    assert data["detail"]["code"] == "INTERNAL_SERVER_ERROR"

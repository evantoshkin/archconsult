import pytest
from httpx import AsyncClient

from app.main import app


@pytest.fixture
def mock_db_pool(mocker):
    mock_pool = mocker.patch("app.db.pool.pool", None)
    mock_create_pool = mocker.patch("app.db.pool.create_pool")
    mock_create_pool.return_value = mocker.AsyncMock()
    return mock_create_pool


@pytest.mark.asyncio
async def test_health_check():
    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.get("/health")
    
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"


@pytest.mark.asyncio
async def test_ready_check_db_available(mocker):
    mock_check = mocker.patch("app.api.v1.health.check_database_ready")
    mock_check.return_value = True
    
    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.get("/ready")
    
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ready"
    assert data["database"] == "ok"


@pytest.mark.asyncio
async def test_ready_check_db_unavailable(mocker):
    mock_check = mocker.patch("app.api.v1.health.check_database_ready")
    mock_check.return_value = False
    
    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.get("/ready")
    
    assert response.status_code == 503

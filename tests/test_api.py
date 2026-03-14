"""Tests for FastAPI endpoints."""


def test_health_returns_ok(client):
    """Test health endpoint."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"


def test_root_has_version_and_count(client):
    """Test root endpoint returns version and count."""
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert "version" in data
    assert "record_count" in data


def test_list_returns_data_and_meta(client):
    """Test list endpoint returns data and meta."""
    response = client.get("/datasets")
    assert response.status_code == 200
    data = response.json()
    assert "data" in data
    assert "meta" in data
    assert len(data["data"]) > 0


def test_list_pagination(client):
    """Test pagination works."""
    response = client.get("/datasets?page=1&limit=2")
    assert response.status_code == 200
    data = response.json()
    assert data["meta"]["page"] == 1
    assert data["meta"]["limit"] == 2


def test_list_filter_by_category(client):
    """Test filtering by topic."""
    response = client.get("/datasets?topic=health")
    assert response.status_code == 200
    data = response.json()
    assert all(r["topic"] == "health" for r in data["data"])


def test_list_sort_works(client):
    """Test sorting works."""
    response = client.get("/datasets?sort=quality_score&order=desc")
    assert response.status_code == 200
    data = response.json()
    assert data["data"][0]["quality_score"] >= data["data"][-1]["quality_score"]


def test_list_limit_over_max_is_422(client):
    """Test limit over 100 returns 422."""
    response = client.get("/datasets?limit=101")
    assert response.status_code == 422


def test_get_known_id(client):
    """Test getting known record."""
    response = client.get("/datasets/1")
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == "1"


def test_get_unknown_id_is_404(client):
    """Test getting unknown record returns 404."""
    response = client.get("/datasets/unknown-id")
    assert response.status_code == 404


def test_search_returns_results(client):
    """Test search returns results."""
    response = client.get("/datasets/search?q=health")
    assert response.status_code == 200
    data = response.json()
    assert "data" in data


def test_search_no_query_is_422(client):
    """Test search without query returns 422."""
    response = client.get("/datasets/search")
    assert response.status_code == 422


def test_schema_has_fields(client):
    """Test schema endpoint returns fields."""
    response = client.get("/meta/schema")
    assert response.status_code == 200
    data = response.json()
    assert "fields" in data
    assert len(data["fields"]) > 0


def test_sources_is_list(client):
    """Test sources endpoint returns list."""
    response = client.get("/meta/sources")
    assert response.status_code == 200


def test_list_generated_at_present(client):
    """Test list response includes generated_at."""
    response = client.get("/datasets")
    assert response.status_code == 200
    data = response.json()
    assert "generated_at" in data["meta"]

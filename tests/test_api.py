
from fastapi.testclient import TestClient
from pydantic_settings import BaseSettings

from main import app, get_settings
from tsapi.model.dataset import OperationSet


class Settings(BaseSettings):
    mdb_host: str = "localhost"
    mdb_port: int = 27017
    mdb_name: str = "test_tsapidb"
    mdb_url: str = "mongodb://localhost:27017/test_tsapidb"


def override_get_settings():
    return Settings()


def test_read_main(config):
    with TestClient(app) as client:
        app.dependency_overrides[get_settings] = override_get_settings
        response = client.get("/")
        assert response.status_code == 200
        assert response.json() == {"message": "This is the Time Series API"}


def test_read_datasets():
    with TestClient(app) as client:
        app.dependency_overrides[get_settings] = override_get_settings
        response = client.get("/tsapi/v1/datasets")
        assert response.status_code == 200
        assert response.json() == []


def test_insert_opset():
    with TestClient(app) as client:
        app.dependency_overrides[get_settings] = override_get_settings
        opset = OperationSet(id="0", dataset_id="2", plot=["a", "b"])
        response = client.post("/tsapi/v1/opsets", json=opset.model_dump())
        assert response.status_code == 200
        assert response.json()['dataset_id'] == opset.dataset_id


import os
import pytest
from unittest import mock
from airflow.models import Variable, Connection, DagBag

#unit_test for API key
@pytest.fixture()
def api_key():
    with mock.patch.dict("os.environ", AIRFLOW_VAR_API_KEY = "MOCK_KEY1234"):
        yield Variable.get("API_KEY")

#unit_test for channel_handle
@pytest.fixture()
def channel_handle():
    with mock.patch.dict("os.environ", AIRFLOW_VAR_CHANNEL_HANDLE = "MRCHEESE"):
        yield Variable.get("CHANNEL_HANDLE")

#unit_test for postgres_connection variables
@pytest.fixture()
def mock_postgres_conn_vars():
    conn = Connection(
            login = "mock_username",
            password = "mock_password",
            host = "mock_host",
            port = 1234,
            schema = "mock_db_name",
    )
    conn_uri = conn.get_uri()

    with mock.patch.dict("os.environ", AIRFLOW_CONN_POSTGRES_DB_YT_ELT = conn_uri):
        yield Connection.get_connection_from_secrets(conn_id = "POSTGRES_DB_YT_ELT")

#unit_test for DAG 
@pytest.fixture()
def dagbag():
    yield DagBag()

#integration test for API requests connection to python
@pytest.fixture()
def airflow_variable():
    def get_airflow_variable(variable_name):
        env_var = f"AIRFLOW_VAR_{variable_name.upper()}"
        return os.getenv(env_var)
    return get_airflow_variable
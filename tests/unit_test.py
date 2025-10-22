#we use fake credentials for unit_test

def test_api_key(api_key):
    assert api_key == "MOCK_KEY1234"

def test_channel_handle(channel_handle):
    assert channel_handle == "MRCHEESE"

def test_postgres_conn(mock_postgres_conn_vars):
    conn = mock_postgres_conn_vars
    assert conn.login == "mock_username"
    assert conn.password == "mock_password"
    assert conn.host == "mock_host"
    assert conn.port == 1234
    assert conn.schema == "mock_db_name"

def test_dags_integrity(dagbag):
    #check dag import error
    assert dagbag.import_errors == {}, f"import error found: : {dagbag.import_errors}"
    print("=============")
    print(dagbag.import_errors)

    #check all expected dags item are loaded
    expected_dag_id = ["produce_json", "update_db", "data_quality"]
    loaded_dag_ids = list(dagbag.dags.keys())#the dags_id from main.py inside dags
    print("=============")
    print(dagbag.dags.keys())

    for dag_id in expected_dag_id:
        assert dag_id in loaded_dag_ids, f"DAG {dag_id} is missing"

    #check number of dags
    assert dagbag.size() == 3
    print("=============")
    print(dagbag.size())

    #check if number of dags have expected number of tasks or not

    expected_dags_counts = {
        "produce_json": 4,
        "update_db": 2,
        "data_quality": 2,
    }

    print("=============") 

    for dag_id, dag in dagbag.dags.items():
        expected_count = expected_dags_counts[dag_id]
        actual_count = len(dag.tasks)
        assert(
            expected_count == actual_count
        ), f"DAG {dag_id} has {actual_count}, expected {expected_count}"
    print(dag_id, len(dag.tasks))
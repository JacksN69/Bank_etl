import unittest
from contextlib import contextmanager
import importlib.util
from pathlib import Path
import sys
from unittest.mock import patch


etl_path = Path(__file__).parent.parent
sys.path.insert(0, str(etl_path))


class _FakeResult:
    def __init__(self, value):
        self._value = value

    def scalar(self):
        return self._value


class _FakeSession:
    def __init__(self, database_name):
        self.database_name = database_name

    def execute(self, _query):
        return _FakeResult(self.database_name)


@contextmanager
def _fake_session_scope(database_name):
    yield _FakeSession(database_name)


class _FakeTaskInstance:
    def __init__(self):
        self.pushed = {}

    def xcom_push(self, key, value):
        self.pushed[key] = value


@unittest.skipUnless(importlib.util.find_spec("airflow") is not None, "airflow is not installed")
class TestValidateEnvironmentIntegration(unittest.TestCase):
    def setUp(self):
        from dags import banking_etl_pipeline_dag as dag_module
        self.dag_module = dag_module

    def test_validate_environment_bootstraps_and_checks_schema(self):
        ti = _FakeTaskInstance()
        context = {'task_instance': ti}

        with patch.object(self.dag_module.Config, 'POSTGRES_DB', 'banking_warehouse'):
            with patch.object(self.dag_module.DatabaseConnection, 'test_connection', return_value=True):
                with patch.object(
                    self.dag_module.DatabaseConnection,
                    'session_scope',
                    side_effect=lambda: _fake_session_scope('banking_warehouse')
                ) as mocked_scope:
                    with patch.object(self.dag_module.DatabaseConnection, 'initialize_warehouse_schema') as mocked_init:
                        with patch.object(
                            self.dag_module.DatabaseConnection,
                            'schema_health_check',
                            return_value=True
                        ) as mocked_health:
                            self.dag_module.validate_environment(**context)

        self.assertTrue(ti.pushed.get('environment_valid'))
        mocked_scope.assert_called_once()
        mocked_init.assert_called_once()
        mocked_health.assert_called_once()

    def test_validate_environment_fails_when_schema_health_check_fails(self):
        ti = _FakeTaskInstance()
        context = {'task_instance': ti}

        with patch.object(self.dag_module.Config, 'POSTGRES_DB', 'banking_warehouse'):
            with patch.object(self.dag_module.DatabaseConnection, 'test_connection', return_value=True):
                with patch.object(
                    self.dag_module.DatabaseConnection,
                    'session_scope',
                    side_effect=lambda: _fake_session_scope('banking_warehouse')
                ):
                    with patch.object(self.dag_module.DatabaseConnection, 'initialize_warehouse_schema'):
                        with patch.object(
                            self.dag_module.DatabaseConnection,
                            'schema_health_check',
                            return_value=False
                        ):
                            with self.assertRaises(RuntimeError):
                                self.dag_module.validate_environment(**context)

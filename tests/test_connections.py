import json
import os
import sys
import tempfile
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from bson import ObjectId

ROOT = Path(__file__).resolve().parents[1]
ETL_PATH = ROOT / "etl"
if str(ETL_PATH) not in sys.path:
    sys.path.insert(0, str(ETL_PATH))

import connections  # noqa: E402
from connections._config import load_connection_config  # noqa: E402
from connections._engine_cache import dispose_all_engines, get_or_create_engine  # noqa: E402
from connections._session_cache import close_all_sessions, get_or_create_session  # noqa: E402
from connections.dw_destination import get_dw_engine, test_dw_connection as dw_connection_healthcheck  # noqa: E402
from connections.sources.data_contract import IngestionResult  # noqa: E402
from connections.sources.factory import create_connector, load_connector_config  # noqa: E402
from connections.sources.ftp.connector import FTPConnector  # noqa: E402
from connections.sources.ftp.webdav_connector import WebDAVConnector  # noqa: E402
from connections.sources.http.connector import HTTPConnector  # noqa: E402
from connections.sources.http.soap_connector import SOAPConnector  # noqa: E402
from connections.sources.http import rest  # noqa: E402
from connections.sources.saas.gsheets.connector import GSheetsConnector  # noqa: E402
from connections.sources.nosql.cassandra import get_cassandra_session, test_cassandra_connection as cassandra_connection_healthcheck  # noqa: E402
from connections.sources.nosql.mongodb.connector import MongoDBConnector  # noqa: E402
from connections.sources.nosql.neo4j import get_neo4j_driver, test_neo4j_connection as neo4j_connection_healthcheck  # noqa: E402
from connections.sources.streams.amqp import AMQPConnector  # noqa: E402
from connections.sources.streams.kafka import KafkaConnector  # noqa: E402
from connections.sources.streams.nats import NATSConnector  # noqa: E402
from connections.sources.ssh.connector import SSHConnector  # noqa: E402
from connections.sources.sql.incremental import fetch_incremental_rows  # noqa: E402
from connections.sources.sql.mssql import get_mssql_engine, test_mssql_connection as mssql_connection_healthcheck  # noqa: E402
from connections.sources.sql.oracle import get_oracle_engine, test_oracle_connection as oracle_connection_healthcheck  # noqa: E402
from connections.sources.sql.postgres import get_postgres_engine, test_postgres_connection as postgres_connection_healthcheck  # noqa: E402
from connections.sources.sql.sqlite import get_sqlite_engine, test_sqlite_connection as sqlite_connection_healthcheck  # noqa: E402


class FakeEngine:
    def __init__(self):
        self.disposed = 0

    def dispose(self):
        self.disposed += 1


class FakeSession:
    def __init__(self):
        self.closed = 0

    def close(self):
        self.closed += 1


class ConfigTests(unittest.TestCase):
    def test_load_connection_config_merges_layers(self):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as tmp:
            json.dump({"host": "file-host", "database": "file-db"}, tmp)
            file_path = tmp.name

        try:
            with patch.dict(os.environ, {"DWTEST_USERNAME": "env-user"}, clear=False):
                result = load_connection_config(
                    config={"database": "config-db"},
                    file_path=file_path,
                    env_prefix="DWTEST",
                    required=("host", "database", "username", "password"),
                    defaults={"port": 5432},
                    overrides={"password": "override-pw"},
                )
        finally:
            os.unlink(file_path)

        self.assertEqual(result["host"], "file-host")
        self.assertEqual(result["database"], "config-db")
        self.assertEqual(result["username"], "env-user")
        self.assertEqual(result["password"], "override-pw")
        self.assertEqual(result["port"], 5432)

    def test_load_connection_config_missing_required_raises(self):
        with self.assertRaises(ValueError):
            load_connection_config(required=("host",), defaults={"port": 5432})


class EngineCacheTests(unittest.TestCase):
    def tearDown(self):
        dispose_all_engines()

    def test_get_or_create_engine_reuses_when_enabled(self):
        call_count = {"n": 0}

        def factory():
            call_count["n"] += 1
            return FakeEngine()

        config = {"host": "localhost", "db": "dw"}
        first = get_or_create_engine("dw", config, factory, reuse=True)
        second = get_or_create_engine("dw", config, factory, reuse=True)

        self.assertIs(first, second)
        self.assertEqual(call_count["n"], 1)

    def test_get_or_create_engine_does_not_reuse_when_disabled(self):
        first = get_or_create_engine("dw", {"host": "a"}, FakeEngine, reuse=False)
        second = get_or_create_engine("dw", {"host": "a"}, FakeEngine, reuse=False)

        self.assertIsNot(first, second)

    def test_dispose_all_engines_disposes_cached(self):
        created = []

        def factory():
            engine = FakeEngine()
            created.append(engine)
            return engine

        get_or_create_engine("dw", {"host": "a"}, factory, reuse=True)
        get_or_create_engine("oracle", {"host": "b"}, factory, reuse=True)

        dispose_all_engines()

        self.assertEqual(created[0].disposed, 1)
        self.assertEqual(created[1].disposed, 1)


class SessionCacheTests(unittest.TestCase):
    def tearDown(self):
        close_all_sessions()

    def test_get_or_create_session_reuses_when_enabled(self):
        call_count = {"n": 0}

        def factory():
            call_count["n"] += 1
            return FakeSession()

        config = {"base_url": "http://api.local"}
        first = get_or_create_session("rest", config, factory, reuse=True)
        second = get_or_create_session("rest", config, factory, reuse=True)

        self.assertIs(first, second)
        self.assertEqual(call_count["n"], 1)

    def test_get_or_create_session_does_not_reuse_when_disabled(self):
        first = get_or_create_session("rest", {"base_url": "x"}, FakeSession, reuse=False)
        second = get_or_create_session("rest", {"base_url": "x"}, FakeSession, reuse=False)

        self.assertIsNot(first, second)

    def test_close_all_sessions_closes_cached(self):
        created = []

        def factory():
            session = FakeSession()
            created.append(session)
            return session

        get_or_create_session("rest", {"base_url": "http://a"}, factory, reuse=True)
        get_or_create_session("rest", {"base_url": "http://b"}, factory, reuse=True)

        close_all_sessions()

        self.assertEqual(created[0].closed, 1)
        self.assertEqual(created[1].closed, 1)


class ConnectionBuildersAndHealthTests(unittest.TestCase):
    def test_get_dw_engine_builds_expected_url(self):
        with patch("connections.dw_destination.get_or_create_engine", side_effect=lambda _t, _c, factory, reuse: factory()):
            with patch("connections.dw_destination.create_engine") as mock_create_engine:
                get_dw_engine(
                    host="localhost",
                    database="data warehouse",
                    username="user",
                    password="p@ss",
                )

        args, kwargs = mock_create_engine.call_args
        self.assertEqual(args[0], "postgresql+psycopg://user:p%40ss@localhost:5432/data+warehouse")
        self.assertTrue(kwargs["pool_pre_ping"])

    def test_get_mssql_engine_builds_expected_url(self):
        with patch(
            "connections.sources.sql.mssql.get_or_create_engine",
            side_effect=lambda _t, _c, factory, reuse: factory(),
        ):
            with patch("connections.sources.sql.mssql.create_engine") as mock_create_engine:
                get_mssql_engine(host="sqlhost", database="db", username="sa", password="pw")

        args, _kwargs = mock_create_engine.call_args
        self.assertIn("mssql+pyodbc://sa:pw@sqlhost:1433/db?driver=ODBC+Driver+18+for+SQL+Server", args[0])

    def test_get_postgres_engine_builds_expected_url(self):
        with patch(
            "connections.sources.sql.postgres.get_or_create_engine",
            side_effect=lambda _t, _c, factory, reuse: factory(),
        ):
            with patch("connections.sources.sql.postgres.create_engine") as mock_create_engine:
                get_postgres_engine(host="pghost", database="db", username="pg", password="pw")

        args, _kwargs = mock_create_engine.call_args
        self.assertEqual(args[0], "postgresql+psycopg://pg:pw@pghost:5432/db")

    def test_get_sqlite_engine_builds_expected_url(self):
        with patch(
            "connections.sources.sql.sqlite.get_or_create_engine",
            side_effect=lambda _t, _c, factory, reuse: factory(),
        ):
            with patch("connections.sources.sql.sqlite.create_engine") as mock_create_engine:
                get_sqlite_engine(database_path=":memory:")

        args, _kwargs = mock_create_engine.call_args
        self.assertEqual(args[0], "sqlite+pysqlite:///:memory:")

    def test_get_oracle_engine_builds_expected_url(self):
        with patch(
            "connections.sources.sql.oracle.get_or_create_engine",
            side_effect=lambda _t, _c, factory, reuse: factory(),
        ):
            with patch("connections.sources.sql.oracle.create_engine") as mock_create_engine:
                get_oracle_engine(host="orcl", service_name="xe", username="scott", password="tiger")

        args, _kwargs = mock_create_engine.call_args
        self.assertEqual(args[0], "oracle+oracledb://scott:tiger@orcl:1521/?service_name=xe")

    def test_test_dw_connection_handles_success_and_failure(self):
        good_engine = MagicMock()
        connection = MagicMock()
        good_engine.connect.return_value.__enter__.return_value = connection

        with patch("connections.dw_destination.get_dw_engine", return_value=good_engine):
            self.assertTrue(dw_connection_healthcheck())

        with patch("connections.dw_destination.get_dw_engine", side_effect=RuntimeError("boom")):
            self.assertFalse(dw_connection_healthcheck())
            with self.assertRaises(RuntimeError):
                dw_connection_healthcheck(raise_on_error=True)

    def test_test_mssql_connection_handles_success_and_failure(self):
        good_engine = MagicMock()
        good_engine.connect.return_value.__enter__.return_value = MagicMock()

        with patch("connections.sources.sql.mssql.get_mssql_engine", return_value=good_engine):
            self.assertTrue(mssql_connection_healthcheck())

        with patch("connections.sources.sql.mssql.get_mssql_engine", side_effect=RuntimeError("boom")):
            self.assertFalse(mssql_connection_healthcheck())

    def test_test_oracle_connection_handles_success_and_failure(self):
        good_engine = MagicMock()
        good_engine.connect.return_value.__enter__.return_value = MagicMock()

        with patch("connections.sources.sql.oracle.get_oracle_engine", return_value=good_engine):
            self.assertTrue(oracle_connection_healthcheck())

        with patch("connections.sources.sql.oracle.get_oracle_engine", side_effect=RuntimeError("boom")):
            self.assertFalse(oracle_connection_healthcheck())

    def test_test_postgres_connection_handles_success_and_failure(self):
        good_engine = MagicMock()
        good_engine.connect.return_value.__enter__.return_value = MagicMock()

        with patch("connections.sources.sql.postgres.get_postgres_engine", return_value=good_engine):
            self.assertTrue(postgres_connection_healthcheck())

        with patch("connections.sources.sql.postgres.get_postgres_engine", side_effect=RuntimeError("boom")):
            self.assertFalse(postgres_connection_healthcheck())

    def test_test_sqlite_connection_handles_success_and_failure(self):
        good_engine = MagicMock()
        good_engine.connect.return_value.__enter__.return_value = MagicMock()

        with patch("connections.sources.sql.sqlite.get_sqlite_engine", return_value=good_engine):
            self.assertTrue(sqlite_connection_healthcheck())

        with patch("connections.sources.sql.sqlite.get_sqlite_engine", side_effect=RuntimeError("boom")):
            self.assertFalse(sqlite_connection_healthcheck())


class SQLIncrementalTests(unittest.TestCase):
    def test_fetch_incremental_rows_by_watermark(self):
        engine = get_sqlite_engine(database_path=":memory:", reuse=False)
        with engine.begin() as connection:
            connection.exec_driver_sql("CREATE TABLE events (id INTEGER PRIMARY KEY, payload TEXT)")
            connection.exec_driver_sql("INSERT INTO events (id, payload) VALUES (1, 'a')")
            connection.exec_driver_sql("INSERT INTO events (id, payload) VALUES (2, 'b')")
            connection.exec_driver_sql("INSERT INTO events (id, payload) VALUES (3, 'c')")

        rows, watermark = fetch_incremental_rows(
            engine=engine,
            table_name="events",
            watermark_column="id",
            last_watermark=1,
            batch_size=2,
        )

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["id"], 2)
        self.assertEqual(rows[1]["id"], 3)
        self.assertEqual(watermark, 3)


class NoSQLConnectionTests(unittest.TestCase):
    def test_get_cassandra_session_uses_native_driver(self):
        with patch("connections.sources.nosql.cassandra.Cluster") as mock_cluster_ctor:
            cluster = MagicMock()
            session = MagicMock()
            cluster.connect.return_value = session
            mock_cluster_ctor.return_value = cluster

            returned = get_cassandra_session(hosts=["cass-1"], keyspace="analytics", reuse=False)

        self.assertIs(returned, session)
        mock_cluster_ctor.assert_called_once()
        cluster.connect.assert_called_once_with("analytics")

    def test_test_cassandra_connection_handles_success_and_failure(self):
        session = MagicMock()
        with patch("connections.sources.nosql.cassandra.get_cassandra_session", return_value=session):
            self.assertTrue(cassandra_connection_healthcheck())

        with patch("connections.sources.nosql.cassandra.get_cassandra_session", side_effect=RuntimeError("boom")):
            self.assertFalse(cassandra_connection_healthcheck())

    def test_get_neo4j_driver_uses_bolt_driver(self):
        with patch("connections.sources.nosql.neo4j.GraphDatabase.driver") as mock_driver_ctor:
            mock_driver = MagicMock()
            mock_driver_ctor.return_value = mock_driver

            returned = get_neo4j_driver(
                uri="bolt://neo4j.local:7687",
                username="neo",
                password="pw",
                reuse=False,
            )

        self.assertIs(returned, mock_driver)
        mock_driver_ctor.assert_called_once_with("bolt://neo4j.local:7687", auth=("neo", "pw"))

    def test_test_neo4j_connection_handles_success_and_failure(self):
        driver = MagicMock()
        session_ctx = driver.session.return_value
        session = session_ctx.__enter__.return_value
        result = MagicMock()
        result.single.return_value = {"ok": 1}
        session.run.return_value = result

        with patch("connections.sources.nosql.neo4j.get_neo4j_driver", return_value=driver):
            self.assertTrue(neo4j_connection_healthcheck())

        with patch("connections.sources.nosql.neo4j.get_neo4j_driver", side_effect=RuntimeError("boom")):
            self.assertFalse(neo4j_connection_healthcheck())


class MongoDBConnectorTests(unittest.TestCase):
    def test_connect_initializes_mongo_client(self):
        with patch("connections.sources.nosql.mongodb.connector.MongoClient") as mock_client_ctor:
            mock_client = MagicMock()
            mock_database = MagicMock()
            mock_client.__getitem__.return_value = mock_database
            mock_client_ctor.return_value = mock_client

            connector = MongoDBConnector(
                host="mongo.local",
                port=27017,
                username="user",
                password="pass",
                database="analytics",
                auth_source="admin",
            )
            connector.connect()

        mock_client_ctor.assert_called_once_with(
            host="mongo.local",
            port=27017,
            username="user",
            password="pass",
            authSource="admin",
        )
        mock_client.__getitem__.assert_called_once_with("analytics")

    def test_fetch_data_serializes_objectid_payload(self):
        connector = MongoDBConnector(host="mongo.local", database="analytics")

        collection = MagicMock()
        object_id = ObjectId("507f1f77bcf86cd799439011")
        collection.find.return_value = [
            {"_id": object_id, "name": "alice", "nested": {"owner_id": object_id}}
        ]

        database = MagicMock()
        database.__getitem__.return_value = collection
        connector._database = database

        result = connector.fetch_data("users")

        self.assertTrue(result.success)
        self.assertEqual(result.protocol, "mongodb")
        self.assertEqual(result.metadata["collection"], "users")
        self.assertEqual(result.metadata["fetched_documents"], 1)
        self.assertEqual(result.items[0].payload["_id"], str(object_id))
        self.assertEqual(result.items[0].payload["nested"]["owner_id"], str(object_id))

    def test_factory_creates_mongodb_connector(self):
        connector = create_connector(
            {
                "protocol": "mongodb",
                "host": "mongo.local",
                "database": "analytics",
            }
        )

        self.assertIsInstance(connector, MongoDBConnector)


class RestConnectionTests(unittest.TestCase):
    def tearDown(self):
        close_all_sessions()

    def test_get_rest_session_sets_headers_base_url_and_timeout(self):
        captured = {}

        def fake_cache(_t, cache_config, factory, reuse):
            captured["cache_config"] = cache_config
            return factory()

        with patch.object(rest, "get_or_create_session", side_effect=fake_cache):
            session = rest.get_rest_session(
                base_url="https://example.com",
                token="abc123",
                headers={"X-Test": "1"},
                timeout_seconds=15,
                reuse=True,
            )

        self.assertEqual(session.base_url, "https://example.com/")
        self.assertEqual(session.timeout_seconds, 15)
        self.assertEqual(session.headers["Content-Type"], "application/json")
        self.assertEqual(session.headers["Authorization"], "Bearer abc123")
        self.assertEqual(session.headers["X-Test"], "1")
        self.assertEqual(captured["cache_config"]["base_url"], "https://example.com/")

    def test_request_rest_sends_expected_request(self):
        response = MagicMock()
        session = SimpleNamespace(
            base_url="https://api.local/",
            timeout_seconds=20,
            headers={"A": "B"},
            request=MagicMock(return_value=response),
        )

        with patch.object(rest, "get_rest_session", return_value=session):
            returned = rest.request_rest("get", "/health", params={"x": 1}, headers={"C": "D"})

        self.assertIs(returned, response)
        session.request.assert_called_once_with(
            method="GET",
            url="https://api.local/health",
            params={"x": 1},
            json=None,
            data=None,
            headers={"A": "B", "C": "D"},
            timeout=20,
        )
        response.raise_for_status.assert_called_once()

    def test_test_rest_connection_status_check(self):
        ok_response = SimpleNamespace(status_code=200)

        with patch.object(rest, "request_rest", return_value=ok_response):
            self.assertTrue(rest.test_rest_connection())

        not_ok_response = SimpleNamespace(status_code=500)
        with patch.object(rest, "request_rest", return_value=not_ok_response):
            self.assertFalse(rest.test_rest_connection())

        with patch.object(rest, "request_rest", side_effect=RuntimeError("fail")):
            self.assertFalse(rest.test_rest_connection())
            with self.assertRaises(RuntimeError):
                rest.test_rest_connection(raise_on_error=True)


class ProtocolConnectorTests(unittest.TestCase):
    def test_http_connector_requests_returns_ingestion_result(self):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '{"ok": true}'
        mock_response.headers = {"Content-Type": "application/json"}

        with patch("connections.sources.http.connector.requests.Session") as mock_session_ctor:
            session = MagicMock()
            session.get.return_value = mock_response
            mock_session_ctor.return_value = session

            connector = HTTPConnector(base_url="https://api.local", token="tkn", client_library="requests")
            connector.connect()
            result = connector.fetch_data("/health")
            connector.close()

        self.assertIsInstance(result, IngestionResult)
        self.assertTrue(result.success)
        self.assertEqual(result.protocol, "http")
        self.assertEqual(result.items[0].payload, {"ok": True})

    def test_ftp_connector_downloads_to_lake_and_returns_contract(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch("connections.sources.ftp.connector.FTP") as mock_ftp_ctor:
                ftp_client = MagicMock()
                ftp_client.nlst.return_value = ["/remote/a.txt", "/remote/b.txt"]

                def fake_download(_command, callback):
                    callback(b"data")

                ftp_client.retrbinary.side_effect = fake_download
                mock_ftp_ctor.return_value = ftp_client

                connector = FTPConnector(
                    host="ftp.local",
                    username="user",
                    password="pw",
                    lake_path=temp_dir,
                    remote_base_path="/remote",
                )
                connector.connect()
                result = connector.fetch_data("")
                connector.close()

                self.assertIsInstance(result, IngestionResult)
                self.assertTrue(result.success)
                self.assertEqual(result.protocol, "ftp")
                self.assertEqual(len(result.items), 2)
                self.assertTrue(Path(result.items[0].lake_path).exists())

    def test_ssh_connector_downloads_to_lake_and_returns_contract(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch("connections.sources.ssh.connector.paramiko.SSHClient") as mock_ssh_client_ctor:
                ssh_client = MagicMock()
                sftp_client = MagicMock()

                remote_file = SimpleNamespace(filename="log.txt", st_size=123)
                sftp_client.listdir_attr.return_value = [remote_file]

                def fake_get(_remote, local):
                    Path(local).write_bytes(b"ssh-data")

                sftp_client.get.side_effect = fake_get
                ssh_client.open_sftp.return_value = sftp_client
                mock_ssh_client_ctor.return_value = ssh_client

                connector = SSHConnector(
                    host="ssh.local",
                    username="user",
                    password="pw",
                    lake_path=temp_dir,
                    remote_base_path="/inbox",
                )
                connector.connect()
                result = connector.fetch_data("")
                connector.close()

                self.assertIsInstance(result, IngestionResult)
                self.assertTrue(result.success)
                self.assertEqual(result.protocol, "ssh")
                self.assertEqual(len(result.items), 1)
                self.assertEqual(result.items[0].size_bytes, 123)
                self.assertTrue(Path(result.items[0].lake_path).exists())

    def test_webdav_connector_connect_validates_connection(self):
        with patch("connections.sources.ftp.webdav_connector.Client") as mock_client_ctor:
            client = MagicMock()
            client.check.return_value = True
            mock_client_ctor.return_value = client

            connector = WebDAVConnector(
                base_url="https://webdav.local/",
                username="user",
                password="pw",
            )
            connector.connect()

        client.check.assert_called_once_with("/")

    def test_webdav_connector_fetch_data_downloads_file_and_returns_payload(self):
        with patch("connections.sources.ftp.webdav_connector.Client") as mock_client_ctor:
            client = MagicMock()
            client.check.return_value = True
            client.is_dir.return_value = False
            client.info.return_value = {"modified": "Sat, 21 Feb 2026 09:00:00 GMT", "etag": "abc123"}

            def fake_download_sync(remote_path, local_path):
                Path(local_path).write_text("hello-webdav", encoding="utf-8")

            client.download_sync.side_effect = fake_download_sync
            mock_client_ctor.return_value = client

            connector = WebDAVConnector(
                base_url="https://webdav.local/",
                username="user",
                password="pw",
            )
            connector.connect()
            result = connector.fetch_data("/reports/daily.txt")
            connector.close()

        self.assertIsInstance(result, IngestionResult)
        self.assertTrue(result.success)
        self.assertEqual(result.protocol, "webdav")
        self.assertEqual(result.items[0].source_path, "/reports/daily.txt")
        self.assertEqual(result.items[0].payload, "hello-webdav")
        self.assertEqual(result.metadata["item_type"], "file")
        self.assertEqual(result.metadata["content_encoding"], "utf-8")

    def test_webdav_connector_fetch_data_lists_directory(self):
        with patch("connections.sources.ftp.webdav_connector.Client") as mock_client_ctor:
            client = MagicMock()
            client.check.return_value = True
            client.is_dir.return_value = True
            client.list.return_value = [
                {"path": "/reports/daily.txt", "size": 12},
                {"path": "/reports/monthly.csv", "size": 24},
            ]
            mock_client_ctor.return_value = client

            connector = WebDAVConnector(
                base_url="https://webdav.local/",
                username="user",
                password="pw",
            )
            connector.connect()
            result = connector.fetch_data("/reports")
            connector.close()

        self.assertTrue(result.success)
        self.assertEqual(result.protocol, "webdav")
        self.assertEqual(result.metadata["item_type"], "directory")
        self.assertEqual(result.metadata["listed_entries"], 2)
        self.assertEqual(result.items[0].source_path, "/reports")
        self.assertEqual(len(result.items[0].payload["entries"]), 2)

    def test_gsheets_connector_reads_worksheet_records_and_returns_contract(self):
        records = [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
        worksheet = MagicMock()
        worksheet.get_all_records.return_value = records

        spreadsheet = MagicMock()
        spreadsheet.worksheet.return_value = worksheet

        client = MagicMock()
        client.open_by_key.return_value = spreadsheet

        fake_gspread_module = SimpleNamespace(service_account_from_dict=MagicMock(return_value=client))

        with patch.dict(sys.modules, {"gspread": fake_gspread_module}):
            connector = GSheetsConnector(
                service_account_info={"type": "service_account", "client_email": "svc@example.iam.gserviceaccount.com"},
                spreadsheet_id="spreadsheet-123",
            )
            connector.connect()
            result = connector.fetch_data("Sheet1")
            connector.close()

        self.assertIsInstance(result, IngestionResult)
        self.assertTrue(result.success)
        self.assertEqual(result.protocol, "gsheets")
        self.assertEqual(result.items[0].source_path, "Sheet1")
        self.assertEqual(result.items[0].payload, records)
        self.assertEqual(result.metadata["worksheet"], "Sheet1")
        self.assertEqual(result.metadata["rows"], 2)
        self.assertEqual(result.metadata["spreadsheet_id"], "spreadsheet-123")

        fake_gspread_module.service_account_from_dict.assert_called_once_with(
            {"type": "service_account", "client_email": "svc@example.iam.gserviceaccount.com"}
        )
        client.open_by_key.assert_called_once_with("spreadsheet-123")
        spreadsheet.worksheet.assert_called_once_with("Sheet1")
        worksheet.get_all_records.assert_called_once()

    def test_soap_connector_connect_uses_http_basic_auth_when_credentials_provided(self):
        mock_client_ctor = MagicMock()
        transport = MagicMock()
        mock_transport_ctor = MagicMock(return_value=transport)

        fake_zeep_module = types.ModuleType("zeep")
        fake_zeep_module.Client = mock_client_ctor

        fake_zeep_transports_module = types.ModuleType("zeep.transports")
        fake_zeep_transports_module.Transport = mock_transport_ctor

        with patch.dict(
            sys.modules,
            {
                "zeep": fake_zeep_module,
                "zeep.transports": fake_zeep_transports_module,
            },
        ):
            with patch("requests.Session") as mock_session_ctor:
                with patch("requests.auth.HTTPBasicAuth", return_value=("basic", "user", "pw")) as mock_basic_auth:
                    session = MagicMock()
                    mock_session_ctor.return_value = session

                    connector = SOAPConnector(
                        wsdl_url="https://api.local/service.wsdl",
                        username="user",
                        password="pw",
                    )
                    connector.connect()

        mock_basic_auth.assert_called_once_with("user", "pw")
        self.assertEqual(session.auth, ("basic", "user", "pw"))
        mock_transport_ctor.assert_called_once_with(session=session)
        mock_client_ctor.assert_called_once_with(wsdl="https://api.local/service.wsdl", transport=transport)

    def test_soap_connector_fetch_data_serializes_zeep_response(self):
        zeep_response = SimpleNamespace(order_id=10, status="ok")
        soap_method = MagicMock(return_value=zeep_response)
        soap_service = SimpleNamespace(GetOrder=soap_method)

        mock_serialize_object = MagicMock(return_value={"order_id": 10, "status": "ok"})
        fake_zeep_helpers_module = types.ModuleType("zeep.helpers")
        fake_zeep_helpers_module.serialize_object = mock_serialize_object

        with patch.dict(sys.modules, {"zeep.helpers": fake_zeep_helpers_module}):
            connector = SOAPConnector(wsdl_url="https://api.local/service.wsdl")
            connector._client = SimpleNamespace(service=soap_service)

            result = connector.fetch_data("GetOrder")

        self.assertIsInstance(result, IngestionResult)
        self.assertTrue(result.success)
        self.assertEqual(result.protocol, "soap")
        self.assertEqual(result.items[0].source_path, "GetOrder")
        self.assertEqual(result.items[0].payload, {"result": {"order_id": 10, "status": "ok"}})
        self.assertEqual(result.metadata["method"], "GetOrder")
        self.assertEqual(result.metadata["wsdl_url"], "https://api.local/service.wsdl")

        soap_method.assert_called_once_with()
        mock_serialize_object.assert_called_once_with(zeep_response)


class StreamConnectorTests(unittest.TestCase):
    def test_kafka_connector_consumes_micro_batch_and_writes_parquet(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            class FakeKafkaMessage:
                def error(self):
                    return None

                def value(self):
                    return b'{"order_id": 10}'

                def key(self):
                    return b"k-1"

                def topic(self):
                    return "orders"

                def partition(self):
                    return 0

                def offset(self):
                    return 12

            class FakeConsumer:
                def __init__(self, _config):
                    self.calls = 0

                def subscribe(self, _topics):
                    return None

                def poll(self, _timeout):
                    self.calls += 1
                    if self.calls == 1:
                        return FakeKafkaMessage()
                    return None

                def close(self):
                    return None

            fake_kafka_module = SimpleNamespace(Consumer=FakeConsumer)

            with patch.dict(sys.modules, {"confluent_kafka": fake_kafka_module}):
                connector = KafkaConnector(
                    bootstrap_servers="localhost:9092",
                    topic="orders",
                    group_id="etl-consumer",
                    lake_path=temp_dir,
                    max_messages=10,
                    max_wait_seconds=0.3,
                    poll_timeout_seconds=0.01,
                )
                connector.connect()
                result = connector.fetch_data("")
                connector.close()

                self.assertTrue(result.success)
                self.assertEqual(result.protocol, "kafka")
                self.assertEqual(result.metadata["messages"], 1)
                self.assertEqual(len(result.items), 1)
                self.assertTrue(Path(result.items[0].lake_path).exists())


class ConnectorFactoryTests(unittest.TestCase):
    def test_create_connector_from_dict_builds_amqp_connector(self):
        connector = create_connector(
            {
                "protocol": "amqp",
                "host": "rabbitmq.local",
                "queue": "orders",
                "username": "guest",
                "password": "guest",
            }
        )

        self.assertIsInstance(connector, AMQPConnector)
        self.assertEqual(connector.config.host, "rabbitmq.local")
        self.assertEqual(connector.config.queue, "orders")

    def test_create_connector_from_dict_builds_soap_connector(self):
        connector = create_connector(
            {
                "protocol": "soap",
                "wsdl_url": "https://example.com/service?wsdl",
                "username": "soap_user",
                "password": "soap_password",
            }
        )

        self.assertIsInstance(connector, SOAPConnector)
        self.assertEqual(str(connector.config.wsdl_url), "https://example.com/service?wsdl")
        self.assertEqual(connector.config.username, "soap_user")

    def test_create_connector_from_dict_builds_webdav_connector(self):
        connector = create_connector(
            {
                "protocol": "webdav",
                "base_url": "https://webdav.example.com/",
                "username": "dav_user",
                "password": "dav_password",
            }
        )

        self.assertIsInstance(connector, WebDAVConnector)
        self.assertEqual(str(connector.config.base_url), "https://webdav.example.com/")
        self.assertEqual(connector.config.username, "dav_user")

    def test_load_connector_config_reads_json_file(self):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as temp_file:
            json.dump(
                {
                    "protocol": "amqp",
                    "host": "rabbitmq.local",
                    "queue": "orders",
                    "username": "guest",
                    "password": "guest",
                },
                temp_file,
            )
            file_path = temp_file.name

        try:
            loaded = load_connector_config(file_path)
            connector = create_connector(file_path)
        finally:
            os.unlink(file_path)

        self.assertEqual(loaded["protocol"], "amqp")
        self.assertIsInstance(connector, AMQPConnector)

    def test_load_connector_config_reads_yaml_file(self):
        with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as temp_file:
            temp_file.write(
                """
protocol: amqp
host: rabbitmq.local
queue: orders
username: guest
password: guest
""".strip()
            )
            file_path = temp_file.name

        try:
            loaded = load_connector_config(file_path)
            connector = create_connector(file_path)
        finally:
            os.unlink(file_path)

        self.assertEqual(loaded["protocol"], "amqp")
        self.assertIsInstance(connector, AMQPConnector)

    def test_create_connector_raises_for_unknown_protocol(self):
        with self.assertRaises(ValueError):
            create_connector(
                {
                    "protocol": "unknown-stream",
                }
            )

    def test_amqp_connector_consumes_micro_batch_and_writes_parquet(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            class FakeMethodFrame:
                delivery_tag = 7
                exchange = "events"
                routing_key = "orders"

            class FakeHeaderFrame:
                content_type = "application/json"

            class FakeChannel:
                def __init__(self):
                    self.calls = 0
                    self.acked: list[int] = []

                def queue_declare(self, queue, durable):
                    return None

                def basic_get(self, queue, auto_ack):
                    self.calls += 1
                    if self.calls == 1:
                        return FakeMethodFrame(), FakeHeaderFrame(), b'{"event": "created"}'
                    return None, None, None

                def basic_ack(self, delivery_tag):
                    self.acked.append(delivery_tag)

                def close(self):
                    return None

            class FakeConnection:
                def __init__(self, _params):
                    self.channel_instance = FakeChannel()

                def channel(self):
                    return self.channel_instance

                def close(self):
                    return None

            fake_pika_module = SimpleNamespace(
                PlainCredentials=lambda username, password: (username, password),
                ConnectionParameters=lambda **kwargs: kwargs,
                BlockingConnection=FakeConnection,
            )

            with patch.dict(sys.modules, {"pika": fake_pika_module}):
                connector = AMQPConnector(
                    host="rabbitmq.local",
                    queue="orders",
                    username="guest",
                    password="guest",
                    lake_path=temp_dir,
                    max_messages=10,
                    max_wait_seconds=0.3,
                    poll_timeout_seconds=0.01,
                )
                connector.connect()
                result = connector.fetch_data("")
                connector.close()

                self.assertTrue(result.success)
                self.assertEqual(result.protocol, "amqp")
                self.assertEqual(result.metadata["messages"], 1)
                self.assertEqual(len(result.items), 1)
                self.assertTrue(Path(result.items[0].lake_path).exists())

    def test_nats_connector_consumes_micro_batch_and_writes_parquet(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            class FakeNatsMessage:
                subject = "orders"
                reply = ""
                data = b'{"status":"ok"}'

            class FakeSubscription:
                def __init__(self):
                    self.calls = 0

                async def next_msg(self, timeout):
                    self.calls += 1
                    if self.calls == 1:
                        return FakeNatsMessage()
                    raise TimeoutError("no message")

                async def unsubscribe(self):
                    return None

            class FakeNatsConnection:
                async def subscribe(self, subject, queue):
                    return FakeSubscription()

                async def drain(self):
                    return None

                async def close(self):
                    return None

            async def fake_connect(servers):
                return FakeNatsConnection()

            fake_nats_module = SimpleNamespace(connect=fake_connect)

            with patch.dict(sys.modules, {"nats": fake_nats_module}):
                connector = NATSConnector(
                    servers=["nats://localhost:4222"],
                    subject="orders",
                    lake_path=temp_dir,
                    max_messages=10,
                    max_wait_seconds=0.3,
                    poll_timeout_seconds=0.01,
                )
                connector.connect()
                result = connector.fetch_data("")
                connector.close()

                self.assertTrue(result.success)
                self.assertEqual(result.protocol, "nats")
                self.assertEqual(result.metadata["messages"], 1)
                self.assertEqual(len(result.items), 1)
                self.assertTrue(Path(result.items[0].lake_path).exists())


class PublicRouterTests(unittest.TestCase):
    def test_get_connection_routes_supported_sources(self):
        with patch("connections.get_dw_engine", return_value="dw"):
            self.assertEqual(connections.get_connection("dw"), "dw")

        with patch("connections.get_mssql_engine", return_value="mssql"):
            self.assertEqual(connections.get_connection("sqlserver"), "mssql")

        with patch("connections.get_oracle_engine", return_value="oracle"):
            self.assertEqual(connections.get_connection("oracle_db"), "oracle")

        with patch("connections.get_rest_session", return_value="rest"):
            self.assertEqual(connections.get_connection("http"), "rest")

        with patch("connections.get_postgres_engine", return_value="source_pg"):
            self.assertEqual(connections.get_connection("postgresql"), "source_pg")

        with patch("connections.get_sqlite_engine", return_value="sqlite"):
            self.assertEqual(connections.get_connection("sqlite3"), "sqlite")

        with patch("connections.get_cassandra_session", return_value="cass"):
            self.assertEqual(connections.get_connection("cassandra"), "cass")

        with patch("connections.get_neo4j_driver", return_value="neo"):
            self.assertEqual(connections.get_connection("bolt"), "neo")

    def test_test_connection_routes_supported_sources(self):
        with patch("connections.test_dw_connection", return_value=True):
            self.assertTrue(connections.test_connection("postgres"))

        with patch("connections.test_mssql_connection", return_value=True):
            self.assertTrue(connections.test_connection("mssql"))

        with patch("connections.test_oracle_connection", return_value=True):
            self.assertTrue(connections.test_connection("oracle"))

        with patch("connections.test_rest_connection", return_value=True):
            self.assertTrue(connections.test_connection("api"))

        with patch("connections.test_postgres_connection", return_value=True):
            self.assertTrue(connections.test_connection("pgsql"))

        with patch("connections.test_sqlite_connection", return_value=True):
            self.assertTrue(connections.test_connection("sqlite"))

        with patch("connections.test_cassandra_connection", return_value=True):
            self.assertTrue(connections.test_connection("cassandra"))

        with patch("connections.test_neo4j_connection", return_value=True):
            self.assertTrue(connections.test_connection("neo4j"))

    def test_router_raises_for_unsupported_source(self):
        with self.assertRaises(ValueError):
            connections.get_connection("csv")
        with self.assertRaises(ValueError):
            connections.test_connection("csv")


if __name__ == "__main__":
    unittest.main()

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
ETL_PATH = ROOT / "etl"
if str(ETL_PATH) not in sys.path:
    sys.path.insert(0, str(ETL_PATH))

from etl.cli import main  # noqa: E402


def test_cli_run_success():
    mock_result = {
        "run_id": "123",
        "status": "success",
        "rows_loaded": 10,
        "parquet_files": 1,
        "duration_seconds": 0.5
    }
    
    with patch("etl.cli.load_connector_config", return_value={"protocol": "http"}), \
         patch("etl.cli.get_dw_engine", return_value=MagicMock()), \
         patch("etl.cli.run_pipeline", return_value=mock_result), \
         patch("sys.stdout.write") as mock_stdout, \
         patch("sys.exit") as mock_exit:
        
        sys.argv = [
            "cli.py", "run", 
            "--config", "conf.json", 
            "--query", "GET /", 
            "--source", "src", 
            "--table", "tbl", 
            "--lake", "/tmp/lake"
        ]
        main()
        
        # Verify JSON was printed to stdout
        output = "".join(call.args[0] for call in mock_stdout.call_args_list)
        assert json.loads(output) == mock_result
        mock_exit.assert_called_once_with(0)


def test_cli_test_connection_dw_success():
    with patch("etl.cli.test_dw_connection", return_value=True), \
         patch("sys.stdout.write") as mock_stdout, \
         patch("sys.exit") as mock_exit:
        
        sys.argv = ["cli.py", "test-connection", "--source", "dw"]
        main()
        
        output = "".join(call.args[0] for call in mock_stdout.call_args_list)
        assert json.loads(output)["success"] is True
        mock_exit.assert_called_once_with(0)


def test_cli_missing_args():
    # argparse prints to stderr and calls sys.exit(2) for missing required args
    with patch("sys.exit") as mock_exit, patch("sys.stderr.write"):
        sys.argv = ["cli.py", "run"] # missing --config etc
        main()
        # It might call exit multiple times if it keeps going after argparse.parse_args
        # but the first exit (from argparse) should be code 2
        assert mock_exit.call_args_list[0].args[0] == 2

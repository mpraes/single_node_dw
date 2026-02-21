import argparse
import json
import sys
from pathlib import Path

from connections.dw_destination import test_dw_connection, get_dw_engine
from connections.sources.factory import load_connector_config
from connections import test_connection as test_source_connection
from pipeline.runner import run_pipeline


def cmd_test_connection(args):
    """Handle test-connection subcommand."""
    success = False
    if args.source == "dw":
        success = test_dw_connection()
        label = "Data Warehouse (PostgreSQL)"
    elif args.config:
        try:
            config = load_connector_config(args.config)
            protocol = config.get("protocol", "unknown")
            success = test_source_connection(protocol, config=config)
            label = f"Source ({protocol}) from {args.config}"
        except Exception as e:
            print(f"Error loading config: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        print("Error: must specify --source dw or --config <path>", file=sys.stderr)
        sys.exit(2)

    result = {"success": success, "label": label}
    print(json.dumps(result))
    
    if success:
        print(f"Connection to {label} successful.", file=sys.stderr)
        sys.exit(0)
    else:
        print(f"Connection to {label} failed.", file=sys.stderr)
        sys.exit(1)


def cmd_run(args):
    """Handle run subcommand."""
    try:
        connector_config = load_connector_config(args.config)
    except Exception as e:
        print(f"Error loading connector config: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        dw_engine = get_dw_engine()
        
        result = run_pipeline(
            connector_config=connector_config,
            query=args.query,
            source_name=args.source,
            target_table=args.table,
            lake_path=args.lake,
            dw_engine=dw_engine,
            schema=args.schema,
            pipeline_name=args.pipeline
        )
        
        print(json.dumps(result))
        
        if result["status"] == "success":
            print(f"Pipeline finished successfully. Rows loaded: {result['rows_loaded']}", file=sys.stderr)
            sys.exit(0)
        else:
            print(f"Pipeline failed: {result.get('error')}", file=sys.stderr)
            sys.exit(1)
            
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Single Node DW ETL CLI")
    subparsers = parser.add_subparsers(dest="command", help="Subcommand to run")

    # Run command
    run_parser = subparsers.add_parser("run", help="Run an ETL pipeline")
    run_parser.add_argument("--config", required=True, help="Path to connector JSON/YAML config")
    run_parser.add_argument("--query", required=True, help="Query or resource to fetch")
    run_parser.add_argument("--source", required=True, help="Logical name of the source")
    run_parser.add_argument("--table", required=True, help="Target DW table name")
    run_parser.add_argument("--lake", required=True, help="Base path for the data lake (Parquet)")
    run_parser.add_argument("--schema", default="public", help="Target DW schema (default: public)")
    run_parser.add_argument("--pipeline", default="default", help="Pipeline name for auditing")

    # Test-connection command
    test_parser = subparsers.add_parser("test-connection", help="Test a connection")
    test_parser.add_argument("--source", choices=["dw"], help="Test the data warehouse connection")
    test_parser.add_argument("--config", help="Test a source connection using a config file")

    args = parser.parse_args()

    if args.command == "run":
        cmd_run(args)
    elif args.command == "test-connection":
        cmd_test_connection(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()

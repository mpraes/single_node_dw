.PHONY: docs-serve docs-build docs-clean make-tests-conn make-tests-staging make-tests-pipeline make-tests-all infra-up infra-down infra-status run-pipeline test-dw integration-test test-mage-integration

infra-up:
	docker compose up -d

infra-down:
	docker compose down

infra-status:
	docker compose ps

docs-serve:
	uv run --group dev mkdocs serve -f mkdocs.yml

docs-build:
	uv run --group dev mkdocs build -f mkdocs.yml
	rm -rf docs/site
	mv site docs/site

docs-clean:
	rm -rf docs/site

make-tests-conn:
	set -a; [ -f .env ] && . ./.env; set +a; \
	uv run --with pytest --with-requirements etl/requirements.txt pytest -q tests/test_connections.py

make-tests-staging:
	uv run --with pytest --with-requirements etl/requirements.txt pytest -q tests/test_staging.py

make-tests-pipeline:
	uv run --with pytest --with-requirements etl/requirements.txt pytest -q tests/test_pipeline.py

make-tests-all:
	uv run --with pytest --with-requirements etl/requirements.txt pytest -q tests/

run-pipeline:
	cd etl && uv run --with-requirements requirements.txt python -m etl.cli run \
		--config $(CONFIG) --query "$(QUERY)" --source $(SOURCE) \
		--table $(TABLE) --lake ./lake

test-dw:
	cd etl && uv run --with-requirements requirements.txt python -m etl.cli test-connection --source dw

integration-test:
	@echo "🔧 Running integration tests..."
	@echo "📋 Testing infrastructure..."
	@$(MAKE) infra-status
	@echo "📋 Testing DW connection..."
	@$(MAKE) test-dw
	@echo "📋 Testing ETL framework in Mage..."
	@$(MAKE) test-mage-integration
	@echo "✅ All integration tests completed!"

test-mage-integration:
	@echo "🧪 Testing Mage.ai integration..."
	@echo "  - Testing DW connection through Mage..."
	docker exec dw_mage bash -c "cd /home/src/project && python -c 'from custom.etl_runner import test_dw_connection; result = test_dw_connection(); print(\"✅ DW connection:\", result[\"success\"])'"
	@echo "  - Testing ETL framework imports..."
	docker exec dw_mage python -c "import sys; sys.path.append('/app'); import etl; print('✅ ETL framework imports successfully')"
	@echo "  - Testing custom Mage blocks..."
	docker exec dw_mage bash -c "cd /home/src/project && python -c 'from custom.etl_runner import execute_etl_pipeline, test_connection; print(\"✅ Custom Mage blocks loaded successfully\")'"

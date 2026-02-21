.PHONY: docs-serve docs-build docs-clean make-tests-conn

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

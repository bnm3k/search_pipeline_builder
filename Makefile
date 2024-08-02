.SILENT:
.DEFAULT_GOAL:=run
SHELL:=/usr/bin/bash

.PHONY: run format clean

run:
	python load.py

format:
	black .

clean:
	rm -rf .venv/
	find . -type d -name '__pycache__' -exec rm -rf {} +
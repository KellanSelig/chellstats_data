#!/bin/bash
set -euo pipefail
trap 's=$?; echo "$0: Error on line "$LINENO": $BASH_COMMAND"; exit $s' ERR
IFS=$'\n\t'

echo "Running Code Quality Checks"

echo "Formatting files..."
isort .
black .
echo

echo "Checking Style..."
flake8
echo

echo "Checking Type Annotations..."
mypy .
echo

echo "Running tests..."
# etl tests
PYTHONPATH=. pytest ./tests/etl -s

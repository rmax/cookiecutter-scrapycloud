#!/bin/bash

set -e

require() {
    type $1 >/dev/null 2>/dev/null
}

cleanup() {
    rm -rf scrapy-project
}
trap cleanup EXIT


require cookiecutter

echo "Running test script..."
cookiecutter . --no-input
(
    cd ./scrapy-project
    pip install -r dev-requirements.txt -r requirements.txt 
    python setup.py test bdist_egg
    scrapy list
    for SCRIPT in bin/*.py; do
      $SCRIPT -h
    done
)

echo Done

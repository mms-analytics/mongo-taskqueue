#!/bin/bash

source .venv/bin/activate

rm dist/*
python -m build
twine upload -u SHi-ON dist/*

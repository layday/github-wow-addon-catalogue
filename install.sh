#!/bin/bash
set -e
rm -rf venv
python -m venv venv
source venv/bin/activate
pip install pip wheel --upgrade
pip install -e .

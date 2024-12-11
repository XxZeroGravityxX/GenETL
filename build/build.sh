#!/bin/bash

# Set variables

## Environment name
env_name=gen_etl
## Anaconda path
anaconda_path=$HOME/anaconda3/etc/profile.d/conda.sh
## Python version
py_version=3.11

# Build the package

## Enable conda
if [ -f "$anaconda_path" ]; then
    source "$anaconda_path"
else
    echo "Conda script not found at $anaconda_path, skipping to environment creation..."
fi
## Create conda environment
if conda env list | grep -q "$env_name"; then
    echo "Conda environment $env_name already exists, skipping creation..."
else
    conda create -n $env_name python=$py_version -y
fi
## Activate conda environment
if conda env list | grep -q "$env_name"; then
    conda activate $env_name
else
    echo "Failed to activate conda environment $env_name. Aborting..."
    exit 1
fi
## Install requirements
if [ -f "requirements.txt" ]; then
    pip install -r requirements.txt
else
    echo "requirements.txt not found. Aborting..."
    exit 1
fi
## Change directory to the root folder of the project
cd "$(dirname "$0")/../.."
echo "Building from $(pwd)"
## Make build
python build/build_pkg.py

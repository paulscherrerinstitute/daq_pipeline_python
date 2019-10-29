#!/bin/bash

VERSION=0.0.1

docker login

python setup.py build

docker build -f docker/Dockerfile --no-cache=true -t paulscherrerinstitute/daq_pipeline_python .
docker tag paulscherrerinstitute/daq_pipeline_python paulscherrerinstitute/daq_pipeline_python:$VERSION

docker push paulscherrerinstitute/daq_pipeline_python:$VERSION
docker push paulscherrerinstitute/daq_pipeline_python

#!/usr/bin/env bash

# build artifact script - start
set -eax
function make_job_lib() {
    echo "Make joblib.zip - Start"
    mkdir -p .compose_lib
    # make variables files
    echo "Make input job variables file - Start"
    rm -rf variables.py
    touch variables.py
    echo "NAMESPACE='${NAMESPACE}'" >> "variables.py"
    echo "ARTIFACT_BUCKET_NAME='${ARTIFACT_BUCKET_NAME}'" >> "variables.py"
    echo "DATA_LANDING_BUCKET_NAME='${DATA_LANDING_BUCKET_NAME}'" >> "variables.py"
    echo "RAW_ZONE='raw_zone'" >> "variables.py"
    echo "GOLDEN_ZONE='golden_zone'" >> "variables.py"
    yes | cp -rf variables.py joblib/
    cat joblib/variables.py
    aws s3 cp variables.py s3://${ARTIFACT_BUCKET_NAME}/glue_libs/${REPO_NAME}/variables.py
    echo "Make input job variables file - End"

    zip -r .compose_lib/joblib.zip joblib/ -x '*__pycache__*'
    touch deployment/__init__.py
    zip -uj .compose_lib/joblib.zip deployment/__init__.py
    aws s3 cp .compose_lib/joblib.zip s3://${ARTIFACT_BUCKET_NAME}/glue_libs/${REPO_NAME}/joblib.zip
    echo "Make joblib.zip - End"
}

make_job_lib
# build artifacts script - end
# upload jobs
echo "Upload glue job script to s3 - Start"
aws s3 cp jobs/ s3://${ARTIFACT_BUCKET_NAME}/glue_jobs/${NAMESPACE} --recursive --exclude "*" --include "*.py"
echo "Upload glue job script to s3 - End"
echo "Upload glue job libs/jars/whl to s3 - Start"
aws s3 cp deployment/jars/ s3://${ARTIFACT_BUCKET_NAME}/glue_libs/${NAMESPACE} --include "*.jar" --recursive
aws s3 cp deployment/whl/ s3://${ARTIFACT_BUCKET_NAME}/glue_libs/${NAMESPACE} --include "*.whl" --recursive
aws s3 cp deployment/whl/ s3://${ARTIFACT_BUCKET_NAME}/pip_pkg/${NAMESPACE} --include "*.whl" --recursive
echo "Upload glue job libs/jars/whl to s3 - End"
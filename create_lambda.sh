#!/bin/bash
# Based on https://github.com/m3h/loadshedding-stage-api/blob/master/1_create_lambda_zip.sh

FUNCTION_FILE="scripts/upload_eskom_current_stage.py"
LAMBDA_NAME="upload_eskom_current_stage"

OUTPUT_ZIP=${LAMBDA_NAME}-deployment-package.zip
STAGING_DIR=./${LAMBDA_NAME}-aws-deploy

rm -rf "$OUTPUT_ZIP" "$STAGING_DIR"
mkdir "$STAGING_DIR"
cp -r ./database ./loadshedding_coct_stage_query ./scraping "$STAGING_DIR"
cp -r ${FUNCTION_FILE} "${STAGING_DIR}/lambda_function.py"


python3.8 -m pip install --target "$STAGING_DIR" -r ./requirements.txt
cd "$STAGING_DIR"
zip -r "../$OUTPUT_ZIP" .
cd ..

rm -rf "$STAGING_DIR"

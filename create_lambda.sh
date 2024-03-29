#!/bin/bash
# Based on https://github.com/m3h/loadshedding-stage-api/blob/master/1_create_lambda_zip.sh

set -e
set -x

FUNCTION_FILE=${1:-"scripts/upload_national_current_schedule_stage.py"}
LAMBDA_NAME=${2:-"upload_national_current_schedule_stage"}

OUTPUT_ZIP=${LAMBDA_NAME}-deployment-package.zip
STAGING_DIR=./${LAMBDA_NAME}-aws-deploy

rm -rf "$OUTPUT_ZIP" "$STAGING_DIR"
mkdir "${STAGING_DIR}"
# Only include .py file in modules (exclude cache etc.)
# https://stackoverflow.com/a/11111793
rsync -a  --include="*/" --include="*.py" --exclude="*" --prune-empty-dirs ./database ./loadshedding_thingamabob ./utility "${STAGING_DIR}"
cp -r ${FUNCTION_FILE} "${STAGING_DIR}/lambda_function.py"

[ -s ./requirements.txt ] && python3.10 -m pip install --target "${STAGING_DIR}" -r ./requirements.txt
cd "${STAGING_DIR}"
zip -r "../$OUTPUT_ZIP" .
cd ..

rm -rf "${STAGING_DIR}"

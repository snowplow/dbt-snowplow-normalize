#!/bin/bash

# Expected input:
# -d (database) target database for dbt

while getopts 'd:' opt
do
  case $opt in
    d) DATABASE=$OPTARG
  esac
done

declare -a SUPPORTED_DATABASES=("bigquery" "databricks" "snowflake")

# set to lower case
DATABASE="$(echo $DATABASE | tr '[:upper:]' '[:lower:]')"

if [[ $DATABASE == "all" ]]; then
  DATABASES=( "${SUPPORTED_DATABASES[@]}" )
else
  DATABASES=$DATABASE
fi

for db in ${DATABASES[@]}; do

  echo "Snowplow normalize integration tests: snakeify case"

  eval "dbt run-operation test_snakeify_case --target $db" || exit 1;

  echo "Snowplow normalize integration tests: normalize events"

  eval "dbt run-operation test_normalize_events --target $db" || exit 1;

  echo "Snowplow normalize integration tests: users table"

  eval "dbt run-operation test_users_table --target $db" || exit 1;

  echo "Snowplow normalize integration tests: All tests passed"

done

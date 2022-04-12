#!/bin/bash

function main_inside_docker {
  DBNAME=${1:-testdb}
  echo "creating database $DBNAME"
  PATH=$MSSQL_PATH:$PATH
  SQL="
  create database $DBNAME;
  select database_id, name, create_date from sys.databases;
  "
  sqlcmd -U SA -P $SA_PASSWORD -Q "$SQL"
}

CMD="$(declare -f main_inside_docker); main_inside_docker $@"
docker-compose exec -T mssql bash -c "$CMD"

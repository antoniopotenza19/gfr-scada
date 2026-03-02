#!/usr/bin/env bash
set -euo pipefail

# DEV ONLY: this will delete data. Use with caution.
echo "DEV ONLY: this will delete data from the database."

DROP_OTHERS=0
ASSUME_YES=0

usage(){
  cat <<EOF
Usage: $(basename "$0") [-y] [--drop-others]

  -y              : assume yes (no interactive confirmation)
  --drop-others   : also drop dev tables: alarms, users, commands

This script will try to run SQL via the DB container using:
  docker compose exec db psql -U \$POSTGRES_USER -d \$POSTGRES_DB -c "..."

If neither `docker` nor `psql` is available, the script will print manual commands.
EOF
}

for arg in "$@"; do
  case "$arg" in
    -y) ASSUME_YES=1 ;;
    --drop-others) DROP_OTHERS=1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $arg"; usage; exit 2 ;;
  esac
done

POSTGRES_USER=${POSTGRES_USER:-postgres}
POSTGRES_DB=${POSTGRES_DB:-gfr_scada}
DB_SERVICE=${DB_SERVICE:-db}

SQL_COMMON="DROP TABLE IF EXISTS measurements CASCADE; DROP TABLE IF EXISTS alembic_version CASCADE;"
SQL_OTHERS="DROP TABLE IF EXISTS alarms CASCADE; DROP TABLE IF EXISTS users CASCADE; DROP TABLE IF EXISTS commands CASCADE;"

SQL_TO_RUN="$SQL_COMMON"
if [ "$DROP_OTHERS" -eq 1 ]; then
  SQL_TO_RUN="$SQL_TO_RUN $SQL_OTHERS"
fi

echo
echo "About to run the following SQL against database '$POSTGRES_DB' as user '$POSTGRES_USER':"
echo "$SQL_TO_RUN"
echo

if [ "$ASSUME_YES" -ne 1 ]; then
  read -r -p "Type 'yes' to continue (this will DELETE DATA): " confirm
  if [ "$confirm" != "yes" ]; then
    echo "Aborted by user."; exit 1
  fi
fi

run_via_docker_compose(){
  if command -v docker >/dev/null 2>&1 ; then
    echo "Running SQL via: docker compose exec $DB_SERVICE psql -U $POSTGRES_USER -d $POSTGRES_DB"
    docker compose exec "$DB_SERVICE" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "$SQL_TO_RUN"
    return $?
  fi
  return 1
}

run_via_psql(){
  if command -v psql >/dev/null 2>&1 ; then
    echo "Running SQL via local psql"
    psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "$SQL_TO_RUN"
    return $?
  fi
  return 1
}

if run_via_docker_compose ; then
  echo "Done (via docker compose)."
  exit 0
fi

if run_via_psql ; then
  echo "Done (via psql)."
  exit 0
fi

cat <<EOF
Could not find a way to run SQL automatically (no 'docker' or 'psql' command available).
Run the following commands from your host machine in the project root instead:

docker compose exec db psql -U $POSTGRES_USER -d $POSTGRES_DB -c "$SQL_COMMON"

# To also drop other dev tables:
docker compose exec db psql -U $POSTGRES_USER -d $POSTGRES_DB -c "$SQL_OTHERS"

After cleanup, re-run migrations in the backend:
  docker compose exec backend alembic upgrade head

EOF

exit 2

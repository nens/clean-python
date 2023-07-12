#!/bin/sh
# wait-for-postgres.sh

# https://docs.docker.com/compose/startup-order/
# https://gist.github.com/mihow/9c7f559807069a03e302605691f85572?permalink_comment_id=3709779#gistcomment-3709779
# https://www.postgresql.org/docs/current/libpq-envars.html

set -e

until psql "postgres://$POSTGRES_URL/" -c '\q'; do
  >&2 echo "Postgres is unavailable - sleeping"
  sleep 1
done

echo "$@"

>&2 echo "Postgres is up - executing command"
exec "$@"

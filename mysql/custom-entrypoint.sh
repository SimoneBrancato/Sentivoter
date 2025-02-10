#!/bin/bash
set -e

# Launch MySQL service
/usr/local/bin/docker-entrypoint.sh mysqld &

# Wait for mysql service to be ready
until mysqladmin ping -h "127.0.0.1" --silent; do
  echo "Waiting for MySQL..."
  sleep 5
done

# Check if there are any tables in the database
TABLE_COUNT=$(mysql -u root -proot -e "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='Elections';" -s --skip-column-names)

if [ "$TABLE_COUNT" -eq 0 ]; then
  echo "No tables found. Initializing database..."
  mysql -u root -proot < /dump.sql
else
  echo "Database already initialized. Skipping import."
fi

wait

#!/bin/bash
set -e

# Launch MySQL service
/usr/local/bin/docker-entrypoint.sh mysqld &

# Wait for mysql service to be ready
until mysqladmin ping -h "127.0.0.1" --silent; do
  echo "Waiting for MySQL..."
  sleep 5
done

# Load data into db
echo "Initializing database..."
mysql -u root -proot < /dump.sql

wait

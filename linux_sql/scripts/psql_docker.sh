#!/bin/sh

# Capture CLI arguments
cmd=$1
db_username=$2
db_password=$3

# Ensure Docker daemon is running (if status fails, start it)
sudo systemctl status docker >/dev/null 2>&1 || sudo systemctl start docker

# Check if the container already exists
docker container inspect jrvs-psql >/dev/null 2>&1
container_status=$?

case "$cmd" in
  create)
    # If container exists, exit with error
    if [ $container_status -eq 0 ]; then
      echo "Container already exists"
      exit 1
    fi

    # Require username and password
    if [ $# -ne 3 ]; then
      echo "Create requires username and password"
      echo "Usage: $0 create <db_username> <db_password>"
      exit 1
    fi

    # Create a named volume if it doesn't exist
    docker volume inspect jrvs-psql >/dev/null 2>&1 || docker volume create jrvs-psql >/dev/null

    # Run a new Postgres container
    docker run -d \
      --name jrvs-psql \
      -e POSTGRES_USER="$db_username" \
      -e POSTGRES_PASSWORD="$db_password" \
      -p 5432:5432 \
      -v jrvs-psql:/var/lib/postgresql/data \
      postgres:16

    exit $?
    ;;

  start|stop)
    # Must be created first
    if [ $container_status -ne 0 ]; then
      echo "Container has not been created"
      exit 1
    fi

    # Start or stop the container
    docker container "$cmd" jrvs-psql
    exit $?
    ;;

  *)
    echo "Illegal command"
    echo "Commands: start|stop|create"
    exit 1
    ;;
esac

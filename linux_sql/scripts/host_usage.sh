#!/usr/bin/env bash
# Usage: bash scripts/host_usage.sh <psql_host> <psql_port> <db_name> <psql_user> <psql_password>
set -euo pipefail

#Setup and validate arguments
psql_host=$1
psql_port=$2
db_name=$3
psql_user=$4
psql_password=$5


# Validate arg count
if [ "$#" -ne 5 ]; then
    echo "Illegal number of parameters"
    exit 1
fi

#save machine stats
vmstat_mb="$(vmstat --unit M)"
hostname="$(hostname -f)"

memory_free="$(echo "$vmstat_mb" | tail -1 | awk '{print $4}' | xargs)"

cpu_idle="$(vmstat 1 2 | tail -1 | awk '{print $15}' | xargs)"
cpu_kernel="$(vmstat 1 2 | tail -1 | awk '{print $14}' | xargs)"

disk_io="$(vmstat -d | tail -1 | awk '{print $10}' | xargs)"
disk_available="$(df -BM / | tail -1 | awk '{print $4}' | sed 's/M//' | xargs)"

#current time
timestamp="$(date -u '+%F %T')"

#construct INSERT
host_id="(SELECT id FROM host_info WHERE hostname='$(printf "%s" "$hostname" | sed "s/'/''/g")')"
insert_stmt="
INSERT INTO host_usage (\"timestamp\", host_id, memory_free, cpu_idle, cpu_kernel, disk_io, disk_available)
VALUES ('$timestamp', $host_id, $memory_free, $cpu_idle, $cpu_kernel, $disk_io, $disk_available);
"

#execute INSERT
export PGPASSWORD="$psql_password"
psql -h "$psql_host" -p "$psql_port" -d "$db_name" -U "$psql_user" \
     -v ON_ERROR_STOP=1 -c "$insert_stmt"
exit $?

#!/usr/bin/env bash
# Usage: ./scripts/host_info.sh <psql_host> <psql_port> <db_name> <psql_user> <psql_password>
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

#parse host hardware specifications using bash cmds
lscpu_out="$(lscpu)"                      #
hostname="$(hostname -f)"
cpu_number="$(echo "$lscpu_out" | awk -F: '/^CPU\(s\):/ {print $2}' | xargs)"
cpu_architecture="$(echo "$lscpu_out" | awk -F: '/^Architecture:/ {print $2}' | xargs)"
cpu_model="$(echo "$lscpu_out" | awk -F: '/^Model name:/ {print $2}' | xargs)"
cpu_mhz="$(echo "$lscpu_out" | awk -F: '/^CPU MHz:/ {print $2}' | xargs)"
# Normalize "256K" or "256 KiB" -> numeric kB value (e.g., 256)
l2_cache="$(echo "$lscpu_out" | awk -F: '/^L2 cache:/ {gsub(/[[:space:]]/,"",$2); sub(/Ki?B$/,"K",$2); print $2}' | sed -E 's/K$//' | xargs)"
# Total memory in kB from /proc/meminfo
total_mem="$(awk '/^MemTotal:/ {print $2}' /proc/meminfo | xargs)"
timestamp="$(date -u '+%F %T')"           # UTC in 2019-11-26 14:40:19 format

#construct the INSERT statement

esc() { printf "%s" "$1" | sed "s/'/''/g"; } # escape thesingle quotes in strings
insert_stmt="
INSERT INTO host_info (hostname, cpu_number, cpu_architecture, cpu_model, cpu_mhz, l2_cache, total_mem, \"timestamp\")
VALUES ('$(esc "$hostname")', $cpu_number, '$(esc "$cpu_architecture")', '$(esc "$cpu_model")',
        $cpu_mhz, $l2_cache, $total_mem, '$timestamp');
"

#execute INSERt
export PGPASSWORD="$psql_password"
psql -h "$psql_host" -p "$psql_port" -d "$db_name" -U "$psql_user" \
     -v ON_ERROR_STOP=1 -c "$insert_stmt"

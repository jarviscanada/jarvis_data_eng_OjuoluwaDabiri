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


#cpu_mhz initial
#cpu_mhz="$(echo "$lscpu_out" | awk -F: '/^CPU MHz:/ {print $2}' | xargs)"

cpu_mhz="$(awk -F: '/^CPU MHz:/ {print $2}' <<<"$lscpu_out" | xargs || true)"
# fallback to max MHz if current not reported
if [ -z "${cpu_mhz:-}" ]; then
  cpu_mhz="$(awk -F: '/^CPU max MHz:/ {print $2}' <<<"$lscpu_out" | xargs || true)"
fi
# fallback: average from /proc/cpuinfo
if [ -z "${cpu_mhz:-}" ]; then
  cpu_mhz="$(awk -F: '/^[[:space:]]*cpu MHz[[:space:]]*:/ {sum+=$2; n++} END{if(n) printf("%.1f", sum/n)}' /proc/cpuinfo)"
fi
# keep only digits and dot (defensive)
cpu_mhz="$(printf '%s' "$cpu_mhz" | sed 's/[^0-9.]//g')"

# --- Total memory in KiB (use MemTotal) ---
total_mem="$(awk '/^MemTotal:/ {print $2}' /proc/meminfo)"  # already KiB



l2_line="$(echo "$lscpu_out" | awk -F: '/^L2 cache:/ {print $2}' | xargs)"
l2_num="$(awk '{print $1}' <<<"$l2_line")"    # 256
l2_unit="$(awk '{print $2}' <<<"$l2_line")"   # KiB/MiB/GiB...

case "$l2_unit" in
  KiB|K|KB) l2_cache="$l2_num" ;;
  MiB|M|MB) l2_cache="$(( l2_num * 1024 ))" ;;
  GiB|G|GB) l2_cache="$(( l2_num * 1024 * 1024 ))" ;;
  *)        l2_cache="$l2_num" ;;  # fallback
esac


timestamp="$(date -u '+%F %T')"           # UTC in 2019-11-26 14:40:19 format

#require every variable fro insert stmt
require() { : "${!1:?variable $1 is empty}"; }

for v in hostname cpu_number cpu_architecture cpu_model cpu_mhz l2_cache total_mem timestamp; do
  require "$v"
done



#construct the INSERT statement



esc() { printf "%s" "$1" | sed "s/'/''/g"; } # escape thesingle quotes in strings
insert_stmt="
INSERT INTO host_info (hostname, cpu_number, cpu_architecture, cpu_model, cpu_mhz, l2_cache, total_mem, \"timestamp\")
VALUES ('$(esc "$hostname")', $cpu_number, '$(esc "$cpu_architecture")', '$(esc "$cpu_model")',
        $cpu_mhz, $l2_cache, $total_mem, '$timestamp')
ON CONFLICT (hostname) DO NOTHING;
"


echo "DEBUG cpu_mhz=[$cpu_mhz]"
#echo "DEBUG l2_line=[$l2_line]"
echo "DEBUG l2_cache=[$l2_cache]"
echo "DEBUG total_mem=[$total_mem]"
echo "INSERT statement to run:"
echo "$insert_stmt"


#execute INSERt
export PGPASSWORD="$psql_password"
psql -h "$psql_host" -p "$psql_port" -d "$db_name" -U "$psql_user" \
     -v ON_ERROR_STOP=1 -c "$insert_stmt"

#!/usr/bin/env bash
# Runs a whole bitacora cluster on this machine: one etcd, a few replicas and
# a controller, with a local directory as object store.
#
# Usage: development/cluster.sh <command> [arguments]
#
# Commands:
#   start             Start whatever is not running and wait until it is ready
#   stop              Stop everything: the controller, the replicas, then etcd
#   status            Say what runs, on which ports, and whether it is ready
#   logs [-f] <name>  Print the log of etcd, controller or replica-<n>; -f follows it
#   bita <args>...    Run the bita tool against the cluster
#   reset             Stop everything and delete the state directory
#   help              Print this text
#
# Environment:
#   BITA_DEV_STATE      Where everything of the cluster lives: data, logs,
#                       configuration, pid files. Nothing is written outside it.
#                       Default: ${TMPDIR:-/tmp}/bitacora-dev-$USER
#   BITA_DEV_REPLICAS   How many replicas to run. Default: 3
#   BITA_DEV_BASE_PORT  The first of the ports the cluster uses, all on
#                       127.0.0.1: etcd on +0 and +1, replica n on +10+n (gRPC)
#                       and +20+n (metrics and probes), the controller on +30.
#                       Default: 27000
#
# The programs are the ones Bazel builds: run directly, this script hands
# itself to `bazel run //development:cluster`, which builds them first. To use
# other builds, name all four in BITA_ETCD_BINARY, BITA_REPLICA_BINARY,
# BITA_CONTROLLER_BINARY and BITA_BINARY.
set -euo pipefail

script_directory="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# How long a process may take to become ready, and to stop. The daemons allow
# themselves ten seconds to shut down.
readonly start_allowance_seconds=60
readonly stop_allowance_seconds=20

# The file that marks a directory as the state of a development cluster, so
# that `reset` never deletes anything else.
readonly state_marker=".bitacora-dev-cluster"

usage() {
    sed -n '2,/^set -euo/{/^set -euo/d;s/^# \{0,1\}//;p;}' "${BASH_SOURCE[0]}"
}

fail() {
    echo "cluster.sh: $*" >&2
    exit 1
}

# Without the programs, let Bazel build them and run this script again with
# their paths in the environment.
if [[ -z "${BITA_ETCD_BINARY:-}" || -z "${BITA_REPLICA_BINARY:-}" ||
    -z "${BITA_CONTROLLER_BINARY:-}" || -z "${BITA_BINARY:-}" ]]; then
    case "${1:-help}" in
        help | -h | --help)
            usage
            exit 0
            ;;
    esac
    command -v bazel >/dev/null ||
        fail "bazel is not on the path; install bazelisk, or name the programs in BITA_*_BINARY"
    cd "${script_directory}/.."
    exec bazel run //development:cluster -- "$@"
fi

# A path as it still works after a change of directory.
absolute_path() {
    case "$1" in
        /*) echo "$1" ;;
        *) echo "${PWD}/$1" ;;
    esac
}

etcd_binary="$(absolute_path "${BITA_ETCD_BINARY}")"
replica_binary="$(absolute_path "${BITA_REPLICA_BINARY}")"
controller_binary="$(absolute_path "${BITA_CONTROLLER_BINARY}")"
bita_binary="$(absolute_path "${BITA_BINARY}")"

temporary_directory="${TMPDIR:-/tmp}"
state="${BITA_DEV_STATE:-${temporary_directory%/}/bitacora-dev-${USER:-developer}}"
replica_count="${BITA_DEV_REPLICAS:-3}"
base_port="${BITA_DEV_BASE_PORT:-27000}"

[[ "${replica_count}" =~ ^[1-9]$ ]] || fail "BITA_DEV_REPLICAS must be a number from 1 to 9"
[[ "${base_port}" =~ ^[0-9]+$ ]] || fail "BITA_DEV_BASE_PORT must be a port number"
((base_port >= 1024 && base_port <= 65000)) || fail "BITA_DEV_BASE_PORT must be between 1024 and 65000"

etcd_client_port="${base_port}"
etcd_peer_port="$((base_port + 1))"
controller_port="$((base_port + 30))"
etcd_endpoint="http://127.0.0.1:${etcd_client_port}"

replica_grpc_port() { echo "$((base_port + 10 + $1))"; }
replica_probe_port() { echo "$((base_port + 20 + $1))"; }

# Every process of the cluster by name: etcd first, the controller last.
process_names() {
    echo etcd
    local replica_number
    for ((replica_number = 0; replica_number < replica_count; replica_number++)); do
        echo "replica-${replica_number}"
    done
    echo controller
}

# The port a process answers its readiness probe on, and the path to ask.
probe_port() {
    case "$1" in
        etcd) echo "${etcd_client_port}" ;;
        controller) echo "${controller_port}" ;;
        replica-*) replica_probe_port "${1#replica-}" ;;
    esac
}

probe_path() {
    case "$1" in
        etcd) echo /health ;;
        *) echo /readyz ;;
    esac
}

pid_file() { echo "${state}/$1/pid"; }
log_file() { echo "${state}/$1/log"; }

# The process id recorded for a process, or nothing.
recorded_pid() {
    local file
    file="$(pid_file "$1")"
    if [[ -f "${file}" ]]; then
        cat "${file}"
    fi
}

# Whether a process id belongs to a process that still runs. A process that
# has exited but was not collected yet does not count.
is_alive() {
    local pid="$1" process_state
    kill -0 "${pid}" 2>/dev/null || return 1
    if command -v ps >/dev/null; then
        process_state="$(ps -o stat= -p "${pid}" 2>/dev/null || true)"
        case "${process_state}" in
            Z*) return 1 ;;
        esac
    fi
    return 0
}

is_running() {
    local pid
    pid="$(recorded_pid "$1")"
    [[ -n "${pid}" ]] && is_alive "${pid}"
}

# The status code of an HTTP GET on a local port, or nothing if nobody
# answers. Bash opens the connection itself, so no HTTP client is needed. It
# does so in a subshell, because a connection that is refused ends the shell
# that tried to open it.
http_status() {
    local port="$1" path="$2"
    (
        exec 3<>"/dev/tcp/127.0.0.1/${port}"
        printf 'GET %s HTTP/1.0\r\nHost: 127.0.0.1\r\n\r\n' "${path}" >&3
        IFS= read -r status_line <&3
        status_line="${status_line#HTTP/* }"
        echo "${status_line%% *}"
    ) 2>/dev/null || true
}

is_ready() {
    [[ "$(http_status "$(probe_port "$1")" "$(probe_path "$1")")" == 200 ]]
}

# Waits until a process answers its readiness probe. Fails, with the end of
# its log, if the process exits or the allowance runs out first.
wait_until_ready() {
    local name="$1" deadline=$((SECONDS + start_allowance_seconds))
    until is_ready "${name}"; do
        if ! is_running "${name}"; then
            tail -n 20 "$(log_file "${name}")" >&2 || true
            fail "${name} exited while starting; its log is $(log_file "${name}")"
        fi
        if ((SECONDS >= deadline)); then
            tail -n 20 "$(log_file "${name}")" >&2 || true
            fail "${name} was not ready within ${start_allowance_seconds}s; its log is $(log_file "${name}")"
        fi
        sleep 0.1
    done
}

# Starts a program in the background with its output in the process's log,
# and records its process id.
launch() {
    local name="$1"
    shift
    nohup "$@" >>"$(log_file "${name}")" 2>&1 </dev/null &
    echo "$!" >"$(pid_file "${name}")"
}

write_cluster_table() {
    cat <<EOF
etcd_endpoints = ["${etcd_endpoint}"]
etcd_key_prefix = "/bitacora/"
EOF
}

write_object_store_table() {
    cat <<EOF
kind = "local_directory"
path = "${state}/objects"
EOF
}

# The configuration bita and applications use to find the cluster.
write_client_config() {
    {
        echo "# Written by development/cluster.sh. Use it with: bita --config <this file>"
        write_cluster_table
        echo
        echo "[object_store]"
        write_object_store_table
    } >"${state}/cluster.toml"
}

# A developer's disk is often fuller than a server's, so the thresholds that
# make a replica refuse segments are switched off here.
write_replica_config() {
    local replica_number="$1" name="replica-$1"
    cat >"${state}/${name}/replica.toml" <<EOF
# Written by development/cluster.sh.
replica_id = "${name}"
data_directory = "${state}/${name}/data"
listen_address = "127.0.0.1:$(replica_grpc_port "${replica_number}")"
advertised_address = "127.0.0.1:$(replica_grpc_port "${replica_number}")"
failure_domains = ["development-host-${replica_number}"]
metrics_listen_address = "127.0.0.1:$(replica_probe_port "${replica_number}")"
registration_interval_seconds = 1

[cluster]
$(write_cluster_table)

[cluster.object_store]
$(write_object_store_table)

[log]
level = "info"
format = "text"

[storage]
preallocated_segment_bytes = 1048576
refuse_new_segments_at_used_percent = 100
refuse_appends_at_used_percent = 100
EOF
}

# Short delays, so that tiering and repair can be watched without waiting for
# the hours a real cluster takes.
write_controller_config() {
    cat >"${state}/controller/controller.toml" <<EOF
# Written by development/cluster.sh.
identity = "controller"
metrics_listen_address = "127.0.0.1:${controller_port}"
pass_interval_milliseconds = 500
replica_lost_after_seconds = 60
tiering_delay_seconds = 30
hot_read_window_seconds = 120

[placement]
liveness_window_seconds = 5
refuse_at_used_percent = 100

[cluster]
$(write_cluster_table)

[cluster.object_store]
$(write_object_store_table)

[log]
level = "info"
format = "text"
EOF
}

start_etcd() {
    is_running etcd && return 0
    launch etcd "${etcd_binary}" \
        --name development \
        --data-dir "${state}/etcd/data" \
        --listen-client-urls "${etcd_endpoint}" \
        --advertise-client-urls "${etcd_endpoint}" \
        --listen-peer-urls "http://127.0.0.1:${etcd_peer_port}" \
        --initial-advertise-peer-urls "http://127.0.0.1:${etcd_peer_port}" \
        --initial-cluster "development=http://127.0.0.1:${etcd_peer_port}" \
        --log-level warn
}

start_replica() {
    local replica_number="$1" name="replica-$1"
    is_running "${name}" && return 0
    write_replica_config "${replica_number}"
    "${replica_binary}" --config "${state}/${name}/replica.toml" --check-config >/dev/null
    launch "${name}" "${replica_binary}" --config "${state}/${name}/replica.toml"
}

start_controller() {
    is_running controller && return 0
    write_controller_config
    "${controller_binary}" --config "${state}/controller/controller.toml" --check-config >/dev/null
    launch controller "${controller_binary}" --config "${state}/controller/controller.toml"
}

command_start() {
    local name replica_number
    mkdir -p "${state}/objects"
    touch "${state}/${state_marker}"
    while IFS= read -r name; do
        mkdir -p "${state}/${name}"
    done < <(process_names)
    write_client_config

    # etcd first: the daemons are not ready until they have reached it.
    start_etcd
    wait_until_ready etcd
    for ((replica_number = 0; replica_number < replica_count; replica_number++)); do
        start_replica "${replica_number}"
    done
    start_controller
    while IFS= read -r name; do
        wait_until_ready "${name}"
    done < <(process_names)

    echo "The cluster is ready: etcd, ${replica_count} replicas and a controller."
    echo "State, logs and configuration: ${state}"
    echo "Use it:   development/cluster.sh bita replica list"
    echo "Directly: ${bita_binary} --config ${state}/cluster.toml replica list"
}

# Asks a process to stop and waits until it has. A process that ignores the
# request for too long is killed, and the caller is told.
stop_process() {
    local name="$1" pid deadline
    pid="$(recorded_pid "${name}")"
    if [[ -z "${pid}" ]]; then
        return 0
    fi
    if is_alive "${pid}"; then
        kill -TERM "${pid}" 2>/dev/null || true
        deadline=$((SECONDS + stop_allowance_seconds))
        while is_alive "${pid}"; do
            if ((SECONDS >= deadline)); then
                kill -KILL "${pid}" 2>/dev/null || true
                echo "cluster.sh: ${name} did not stop within ${stop_allowance_seconds}s and was killed" >&2
                break
            fi
            sleep 0.1
        done
    fi
    rm -f "$(pid_file "${name}")"
}

command_stop() {
    local replica_number
    [[ -d "${state}" ]] || return 0
    # The reverse of the start: etcd goes last, so that the daemons can still
    # reach it while they shut down.
    stop_process controller
    for ((replica_number = replica_count - 1; replica_number >= 0; replica_number--)); do
        stop_process "replica-${replica_number}"
    done
    stop_process etcd
    echo "The cluster is stopped. Its state is kept in ${state}"
}

command_status() {
    local name pid condition
    printf '%-12s %-8s %-8s %-7s %s\n' PROCESS STATE PID PORT LOG
    while IFS= read -r name; do
        pid="$(recorded_pid "${name}")"
        if [[ -n "${pid}" ]] && is_alive "${pid}"; then
            if is_ready "${name}"; then
                condition=ready
            else
                condition=starting
            fi
        else
            condition=stopped
            pid=-
        fi
        printf '%-12s %-8s %-8s %-7s %s\n' "${name}" "${condition}" "${pid}" \
            "$(probe_port "${name}")" "$(log_file "${name}")"
    done < <(process_names)
}

command_logs() {
    local follow=no name
    if [[ "${1:-}" == -f ]]; then
        follow=yes
        shift
    fi
    name="${1:-}"
    [[ -n "${name}" ]] || fail "logs needs the name of a process: $(process_names | tr '\n' ' ')"
    process_names | grep -qx -- "${name}" || fail "no process is called ${name}: $(process_names | tr '\n' ' ')"
    [[ -f "$(log_file "${name}")" ]] || fail "${name} has not written a log yet"
    if [[ "${follow}" == yes ]]; then
        exec tail -n 100 -f "$(log_file "${name}")"
    fi
    cat "$(log_file "${name}")"
}

command_bita() {
    [[ -f "${state}/cluster.toml" ]] || fail "there is no cluster in ${state}; run: development/cluster.sh start"
    exec "${bita_binary}" --config "${state}/cluster.toml" "$@"
}

command_reset() {
    [[ -e "${state}" ]] || return 0
    [[ -f "${state}/${state_marker}" ]] ||
        fail "${state} is not the state of a development cluster; nothing was deleted"
    command_stop >/dev/null
    rm -rf "${state}"
    echo "The cluster and its state in ${state} are gone."
}

command="${1:-help}"
if (($# > 0)); then
    shift
fi
case "${command}" in
    start) command_start ;;
    stop) command_stop ;;
    status) command_status ;;
    logs) command_logs "$@" ;;
    bita) command_bita "$@" ;;
    reset) command_reset ;;
    help | -h | --help) usage ;;
    *)
        usage >&2
        fail "unknown command: ${command}"
        ;;
esac

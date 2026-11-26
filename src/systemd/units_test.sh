#!/usr/bin/env bash
# Tests the systemd units against what they are installed with: unpacks the
# package and checks that each unit is well formed, starts a program that
# exists with options that program has, names a configuration the program
# accepts, runs as a user the package creates, and can write where the
# configuration tells the daemon to write.
#
# Where systemd is installed, `systemd-analyze verify` also checks every
# unit. Everything else runs on any machine.
#
# Usage: units_test.sh <package.tar.gz> [<replacement unit>]
#
# A replacement unit takes the place of the packaged unit of the same name
# before anything is checked. The test of this test uses it to hand in a
# unit with known mistakes.
set -euo pipefail

package="${1:?usage: units_test.sh <package.tar.gz> [<replacement unit>]}"
replacement_unit="${2:-}"
root="${TEST_TMPDIR:?the test needs a temporary directory}/root"

readonly units_directory="usr/lib/systemd/system"
readonly sysusers_file="usr/lib/sysusers.d/bitacora.conf"

rm -rf "${root}"
mkdir -p "${root}"
tar -C "${root}" -xzf "${package}"
if [[ -n "${replacement_unit}" ]]; then
    cp "${replacement_unit}" "${root}/${units_directory}/$(basename "${replacement_unit}")"
fi

# The directives these units may use. A directive that is not listed fails
# the test, which is what catches a misspelled one: systemd itself only logs
# a warning and carries on without the protection the line was meant to give.
# Add a directive here when a unit starts to use it.
readonly known_unit_keys="Description Documentation Wants After"
readonly known_service_keys="Type User Group ExecStart EnvironmentFile Restart RestartSec
    RestartPreventExitStatus KillSignal TimeoutStopSec LimitNOFILE StateDirectory StateDirectoryMode
    UMask NoNewPrivileges ProtectSystem ProtectHome PrivateTmp PrivateDevices ProtectKernelTunables
    ProtectKernelModules ProtectKernelLogs ProtectControlGroups ProtectClock ProtectHostname
    RestrictAddressFamilies RestrictNamespaces RestrictRealtime RestrictSUIDSGID LockPersonality
    MemoryDenyWriteExecute CapabilityBoundingSet AmbientCapabilities SystemCallArchitectures
    SystemCallFilter"
readonly known_install_keys="WantedBy"

# The daemons give themselves this long to shut down; the units must wait longer.
readonly daemon_shutdown_seconds=10

failures=0
current_unit=""

problem() {
    echo "FAILED ${current_unit}: $*" >&2
    failures=$((failures + 1))
}

# Whether a word is in a list of words.
is_listed() {
    local word="$1" candidate
    shift
    for candidate in "$@"; do
        [[ "${candidate}" == "${word}" ]] && return 0
    done
    return 1
}

# The values of a key in a section of the unit under test, one per line.
values_of() {
    local unit_file="$1" wanted_section="$2" wanted_key="$3" line section=""
    while IFS= read -r line; do
        case "${line}" in
            "["*"]") section="${line}" ;;
            "${wanted_key}="*)
                if [[ "${section}" == "[${wanted_section}]" ]]; then
                    echo "${line#*=}"
                fi
                ;;
        esac
    done <"${unit_file}"
}

# The single value of a key, or nothing.
value_of() {
    values_of "$@" | tail -n 1
}

# Checks sections and keys: only known ones, every line a comment, a section
# header or a `Key=value`.
check_syntax() {
    local unit_file="$1" line section="" key known_keys
    while IFS= read -r line; do
        case "${line}" in
            "" | "#"*) continue ;;
            "[Unit]" | "[Service]" | "[Install]")
                section="${line}"
                continue
                ;;
            "["*) problem "unknown section ${line}" ;;
        esac
        if [[ "${line}" != *=* ]]; then
            problem "line is neither a comment, a section nor Key=value: ${line}"
            continue
        fi
        key="${line%%=*}"
        case "${section}" in
            "[Unit]") known_keys="${known_unit_keys}" ;;
            "[Service]") known_keys="${known_service_keys}" ;;
            "[Install]") known_keys="${known_install_keys}" ;;
            *)
                problem "${key} comes before any section"
                continue
                ;;
        esac
        # shellcheck disable=SC2086 # The list is meant to be split into words.
        is_listed "${key}" ${known_keys} || problem "unknown directive ${key} in ${section}"
    done <"${unit_file}"
}

# Checks the command a unit starts: the program is in the package, every
# option it is given is one the program knows, and the configuration file it
# names has an example in the package that the program accepts. The package
# installs `<file>.example`, never the file itself, which belongs to the
# operator.
check_command() {
    local unit_file="$1" command program config_path="" help word expect_config=no
    command="$(value_of "${unit_file}" Service ExecStart)"
    if [[ -z "${command}" ]]; then
        problem "no ExecStart"
        return
    fi
    program="${command%% *}"
    if [[ ! -x "${root}${program}" ]]; then
        problem "ExecStart runs ${program}, which the package does not install"
        return
    fi
    help="$("${root}${program}" --help)"
    # shellcheck disable=SC2086 # The command line is meant to be split into words.
    for word in ${command#* }; do
        if [[ "${expect_config}" == yes ]]; then
            config_path="${word}"
            expect_config=no
            continue
        fi
        case "${word}" in
            --config) expect_config=yes ;;
            --*)
                [[ "${help}" == *"${word}"* ]] || problem "${program} has no option ${word}"
                ;;
            *) problem "ExecStart has an argument that belongs to no option: ${word}" ;;
        esac
    done
    [[ "${help}" == *"--config"* ]] || problem "${program} has no option --config"
    if [[ -z "${config_path}" ]]; then
        problem "ExecStart names no configuration file"
        return
    fi
    if [[ -e "${root}${config_path}" ]]; then
        problem "the package installs ${config_path}, which would overwrite the operator's configuration"
    fi
    if [[ ! -f "${root}${config_path}.example" ]]; then
        problem "ExecStart names ${config_path}, and the package installs no ${config_path}.example"
        return
    fi
    "${root}${program}" --config "${root}${config_path}.example" --check-config >/dev/null ||
        problem "${program} refuses the example configuration the package installs for ${config_path}"
    check_writable_paths "${unit_file}" "${root}${config_path}.example"
}

# Checks that every directory the installed configuration tells the daemon
# to write to lies in a directory the unit lets it write to. With
# ProtectSystem=strict everything else is read-only.
check_writable_paths() {
    local unit_file="$1" config_file="$2" state_directory writable_root path
    state_directory="$(value_of "${unit_file}" Service StateDirectory)"
    if [[ -z "${state_directory}" ]]; then
        problem "no StateDirectory"
        return
    fi
    writable_root="/var/lib/${state_directory}"
    while IFS= read -r path; do
        case "${path}" in
            "${writable_root}" | "${writable_root}/"*) ;;
            *) problem "the installed configuration writes to ${path}, outside ${writable_root}" ;;
        esac
    done < <(sed -n -E 's/^(data_directory|path) = "(.*)"$/\2/p' "${config_file}")
}

# Checks the account the unit runs as: named, not root, and created by the
# package.
check_user() {
    local unit_file="$1" user
    user="$(value_of "${unit_file}" Service User)"
    case "${user}" in
        "") problem "no User: the daemon would run as root" ;;
        root) problem "the daemon runs as root" ;;
        *)
            grep -q -E "^u[[:space:]]+${user}[[:space:]]" "${root}/${sysusers_file}" ||
                problem "user ${user} is not created by ${sysusers_file}"
            ;;
    esac
    [[ "$(value_of "${unit_file}" Service Group)" == "${user}" ]] ||
        problem "Group is not the group of user ${user}"
}

# Checks how the unit reacts to the daemon's exit codes: 1 restarts, 2 (a
# wrong configuration) does not, and a stop is given longer than the daemon
# takes to shut down.
check_supervision() {
    local unit_file="$1" stop_timeout
    [[ "$(value_of "${unit_file}" Service Restart)" == on-failure ]] ||
        problem "Restart is not on-failure"
    [[ " $(value_of "${unit_file}" Service RestartPreventExitStatus) " == *" 2 "* ]] ||
        problem "exit code 2, a wrong configuration, would be restarted for ever"
    stop_timeout="$(value_of "${unit_file}" Service TimeoutStopSec)"
    stop_timeout="${stop_timeout%s}"
    if [[ ! "${stop_timeout}" =~ ^[0-9]+$ ]]; then
        problem "TimeoutStopSec is not a number of seconds"
    elif ((stop_timeout <= daemon_shutdown_seconds)); then
        problem "TimeoutStopSec is not longer than the ${daemon_shutdown_seconds}s the daemon takes to shut down"
    fi
    [[ -n "$(value_of "${unit_file}" Service LimitNOFILE)" ]] || problem "no LimitNOFILE"
    [[ "$(value_of "${unit_file}" Install WantedBy)" == multi-user.target ]] ||
        problem "the unit cannot be enabled: no WantedBy=multi-user.target"
}

# Checks that the protections the units are meant to have are switched on.
check_hardening() {
    local unit_file="$1" directive
    for directive in NoNewPrivileges ProtectHome PrivateTmp PrivateDevices ProtectKernelTunables \
        ProtectKernelModules ProtectControlGroups RestrictSUIDSGID LockPersonality; do
        [[ "$(value_of "${unit_file}" Service "${directive}")" == yes ]] ||
            problem "${directive} is not switched on"
    done
    [[ "$(value_of "${unit_file}" Service ProtectSystem)" == strict ]] ||
        problem "ProtectSystem is not strict"
    [[ -z "$(value_of "${unit_file}" Service CapabilityBoundingSet)" ]] ||
        problem "the daemon keeps capabilities it does not need"
}

# Lets systemd itself judge the unit, where systemd is installed.
#
# systemd looks for the program a unit starts under /, where the test has not
# installed it, so it is shown a copy of the unit that starts the unpacked
# program. Everything else in the copy is the unit as packaged. systemd only
# warns about a directive or a value it does not know and still exits with 0,
# so anything it says about the unit counts as a problem.
check_with_systemd() {
    local unit_file="$1" unit_name scratch verdict about_the_unit
    command -v systemd-analyze >/dev/null || return 0
    unit_name="$(basename "${unit_file}")"
    scratch="${TEST_TMPDIR}/systemd-verify"
    mkdir -p "${scratch}"
    sed "s|^ExecStart=/|ExecStart=${root}/|" "${unit_file}" >"${scratch}/${unit_name}"
    verdict="$(systemd-analyze verify "${scratch}/${unit_name}" 2>&1 || true)"
    about_the_unit="$(printf '%s\n' "${verdict}" | grep -F -- "${unit_name}" || true)"
    if [[ -n "${about_the_unit}" ]]; then
        problem "systemd-analyze verify complains: ${about_the_unit}"
    fi
}

shopt -s nullglob
unit_files=("${root}/${units_directory}"/*.service)
if ((${#unit_files[@]} != 2)); then
    echo "FAILED: expected the units of the two daemons in ${units_directory}, found ${#unit_files[@]}" >&2
    exit 1
fi
[[ -f "${root}/${sysusers_file}" ]] || {
    echo "FAILED: the package does not install ${sysusers_file}" >&2
    exit 1
}

for unit_file in "${unit_files[@]}"; do
    current_unit="$(basename "${unit_file}")"
    check_syntax "${unit_file}"
    check_command "${unit_file}"
    check_user "${unit_file}"
    check_supervision "${unit_file}"
    check_hardening "${unit_file}"
    check_with_systemd "${unit_file}"
done

if ((failures > 0)); then
    echo "${failures} problems found" >&2
    exit 1
fi
if command -v systemd-analyze >/dev/null; then
    echo "PASSED, with systemd-analyze"
else
    echo "PASSED, without systemd-analyze: it is not installed here"
fi

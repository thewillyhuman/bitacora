#!/usr/bin/env bash
# Builds the archive that installs bitacora on a machine with systemd: the
# programs, the units, the user and the example configuration files, laid out
# as they go under /.
#
# Usage: package.sh <output.tar.gz> <mode>:<source>=<path in the archive>...
#
# Each further argument installs one file: `0755:bazel-bin/x=usr/bin/x` puts
# the file bazel-bin/x at usr/bin/x with mode 0755. Bazel calls this script
# from //src/systemd:package; nothing else needs to.
set -euo pipefail

if (($# < 2)); then
    sed -n '2,/^set -euo/{/^set -euo/d;s/^# \{0,1\}//;p;}' "${BASH_SOURCE[0]}" >&2
    exit 2
fi

output="$1"
shift

staging="$(mktemp -d)"
trap 'rm -rf "${staging}"' EXIT

for installation in "$@"; do
    mode="${installation%%:*}"
    source_and_target="${installation#*:}"
    source="${source_and_target%%=*}"
    target="${source_and_target#*=}"
    case "${target}" in
        /* | *..*)
            echo "package.sh: ${target} must be a path inside the archive" >&2
            exit 2
            ;;
    esac
    mkdir -p "${staging}/$(dirname "${target}")"
    # Bazel hands out symbolic links; the archive must hold the files.
    cp -L "${source}" "${staging}/${target}"
    chmod "${mode}" "${staging}/${target}"
done

# The archive is unpacked at / by root, so it must not carry anything of the
# machine it was built on: only files, no directories (unpacking must not
# change the mode or owner of /usr or /etc), and every file owned by root.
file_list="${staging}.files"
trap 'rm -rf "${staging}" "${file_list}"' EXIT
(cd "${staging}" && find . -type f | sed 's|^\./||' | LC_ALL=C sort) >"${file_list}"
tar_version="$(tar --version 2>/dev/null || true)"
if [[ "${tar_version}" == *"GNU tar"* ]]; then
    root_owner=(--owner=0 --group=0 --numeric-owner)
else
    root_owner=(--uid 0 --gid 0 --uname root --gname root)
fi
tar -C "${staging}" "${root_owner[@]}" -czf "${output}" -T "${file_list}"

#!/usr/bin/env bash
# Checks docs/README.md, the index of the documentation: it lists every
# decision record, and every file it links to exists. An index that is
# written by hand falls behind unless something compares it with the files.
set -euo pipefail

readonly index="docs/README.md"
failures=0

fail() {
    echo "FAIL: $*" >&2
    failures=$((failures + 1))
}

record_count=0
for record in docs/decisions/[0-9]*.md; do
    record_count=$((record_count + 1))
    link="decisions/$(basename "${record}")"
    grep --quiet --fixed-strings "(${link})" "${index}" ||
        fail "${index} does not list ${link}"
done
# A pattern that matches nothing stays as it is, and the loop above would
# then pass without having looked at a single record.
[ "${record_count}" -ge 16 ] || fail "expected the decision records next to the index, found ${record_count}"

# Links to other sites are not followed: the test must not need a network.
links="$(grep --only-matching --extended-regexp '\]\([^)#]+' "${index}" | sed 's/^](//' | grep --invert-match '://')"
for link in ${links}; do
    [ -e "docs/${link}" ] || fail "${index} links to ${link}, which does not exist"
done

if [ "${failures}" -gt 0 ]; then
    echo "${failures} problem(s) in ${index}" >&2
    exit 1
fi
echo "the index lists ${record_count} decision records and every link resolves"

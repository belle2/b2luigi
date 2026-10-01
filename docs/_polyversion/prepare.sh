#!/bin/bash
# Prepare the docs source directory of a checked-out revision for a versioned build.
#
# Called by docs/poly.py before sphinx-build with the source directory as
# argument. Revisions that predate the versioned documentation get the version
# switcher configuration appended to their conf.py and receive the static files
# and templates it refers to.
set -euo pipefail

src="$1"
here="$(cd "$(dirname "$0")" && pwd)"

if ! grep -q 'POLYVERSION_SWITCHER' "${src}/conf.py"; then
    cat "${here}/switcher_conf.py" >> "${src}/conf.py"
fi
mkdir -p "${src}/_polyversion"
cp -R "${here}/static" "${here}/sphinx_templates" "${src}/_polyversion/"

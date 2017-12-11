#!/bin/bash
set -o errexit
set -o nounset
set -o pipefail

glide update --strip-vendor

git add vendor glide.yaml

echo "Vendoring complete. Next steps:

1. Use 'git status' to make sure the change list looks reasonable.
2. Run 'make' to verify everything still compiles.
3. Run tests to verify everything still works.
4. Commit the changes to 'vendor' and 'glide.yaml' in a single commit.
"

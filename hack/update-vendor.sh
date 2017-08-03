#!/bin/bash
set -o errexit
set -o nounset
set -o pipefail

# Clear out the existing component vendor directory.
if [ -d "vendor" ]; then
    git rm -qrf vendor
fi

# Fetch without stripping vendor directories so that we get Origin and its own
# vendored dependencies as well as our component dependencies.
glide up --no-recursive

# Set up a working directory.
TMPDIR=$(mktemp -d)

# The general steps to make this work are:
#
# 1. Move Origin's vendored deps into place as the baseline dependency tree.
# 2. Move Origin itself into place as a vendored dependency.
# 3. Delete any of Origin's vendored dependencies that conflict with ours.
# 4. Vendor our explicitly declared dependencies.
#
# The resulting tree should replace the current vendor directory.

mv vendor/github.com/openshift/origin/vendor/* $TMPDIR
rm -r vendor/github.com/openshift/origin/vendor
mkdir -p $TMPDIR/github.com/openshift
mv vendor/github.com/openshift/origin $TMPDIR/github.com/openshift

PKG_PATTERN="- package: (.*)"
while IFS='' read -r LINE || [[ -n "$LINE" ]]; do
  if [[ "$LINE" =~ $PKG_PATTERN ]]; then
    PKG="${BASH_REMATCH[1]}"

    # Ignore origin itself.
    [ "$PKG" == "github.com/openshift/origin" ] && continue

    CONFLICT="$TMPDIR/$PKG"
    if [ -d "$CONFLICT" ]; then
      echo "Deleting conflicting Origin dependency $CONFLICT"
      rm -rf "$CONFLICT"
    fi
  fi
done < glide.yaml

cp -R vendor/* $TMPDIR

# Remove any nested vendor directories that may have appeared.
find $TMPDIR -mindepth 1 -type d -name vendor -exec rm -rf {} \; || :

# Delete the component vendor directory to make way for the merged version.
rm -rf vendor

# Copy the merged vendor directory back into place.
cp -R $TMPDIR vendor

# Clean up the temp dir.
rm -rf $TMPDIR

git add vendor glide.yaml

echo "Vendoring complete. Next steps:

1. Use 'git status' to make sure the change list looks reasonable.
2. Run 'make' to verify everything still compiles.
3. Run tests to verify everything still works.
4. Commit the changes to 'vendor' and 'glide.yaml' in a single commit.
"

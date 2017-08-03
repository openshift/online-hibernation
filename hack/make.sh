#!/bin/bash

set -euo pipefail

TARGET="$1"
shift
STARTTIME=$(date +%s)

EXITCODE=0

for PROJECT
do
  target_exists=1
  if [[ "$(make -C "$PROJECT" --question "$TARGET" >& /dev/null; echo $?)" != 2 ]]; then
      testexec="make $TARGET -k"
  else
      target_exists=0
  fi
  echo
  echo "*****************************************************************************************"
  echo "Executing $PROJECT target $TARGET..."
  echo
  pushd $PROJECT >/dev/null
  if [ $target_exists -eq 1 ]; then
    if $testexec; then
      echo "$PROJECT target $TARGET PASS"
    else
      echo "$PROJECT target $TARGET FAIL"
      EXITCODE=1
    fi
  else
    echo "Directory $PROJECT does not have target $TARGET, skipping."
  fi
  popd >/dev/null
done

ENDTIME=$(date +%s); echo "$0 took $(($ENDTIME - $STARTTIME)) seconds"
exit $EXITCODE


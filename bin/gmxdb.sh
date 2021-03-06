#!/bin/bash

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  TARGET="$(readlink "${SOURCE}")"
  if [[ $TARGET == /* ]]; then
    SOURCE="${TARGET}"
  else
    DIR="$( dirname "${SOURCE}" )"
    SOURCE="$DIR/$TARGET" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
  fi
done
DIR="$( dirname "${SOURCE}" )"

MODULEPATH=$(realpath "${DIR}/../")


export PYTHONPATH="${PYTHONPATH}:${MODULEPATH}"



cmd="python ${DIR}/gmxdb.py ${*}"
eval $cmd
#!/bin/bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
# LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so
OMP_CANCELLATION=TRUE OMP_NESTED=TRUE OMP_DYNAMIC=TRUE ${DIR}/build/sigmod-2018

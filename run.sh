#!/bin/bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
# LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so
${DIR}/build/sigmod-2018

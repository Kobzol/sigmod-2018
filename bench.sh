#!/usr/bin/env bash

pushd reference/small_workload
time -p ../../cmake-build-debug/sigmod-2018 < small.complete > output.txt && diff output.txt small.result
popd

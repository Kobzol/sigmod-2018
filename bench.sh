#!/usr/bin/env bash

pushd reference/workloads/public
time -p ../../../cmake-build-release/reference/harness public.init public.work public.result ../../../cmake-build-release/sigmod-2018
popd

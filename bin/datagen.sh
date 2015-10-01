#!/bin/bash

set -vx


hadoop jar ./HiveTestDataGenerator-1.0-SNAPSHOT.jar -Dmapreduce.task.timeout=6000000 -q s3n://path/creation_query.sql "$@"

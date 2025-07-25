#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#suite:tools

set -u -o pipefail

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR

export SECURITY_ENABLED=true
export OM_SERVICE_ID=omservice
export SCM=scm1.org
export OM=om1
export COMPOSE_FILE=docker-compose.yaml:debug-tools.yaml
export OZONE_DIR=/opt/hadoop

: "${OZONE_VOLUME:="${COMPOSE_DIR}/data"}"

export OZONE_VOLUME

# shellcheck source=/dev/null
source "$COMPOSE_DIR/../testlib.sh"

create_data_dirs dn{1..5} kms om{1..3} recon s3g scm{1..3}

start_docker_env

execute_robot_test ${OM} kinit.robot

echo "Creating test keys to verify om compaction"
om_container="ozonesecure-ha-om1-1"
docker exec "${om_container}" ozone freon ockg -n 100000 -t 20 -s 0 > /dev/null 2>&1
echo "Test keys created"

echo "Restarting OM after key creation to flush and generate sst files"
docker restart "${om_container}"

execute_robot_test ${OM} repair/om-compact.robot

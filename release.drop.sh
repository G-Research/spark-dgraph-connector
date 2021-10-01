#!/bin/bash
#
# Copyright 2020 G-Research
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Script to drop a release, see RELEASE.md for details

set -e -o pipefail

mvn nexus-staging:drop
echo

echo "Now delete the release commit and its tag:"
echo "git tag -d vX.Y.Z_spark-N.M"
echo "git reset --hard HEAD^  ## deletes latest commit, not recoverable"

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

# Script to prepare release, see RELEASE.md for details

set -e -o pipefail

# check for clean git status (except for CHANGELOG.md)
readarray -t git_status < <(git status -s --untracked-files=no 2>/dev/null | grep -v " CHANGELOG.md$")
if [ ${#git_status[@]} -gt 0 ]
then
  echo "There are pending git changes:"
  for (( i=0; i<${#git_status[@]}; i++ )); do echo "${git_status[$i]}" ; done
  exit 1
fi

# check for unreleased entry in CHANGELOG.md
readarray -t changes < <(grep -A 100 "^## \[UNRELEASED\] - YYYY-MM-DD" CHANGELOG.md | grep -B 100 --max-count=1 -E "^## \[[0-9.]+\]" | grep "^-")
if [ ${#changes[@]} -eq 0 ]
then
  echo "Did not find any changes in CHANGELOG.md under '## [UNRELEASED] - YYYY-MM-DD'"
  exit 1
fi

# get latest, release and Spark version
latest=$(grep --max-count=1 "<version>.*</version>" README.md | sed -E -e "s/\s*<[^>]+>//g" -e "s/-[0-9.]+//g")
version=$(grep --max-count=1 "<version>.*</version>" pom.xml | sed -E -e "s/\s*<[^>]+>//g" -e "s/-SNAPSHOT//" -e "s/-[0-9.]+//g")
spark=$(git rev-parse --abbrev-ref HEAD | sed -e "s/^spark-//" -e "s/_.*//")

echo "Releasing ${#changes[@]} changes as version $version:"
for (( i=0; i<${#changes[@]}; i++ )); do echo "${changes[$i]}" ; done

sed -i "s/## \[UNRELEASED\] - YYYY-MM-DD/## [$version] - $(date +%Y-%m-%d)/" CHANGELOG.md
sed -i "s/-SNAPSHOT//g" pom.xml examples/scala/pom.xml
sed -i "s/$latest-/$version-/g" README.md

# commit changes to local repo
echo
echo "Committing release to local git"
git commit -a -m "Releasing $version"
git tag -a "v${version}_spark-$spark" -m "Release v${version}"
echo

echo "Creating release package, please inspect git changes in the meantime: git show HEAD"
sleep 10
echo

# create release
mvn clean deploy -Dsign
echo

echo "Release package created, please test it, see RELEASE.md for details"
echo "Release the package with release.release.sh or drop it with release.drop.sh"

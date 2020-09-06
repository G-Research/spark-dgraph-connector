# Releasing Spark Dgraph Connector

This provides instructions on how to release a version of `spark-dgraph-connector`.

## Releasing from default branch

Follow this procedure to release a new version from default branch `spark-3.0`:

- Add a new entry to `CHANGELOG.md` listing all notable changes of this release.
  Use the heading `## [VERSION] - YYYY-MM-dd`, e.g. `## [1.1.0] - 2020-06-09`.
  No need to mention the branch as `CHANGELOG.md` is branch-specific.
- Remove the `-SNAPSHOT` suffix from `<version>` in the [`pom.xml`](pom.xml) file, e.g. `1.1.0-3.0-SNAPSHOT` → `1.1.0-3.0`.
- Update the versions in the `README.md` file to the version of your `pom.xml` to reflect the latest version,
  e.g. replace all `1.0.0-3.0` with `1.1.0-3.0` and `1.0.0-2.4` with `1.1.0-2.4`, respectively.
  All these changes should occur in the `Using Spark Dgraph Connector` section and its subsections.
- Commit the change to your local git repository, use a commit message like `Releasing 1.1.0`. Do not push to github yet.
- Tag that commit with a version tag like `v1.1.0_spark-3.0` and message like `Release v1.1.0`. Do not push to github yet.
- Release the version with `mvn clean deploy`. This will be put into a staging repository and not automatically released (due to `<autoReleaseAfterClose>false</autoReleaseAfterClose>` in your [`pom.xml`](pom.xml) file).
- Inspect and test the staged version. Use `spark-examples` for that. If you are happy with everything:
  - Push the commit and tag to origin.
  - Release the package with `mvn nexus-staging:release`.
  - Bump the version to the next [minor version](https://semver.org/) in `pom.xml` and append the `-SNAPSHOT` suffix again, e.g. `1.1.0-3.0` → `1.2.0-3.0-SNAPSHOT`.
  - Commit this change to your local git repository, use a commit message like `Post-release version bump to 1.2.0`.
  - Push all local commits to origin.
- Otherwise drop it with `mvn nexus-staging:drop`. Remove the last two commits from your local history.

After successfully releasing default branch `spark-3.0`, consider releasing backport branches like `spark-2.4`.

## Releasing from backport branches

Compare the backport branch (e.g. `spark-2.4`) against the default branch `spark-3.0`. Use the following script:

    ./git-compare-logs-2.4.sh

This script shows the commits that are in one branch but not the other, together with all commits
since `spark-2.4` branched off `spark-3.0`. It is important not to change the commit message when
merging (cherry-pick) one commit from one branch to the other and not to enhance a commit.

Once the backport branch is in sync (feature wise) with the default branch, repeat above release process
for the backport branch with the same version, e.g. version `1.1.0-2.4` and tag `v1.1.0_spark-2.4`.
Minor versions need to be in sync across all release branches (same feature set), whereas patch versions may differ.

### Backporting commits from default branch

For each commit that you want to backport (here to `spark-2.4`) perform the following steps:

Identify the commit that you want to backport:

    ./git-compare-logs-2.4.sh
    ===== missing commits =====
    spark-2.4: Improved example code snippets in README.md (spark-3.0) (vor 18 Minuten)
    spark-3.0: Backporting to Spark 2.4

    ===== spark-3.0 =====
    * acd8c55 - Improved example code snippets in README.md (spark-3.0) (vor 18 Minuten)
    * d7b4eb9 - Add note on spark branch versions to README (vor 18 Minuten)
    * 3de809c - Add wide node mode to NodeSource (vor 18 Minuten)

    ===== spark-2.4 =====
    * c52a479 - Add note on spark branch versions to README (vor 17 Minuten)
    * b3337b6 - Add wide node mode to NodeSource (vor 17 Minuten)
    * a47b558 - Backporting to Spark 2.4 (origin/spark-2.4) (vor 14 Stunden)

As an example, we want to backport commit `acd8c55` to `spark-2.4`.

    git checkout spark-2.4
    git cherry-pick acd8c55
    # resolve any conflicts
    # add resolved files with 'git add'
    # finish with 'git cherry-pick --continue'
    git show HEAD

Review the actual changes of the commit and look out for any backport related changes that need adjustment.
Also run unit tests. If you have changes (only backporting changes, no new features or enhancements) commit
them with `git commit --amend` to add them to the cherry-picked commit.

Finally, push your changes to origin.

## Releasing a bug-fix version

A bug-fix version needs to be released from a [minor-version branch](https://semver.org/), e.g. `spark-3.0_v1.1`.

### Create a bug-fix branch

If there is no bug-fix branch yet, create it:

- Create such a branch from the respective [minor-version tag](https://semver.org/), e.g. create minor version branch `spark-3.0_v1.1` from tag `v1.1.0_spark-3.0`.
- Bump the version to the next [patch version](https://semver.org/) in `pom.xml` and append the `-SNAPSHOT` suffix again, e.g. `1.1.0-3.0` → `1.1.1-3.0-SNAPSHOT`.
- Commit this change to your local git repository, use a commit message like `Post-release version bump to 1.1.1`.
- Push this commit to origin.

Merge your bug fixes into this branch as you would normally do for the default branch `spark-3.0`, use PRs for that.

Remember to also merge the bug-fix into the default branch `spark-3.0`. Also consider [backporting the bug-fix](#backporting-commits-from-default-branch)
(e.g. into `spark-2.4`) and releasing backport bug-fix releases.

### Release from a bug-fix branch

This is very similar to [releasing from default branch](#releasing-from-default-branch),
but the version increment occurs on [patch level](https://semver.org/):

- Add a new entry to `CHANGELOG.md` listing all notable changes of this release.
  Use the heading `## [VERSION] - YYYY-MM-dd`, e.g. `## [1.1.1] - 2020-06-09`.
  No need to mention the branch as `CHANGELOG.md` is branch-specific.
- Remove the `-SNAPSHOT` suffix from `<version>` in the [`pom.xml`](pom.xml) file, e.g. `1.1.1-3.0-SNAPSHOT` → `1.1.1-3.0`.
- Update the versions in the `README.md` file to the version of your `pom.xml` to reflect the latest version,
  e.g. replace all `1.1.0-3.0` with `1.1.1-3.0` and `1.1.0-2.4` with `1.1.1-2.4`, respectively.
  All these changes should occur in the `Using Spark Dgraph Connector` section and its subsections.
- Commit the change to your local git repository, use a commit message like `Releasing 1.1.1`. Do not push to github yet.
- Tag that commit with a version tag like `v1.1.1_spark-3.0` and message like `Release v1.1.1`. Do not push to github yet.
- Release the version with `mvn clean deploy`. This will be put into a staging repository and not automatically released (due to `<autoReleaseAfterClose>false</autoReleaseAfterClose>` in your [`pom.xml`](pom.xml) file).
- Inspect and test the staged version. Use `spark-examples` for that. If you are happy with everything:
  - Push the commit and tag to origin.
  - Release the package with `mvn nexus-staging:release`.
  - Bump the version to the next [patch version](https://semver.org/) in `pom.xml` and append the `-SNAPSHOT` suffix again, e.g. `1.1.1-3.0` → `1.1.2-3.0-SNAPSHOT`.
  - Commit this change to your local git repository, use a commit message like `Post-release version bump to 1.1.2`.
  - Push all local commits to origin.
- Otherwise drop it with `mvn nexus-staging:drop`. Remove the last two commits from your local history.

After successfully releasing from a `spark-3.0` bug-fix branch, merge the bug-fixes into other bug-fix branches like `spark-2.4_v1.1`.
Repeat above release process for those branches with the same versions, e.g. version `1.1.1-2.4` and tag `v1.1.1_spark-2.4`.

## Git cheat sheet

    git commit -a -m "Releasing 1.1.0"
    git tag -a v1.1.0_spark-3.0 -m "Release v1.1.0"
    git push origin spark-3.0 v1.1.0_spark-3.0
    git commit -a -m "Post-release version bump to 1.2.0"

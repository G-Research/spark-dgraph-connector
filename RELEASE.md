# Releasing Spark Dgraph Connector

This provides instructions on how to release a version of `spark-dgraph-connector`.
These steps are codified by these release scripts (run in this order):

- release.prepare.sh
- release.release.sh or release.drop.sh

## Releasing from default branch

Follow this procedure to release a new version from default branch `spark-3.2`:

- Add a new entry to `CHANGELOG.md` listing all notable changes of this release.
  Use the heading `## [VERSION] - YYYY-MM-dd`, e.g. `## [1.1.0] - 2020-06-09`.
  No need to mention the branch as `CHANGELOG.md` is branch-specific.
- Remove the `-SNAPSHOT` suffix from `<version>` in the [`pom.xml`](pom.xml)
  and [`examples/scala/pom.xml`](examples/scala/pom.xml) files, e.g. `1.1.0-3.2-SNAPSHOT` → `1.1.0-3.2`.
- Update the versions in the `README.md` file to the version of your `pom.xml` to reflect the latest version,
  e.g. replace all `1.0.0-3.2` with `1.1.0-3.2`.
  All these changes should occur in the `Using Spark Dgraph Connector` section and its subsections.
- Commit the change to your local git repository, use a commit message like `Releasing 1.1.0`. Do not push to github yet.
- Tag that commit with a version tag like `v1.1.0_spark-3.2` and message like `Release v1.1.0`. Do not push to github yet.
- Release the version with `mvn clean deploy -Dsign`. This will be put into a staging repository and not automatically released (due to `<autoReleaseAfterClose>false</autoReleaseAfterClose>` in your [`pom.xml`](pom.xml) file).
- Inspect and test the staged version. Use `examples/scala/` for that. If you are happy with everything:
  - Push the commit and tag to origin.
  - Release the package with `mvn nexus-staging:release`.
  - Bump the version to the next [minor version](https://semver.org/) in `pom.xml` and `examples/scala/pom.xml`,
    and append the `-SNAPSHOT` suffix again, e.g. `1.1.0-3.2` → `1.2.0-3.2-SNAPSHOT`.
  - Commit this change to your local git repository, use a commit message like `Post-release version bump to 1.2.0`.
  - Push all local commits to origin.
- Otherwise drop it with `mvn nexus-staging:drop`. Remove the last two commits from your local history.

After successfully releasing default branch `spark-3.2`, consider releasing backport branches like `spark-3.0` and `spark-3.1`.

## Releasing from backport branches

Compare the backport branch (e.g. `spark-3.0`) against the default branch `spark-3.2`. Use the following script:

    ./git-compare-logs-3.0.sh

This script shows the commits that are in one branch but not the other, together with all commits
since `spark-3.0` branched off `spark-3.2`. It is important not to change the commit message when
merging (cherry-pick) one commit from one branch to the other and not to enhance a commit.

Once the backport branch is in sync (feature wise) with the default branch, repeat above release process
for the backport branch with the same version, e.g. version `1.1.0-3.0` and tag `v1.1.0_spark-3.0`.
Minor versions need to be in sync across all release branches (same feature set), whereas patch versions may differ.

### Backporting commits from default branch

For each commit that you want to backport (here to `spark-3.0`) perform the following steps:

Identify the commit that you want to backport:

	===== missing commits =====
	spark-3.0: Upgrade dgraph4j to v21.12.0 (#158)
	spark-3.0: Upgrade Spark to 3.1.2 (#132)
	spark-3.1: Upgrade Spark to 3.0.3 (#134)

	===== spark-3.1 =====
	* 7cd46e9 - Upgrade dgraph4j to v21.12.0 (#158) (vor 4 Wochen)
	* 0a8feac - Support dgraph v21.12 (#147) (vor 7 Wochen)
	* 065c2bc - Post-release version bump to 0.8.0 (vor 4 Monaten)
	* b141836 - Releasing 0.7.0 (tag: v0.7.0_spark-3.1) (vor 4 Monaten)

	===== spark-3.0 =====
	* ea0cb2d - Support dgraph v21.12 (#147) (vor 86 Minuten)
	* ae56e77 - Post-release version bump to 0.8.0 (vor 4 Monaten)
	* 8d63fe6 - Releasing 0.7.0 (tag: v0.7.0_spark-3.0) (vor 4 Monaten)

As an example, we want to backport commit `7cd46e9` to `spark-3.0`.

    git checkout spark-3.0
    git cherry-pick 7cd46e9
    # resolve any conflicts
    # add resolved files with 'git add'
    # finish with 'git cherry-pick --continue'
    git show HEAD

Review the actual changes of the commit and look out for any backport related changes that need adjustment.
Also run unit tests. If you have changes (only backporting changes, no new features or enhancements) commit
them with `git commit --amend` to add them to the cherry-picked commit.

Finally, push your changes to origin.

## Releasing a bug-fix version

A bug-fix version needs to be released from a [minor-version branch](https://semver.org/), e.g. `spark-3.2_v1.1`.

### Create a bug-fix branch

If there is no bug-fix branch yet, create it:

- Create such a branch from the respective [minor-version tag](https://semver.org/), e.g. create minor version branch `spark-3.2_v1.1` from tag `v1.1.0_spark-3.2`.
- Bump the version to the next [patch version](https://semver.org/) in `pom.xml` and append the `-SNAPSHOT` suffix again, e.g. `1.1.0-3.2` → `1.1.1-3.2-SNAPSHOT`.
- Commit this change to your local git repository, use a commit message like `Post-release version bump to 1.1.1`.
- Push this commit to origin.

Merge your bug fixes into this branch as you would normally do for the default branch `spark-3.2`, use PRs for that.

Remember to also merge the bug-fix into the default branch `spark-3.2`. Also consider [backporting the bug-fix](#backporting-commits-from-default-branch)
(e.g. into `spark-3.0`) and releasing backport bug-fix releases.

### Release from a bug-fix branch

This is very similar to [releasing from default branch](#releasing-from-default-branch),
but the version increment occurs on [patch level](https://semver.org/):

- Add a new entry to `CHANGELOG.md` listing all notable changes of this release.
  Use the heading `## [VERSION] - YYYY-MM-dd`, e.g. `## [1.1.1] - 2020-06-09`.
  No need to mention the branch as `CHANGELOG.md` is branch-specific.
- Remove the `-SNAPSHOT` suffix from `<version>` in the [`pom.xml`](pom.xml)
  and [`examples/scala/pom.xml`](examples/scala/pom.xml) files, e.g. `1.1.1-3.2-SNAPSHOT` → `1.1.1-3.2`.
- Update the versions in the `README.md` file to the version of your `pom.xml` to reflect the latest version,
  e.g. replace all `1.1.0-3.2` with `1.1.1-3.2`, respectively.
  All these changes should occur in the `Using Spark Dgraph Connector` section and its subsections.
- Commit the change to your local git repository, use a commit message like `Releasing 1.1.1`. Do not push to github yet.
- Tag that commit with a version tag like `v1.1.1_spark-3.2` and message like `Release v1.1.1`. Do not push to github yet.
- Release the version with `mvn clean deploy -Dsign`. This will be put into a staging repository and not automatically released (due to `<autoReleaseAfterClose>false</autoReleaseAfterClose>` in your [`pom.xml`](pom.xml) file).
- Inspect and test the staged version. Use `examples/scala/` for that. If you are happy with everything:
  - Push the commit and tag to origin.
  - Release the package with `mvn nexus-staging:release`.
  - Bump the version to the next [patch version](https://semver.org/) in `pom.xml` and `examples/scala/pom.xml`,
    and append the `-SNAPSHOT` suffix again, e.g. `1.1.1-3.2` → `1.1.2-3.2-SNAPSHOT`.
  - Commit this change to your local git repository, use a commit message like `Post-release version bump to 1.1.2`.
  - Push all local commits to origin.
- Otherwise drop it with `mvn nexus-staging:drop`. Remove the last two commits from your local history.

After successfully releasing from a `spark-3.2` bug-fix branch, merge the bug-fixes into other bug-fix branches like `spark-3.0_v1.1`.
Repeat above release process for those branches with the same versions, e.g. version `1.1.1-3.0` and tag `v1.1.1_spark-3.0`.

## Git cheat sheet

    git commit -a -m "Releasing 1.1.0"
    git tag -a v1.1.0_spark-3.2 -m "Release v1.1.0"
    git push origin spark-3.2 v1.1.0_spark-3.2
    git commit -a -m "Post-release version bump to 1.2.0"

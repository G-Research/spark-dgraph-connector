# Releasing Spark DGraph Connector

This provides instructions on how to release a version of `spark-dgraph-connector`.

## Releasing from default branch

Follow this procedure to release a new version from default branch `spark-3.0`:

- Add a new entry to `CHANGELOG.md` listing all notable changes of this release. Use the heading `## [VERSION] - YYYY-MM-dd`, e.g. `## [1.1.0] - 2020-06-09`. No need to mention the branch as `CHANGELOG.md` is branch-specific.
- Remove the `-SNAPSHOT` suffix from `<version>` in the [`pom.xml`](pom.xml) file, e.g. `1.1.0-SNAPSHOT` → `1.1.0`.
- Commit the change to your local git repository, use a commit message like `Releasing 1.1.0`. Do not push to github yet.
- Tag that commit with a version tag like `spark-3.0_v1.1.0` and message like `Release v1.1.0`. Do not push to github yet.
- Release the version with `mvn clean deploy`. This will be put into a staging repository and not automatically released (due to `<autoReleaseAfterClose>false</autoReleaseAfterClose>` in your [`pom.xml`](pom.xml) file).
- Inspect and test the staged version. Use `spark-examples` for that. If you are happy with everything:
  - Push the commit and tag to origin.
  - Release the package with `mvn nexus-staging:release`.
  - Bump the version to the next [minor version](https://semver.org/) in `pom.xml` and append the `-SNAPSHOT` suffix again, e.g. `1.1.0` → `1.2.0-SNAPSHOT`.
  - Commit this change to your local git repository, use a commit message like `Post-release version bump to 1.2.0`.
  - Push all local commits to origin.
- Otherwise drop it with `mvn nexus-staging:drop`. Remove the last two commits from your local history.

After successfully releasing default branch `spark-3.0`, merge this branch into other release branches like `spark-2.4`.
Repeat above release process for those branches with the same versions, e.g. version `1.1.0` and tag `spark-2.4_v1.1.0`.

## Releasing a bug-fix version

A bug-fix version needs to be released from a [minor-version branch](https://semver.org/), e.g. `spark-3.0_v1.1`.

### Create a bug-fix branch

If there is no bug-fix branch yet, create it:

- Create such a branch from the respective [minor-version tag](https://semver.org/), e.g. create minor version branch `spark-3.0_v1.1` from tag `spark-3.0_v1.1.0`.
- Bump the version to the next [patch version](https://semver.org/) in `pom.xml` and append the `-SNAPSHOT` suffix again, e.g. `1.1.0` → `1.1.1-SNAPSHOT`.
- Commit this change to your local git repository, use a commit message like `Post-release version bump to 1.1.1`.
- Push this commit to origin.

Merge your bug fixes into this branch as you would normally do for the default branch `spark-3.0`, use PRs for that.

### Release from a bug-fix branch

This is very similar to [releasing from default branch](#releasing-from-default-branch),
but the version increment occurs on [patch level](https://semver.org/):

- Add a new entry to `CHANGELOG.md` listing all notable changes of this release. Use the heading `## [VERSION] - YYYY-MM-dd`, e.g. `## [1.1.1] - 2020-06-09`. No need to mention the branch as `CHANGELOG.md` is branch-specific.
- Remove the `-SNAPSHOT` suffix from `<version>` in the [`pom.xml`](pom.xml) file, e.g. `1.1.1-SNAPSHOT` → `1.1.1`.
- Commit the change to your local git repository, use a commit message like `Releasing 1.1.1`. Do not push to github yet.
- Tag that commit with a version tag like `spark-3.0_v1.1.1` and message like `Release v1.1.1`. Do not push to github yet.
- Release the version with `mvn clean deploy`. This will be put into a staging repository and not automatically released (due to `<autoReleaseAfterClose>false</autoReleaseAfterClose>` in your [`pom.xml`](pom.xml) file).
- Inspect and test the staged version. Use `spark-examples` for that. If you are happy with everything:
  - Push the commit and tag to origin.
  - Release the package with `mvn nexus-staging:release`.
  - Bump the version to the next [patch version](https://semver.org/) in `pom.xml` and append the `-SNAPSHOT` suffix again, e.g. `1.1.1` → `1.1.2-SNAPSHOT`.
  - Commit this change to your local git repository, use a commit message like `Post-release version bump to 1.1.2`.
  - Push all local commits to origin.
- Otherwise drop it with `mvn nexus-staging:drop`. Remove the last two commits from your local history.

After successfully releasing from a `spark-3.0` bug-fix branch, merge the bug-fixes into other bug-fix branches like `spark-2.4_v1.1`.
Repeat above release process for those branches with the same versions, e.g. version `1.1.1` and tag `spark-2.4_v1.1.1`.

## Git cheat sheet

    git commit -a -m "Releasing 1.1.0"
    git tag -a v1.1.0-spark-3.0 -m "Release v1.1.0"
    git push origin spark-3.0 v1.1.0-spark-3.0
    git commit -a -m "Post-release version bump to 1.2.0"

    git tag -d v1.1.0-spark-3.0

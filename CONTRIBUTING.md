# Contributing to the Spark Dgraph Connector

Hello and thank you for taking the time to contribute! 👋 🎉

The following is a set of guidelines for contributing to the [Spark Dgraph Connector](https://github.com/G-Research/spark-dgraph-connector) (SDC) on GitHub. While this open-source project is currently maintained by G-Research, there aren't enough core contributors to go around and your contribution will help us build better for all!

Note that these are guidelines, not rules. Use your best judgment, and feel free to propose changes to this document in a pull request.


#### Table Of Contents

[Code of Conduct](#code-of-conduct)

[I don't want to read this whole thing, I just have a question!](#i-dont-want-to-read-this-whole-thing-i-just-have-a-question)

[What should I know before I get started?](#what-should-i-know-before-i-get-started)

[How Can I Contribute?](#how-can-i-contribute)
  * [Reporting Bugs](#reporting-bugs)
  * [Feature requests](#feature-requests)
  * [Making Pull Requests](#making-pull-requests)

## Code of Conduct

This project and everyone participating in it is governed by the [SDC Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code. Please report unacceptable behavior to <conduct.sparkdc@gr-oss.io>.

## I don't want to read this whole thing I just have a question!

> **Note:** Please don't file an issue to ask a question.  You'll get faster results by using [Github Discussions](https://github.com/G-Research/spark-dgraph-connector/discussions), the primary SDC discussion forum.

## What should I know before I get started?

### The Spark Dgraph Connector

The SDC is an open source project that supports Spark for Python and Scala. When considering a contribution to SDC, you might be unsure where to start or how to report a bug. This document should help you with that.

We plan to document all significant decisions regarding project maintenance, support, and future features in Github Discussions under [announcements](https://github.com/G-Research/spark-dgraph-connector/discussions/categories/announcements). If you have a question around how we do things, and it is *not* documented there, please open a new topic on Github Discussions under [questions](https://github.com/G-Research/spark-dgraph-connector/discussions/categories/q-a) and ask your question.

## How Can I Contribute?

### Reporting Bugs

This section guides you through submitting a bug report for SDC. Following these guidelines helps core contributors and the community understand your report :pencil:, reproduce the behavior :computer: :computer:, and find related reports :mag_right:.

When you are creating a bug report, please [include as many details as possible](#how-do-i-submit-a-good-bug-report). Fill out [the required template](#templates), the information it asks for helps us resolve issues faster.

> **Note:** If you find a **Closed** issue that seems like it is the same thing that you're experiencing, open a new issue and include a link to the original issue in the body of your new one.

#### How Do I Submit a Bug Report?

Bugs are tracked as [GitHub issues](https://github.com/G-Research/spark-dgraph-connector/issues). 

1. Determine what type of bug it is &mdash; is it related to 📑*documentation*📑 or is it just a 🐞*bug*🐞 &mdash; and **label** it.

2. Provide details by explaining the problem and include additional details to help core contributors reproduce the problem. You can choose from preset[templates](https://github.com/G-Research/spark-dgraph-connector/issues/new/choose) if applicable.


* **Use a clear and descriptive title** for the issue to identify the problem.
* **Describe the exact steps which reproduce the problem** in as much detail as possible. For example, start by explaining how you started SDC, that is, which command you typed in the terminal, or how you otherwise started SDC.
* **Provide specific examples to demonstrate the steps**. Include links to files or GitHub projects, or copy/pasteable snippets, which you use in those examples. If you're providing snippets in the issue, use [Markdown code blocks](https://help.github.com/articles/markdown-basics/#multiple-lines).
* **Describe the behavior you observed after following the steps** and point out what exactly is the problem with that behavior.
* **Explain which behavior you expected to see instead and why.**
* **If the problem wasn't triggered by a specific action**, describe what you were doing before the problem happened and share more information using the guidelines below.

3. Provide more context by answering these questions:

* **Can you reliably reproduce the issue?** If not, provide details about how often the problem happens and under which conditions it normally happens.
* If the problem is related to working with files (e.g. opening and editing files), **does the problem happen for all files and projects or only some?** Does the problem happen only when working with local or remote files (e.g. on network drives), with files of a specific type (e.g. only Scala or Python files), with large files or files in a specific encoding? Is there anything else special about the files you are using?

### Feature requests

This section guides you through submitting a feature request for SDC, including minor improvements to existing functionality. Following these guidelines helps core contributors and the community understand your suggestion :pencil: and find related suggestions :mag_right:.

Before creating enhancement suggestions, please review the features and enhancements already in [issues](https://github.com/G-Research/spark-dgraph-connector/issues) as you might learn that your feature request has already been suggested. When you are creating a feature request, please include as many details as possible.

* **Use a clear and descriptive title** for the issue to identify the suggestion.
* **Provide a step-by-step description of the suggested feature/enhancement** in as much detail as possible.
* **Provide examples to demonstrate the feature/enhancement**. Include copy/pasteable snippets which you use in those examples, as [Markdown code blocks](https://help.github.com/articles/markdown-basics/#multiple-lines).
* **Describe the current behavior** and **explain why the suggested behavior would be an improvement**.
* If applicable, **include screenshots or animated GIFs** which help you demonstrate the steps or point out the part the suggestion is related to. You can use [this tool](https://www.cockos.com/licecap/) to record GIFs on macOS and Windows, and [this tool](https://github.com/colinkeenan/silentcast) or [this tool](https://github.com/GNOME/byzanz) on Linux.
* **Explain why this feature/enhancement would be useful** to most SDC users.
* If possible, **list some other applications where this feature/enhancement exists.**
* **Specify the name and version of the OS you're using.**


### Making Pull Requests

If you would like to offer a contribution, please open a pull request following the instructions below. The process described here has several goals:

- Maintain the quality of the SDC
- Fix problems that are important to users
- Engage the community in working toward the best possible product
- Enable a sustainable system for SDC's core contributors to review contributions

Please follow these steps to have your contribution considered by the core contributors:

1. Follow all instructions in [the template](https://github.com/G-Research/spark-dgraph-connector/blob/spark-3.1/.github/pull_request_template.md)
2. Follow the [styleguides](#styleguide)
3. After you submit your pull request, verify that all [status checks](https://help.github.com/articles/about-status-checks/) are passing <details><summary>What if the status checks are failing?</summary>If a status check is failing, and you believe that the failure is unrelated to your change, please leave a comment on the pull request explaining why you believe the failure is unrelated. A core contributor will re-run the status check for you. If we conclude that the failure was a false positive, then we will open an issue to track that problem with our status check suite.</details>

While the prerequisites above must be satisfied prior to having your pull request reviewed, the reviewer(s) may ask you to complete additional design work, tests, or other changes before your pull request can be approved.

## Styleguide

### Git Commit Messages

* Use the present tense ("Add feature" not "Added feature")
* Use the imperative mood ("Move cursor to..." not "Moves cursor to...")
* Limit the first line to 72 characters or less
* Reference issues and pull requests liberally after the first line
* [Label](#type-of-issues) your issue if possible


## Additional Notes

### Issue and Pull Request Labels

This section lists the labels we use to help us track and manage issues and pull requests. 

[GitHub search](https://help.github.com/articles/searching-issues/) makes it easy to use labels for finding groups of issues or pull requests you're interested in. The labels are loosely grouped by their purpose, but it's not required that every issue has a label from every group or that an issue can't have more than one label from the same group.

Please open an [issue](https://github.com/G-Research/spark-dgraph-connector/issues) if you have suggestions for new labels.

#### Available Labels

| Label | Search | Description |
| --- | --- | --- |
| `bug` | [🔎](https://github.com/G-Research/spark-dgraph-connector/labels/bug) | Something isn't working |
| `dependencies` | [🔎](https://github.com/G-Research/spark-dgraph-connector/labels/dependencies) | Upgrading or fixing dependencies |
| `documentation` | [🔎](https://github.com/G-Research/spark-dgraph-connector/labels/documentation) | Improvements or additions to documentation |
| `duplicate` | [🔎](https://github.com/G-Research/spark-dgraph-connector/labels/duplicate) | This issue or pull request already exists |
| `enhancement` | [🔎](https://github.com/G-Research/spark-dgraph-connector/labels/enhancement) | New feature or request |
| `github_actions` | [🔎](https://github.com/G-Research/spark-dgraph-connector/labels/github_actions) | Pull requests that update Github actions code |
| `good first issue` | [🔎](https://github.com/G-Research/spark-dgraph-connector/labels/good%20first%20issue) | Good for newcomers |
| `help-wanted` | [🔎](https://github.com/G-Research/spark-dgraph-connector/labels/help%20wanted) | Extra attention is needed |
| `invalid` | [🔎](https://github.com/G-Research/spark-dgraph-connector/labels/invalid) | This doesn't seem right |
| `java` | [🔎](https://github.com/G-Research/spark-dgraph-connector/labels/java) | Pull requests that update Java code |
| `python` | [🔎](https://github.com/G-Research/spark-dgraph-connector/labels/python) | Pull requests that update Python code |
| `question` | [🔎](https://github.com/G-Research/spark-dgraph-connector/labels/question) | Further information is requested |
| `use_case` | [🔎](https://github.com/G-Research/spark-dgraph-connector/labels/use_case) | A specific use case or testimonial |
| `wontfix` | [🔎](https://github.com/G-Research/spark-dgraph-connector/labels/wontfix) | This will not be worked on for now |


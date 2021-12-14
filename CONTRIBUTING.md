# Contributing to the Spark-Dgraph Connector

First off, hello and thank you for taking the time to contribute! 👋 🎉 

The following is a set of guidelines for contributing to the [Spark-Dgraph connector](https://github.com/G-Research/spark-dgraph-connector) on GitHub. While this open-source project is currently maintained by G-research, there aren't enough maintainers to go around and your contribution will help us build better for all! 

The acronym SDC in this document refers to the Spark-Dgraph Connector. 

Note that these are mostly guidelines, not rules. Use your best judgment, and feel free to propose changes to this document in a pull request.


#### Table Of Contents

[Code of Conduct](#code-of-conduct)

[I don't want to read this whole thing, I just have a question!](#i-dont-want-to-read-this-whole-thing-i-just-have-a-question)

[What should I know before I get started?](#what-should-i-know-before-i-get-started)

[How Can I Contribute?](#how-can-i-contribute)
  * [Reporting Bugs](#reporting-bugs)
  * [Suggesting Enhancements](#suggesting-enhancements)
  * [Pull Requests](#pull-requests)

## Code of Conduct

This project and everyone participating in it is governed by the [SDC Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code. Please report unacceptable behavior to [](conduct.sparkdc@gr-oss.io).

## I don't want to read this whole thing I just have a question!

> **Note:** Please don't file an issue to ask a question. 

You'll get faster results by using [Github Discussions, the official SDC](https://github.com/G-Research/spark-dgraph-connector/discussions).


## What should I know before I get started?

### The Spark D-graph Connector

The SDC is an open source project &mdash; that supports databases in both Python and Scala. When you initially consider contributing to SDC, you might be unsure where to start or how to report a bug. This document should help you with that.

When we make a significant decision in how we maintain the project and what we can or cannot support, we will document it in Github Discussions under [announcements](https://github.com/G-Research/spark-dgraph-connector/discussions/categories/announcements). If you have a question around how we do things, and it is *not* documented there, please open a new topic on Github Discussions under[questions](https://github.com/G-Research/spark-dgraph-connector/discussions/categories/q-a) and ask your question.

## How Can I Contribute?

### Reporting Bugs

This section guides you through submitting a bug report for SDC. Following these guidelines helps maintainers and the community understand your report :pencil:, reproduce the behavior :computer: :computer:, and find related reports :mag_right:.

When you are creating a bug report, please [include as many details as possible](#how-do-i-submit-a-good-bug-report). Fill out [the required template](https://github.com/atom/.github/blob/master/.github/ISSUE_TEMPLATE/bug_report.md), the information it asks for helps us resolve issues faster.

> **Note:** If you find a **Closed** issue that seems like it is the same thing that you're experiencing, open a new issue and include a link to the original issue in the body of your new one.

#### How Do I Submit a Bug Report or Issue?

Bugs and issues are tracked as [GitHub issues](https://github.com/G-Research/spark-dgraph-connector/issues). 

1. Determine what type of bug it is, e.g. is it an ✨*enhancement*✨, related to 📑*documentation*📑 or just a 🐞*bug*🐞 and **label** it.

2. Provide details by explaining the problem and include additional details to help maintainers reproduce the problem. You can use the suggested [template](https://github.com/atom/.github/blob/master/.github/ISSUE_TEMPLATE/bug_report.md).


* **Use a clear and descriptive title** for the issue to identify the problem.
* **Describe the exact steps which reproduce the problem** in as many details as possible. For example, start by explaining how you started SDC, e.g. which command exactly you used in the terminal, or how you started SDC otherwise. When listing steps, 
* **Provide specific examples to demonstrate the steps**. Include links to files or GitHub projects, or copy/pasteable snippets, which you use in those examples. If you're providing snippets in the issue, use [Markdown code blocks](https://help.github.com/articles/markdown-basics/#multiple-lines).
* **Describe the behavior you observed after following the steps** and point out what exactly is the problem with that behavior.
* **Explain which behavior you expected to see instead and why.**
* **If the problem wasn't triggered by a specific action**, describe what you were doing before the problem happened and share more information using the guidelines below.

Provide more context by answering these questions:

* **Can you reliably reproduce the issue?** If not, provide details about how often the problem happens and under which conditions it normally happens.
* If the problem is related to working with files (e.g. opening and editing files), **does the problem happen for all files and projects or only some?** Does the problem happen only when working with local or remote files (e.g. on network drives), with files of a specific type (e.g. only Scala or Python files), with large files or files in a specific encoding? Is there anything else special about the files you are using?

Include details about your configuration and environment:

* **What's the name and version of the OS you're using**?
* **Are you running SDCc in a virtual machine?** If so, which VM software are you using and which operating systems and versions are used for the host and the guest?

### Suggesting Enhancements

This section guides you through submitting an enhancement suggestion for SDC, including completely new features and minor improvements to existing functionality. Following these guidelines helps maintainers and the community understand your suggestion :pencil: and find related suggestions :mag_right:.

Before creating enhancement suggestions, please check out the enhancements listed in[issues](https://github.com/G-Research/spark-dgraph-connector/issues) as you might find out that you don't need to create one. When you are creating an enhancement suggestion, please include as many details as possible.

* **Use a clear and descriptive title** for the issue to identify the suggestion.
* **Provide a step-by-step description of the suggested enhancement** in as many details as possible.
* **Provide specific examples to demonstrate the steps**. Include copy/pasteable snippets which you use in those examples, as [Markdown code blocks](https://help.github.com/articles/markdown-basics/#multiple-lines).
* **Describe the current behavior** and **explain which behavior you expected to see instead** and why.
* **Include screenshots and animated GIFs** which help you demonstrate the steps or point out the part the suggestion is related to. You can use [this tool](https://www.cockos.com/licecap/) to record GIFs on macOS and Windows, and [this tool](https://github.com/colinkeenan/silentcast) or [this tool](https://github.com/GNOME/byzanz) on Linux.
* **Explain why this enhancement would be useful** to most SDC users.
* **List some other applications where this enhancement exists.**
* **Specify the name and version of the OS you're using.**


### Pull Requests

The process described here has several goals:

- Maintain the connector's quality
- Fix problems that are important to users
- Engage the community in working toward the best possible connector
- Enable a sustainable system for SDC's maintainers to review contributions

* After you submit your pull request, verify that all [status checks](https://help.github.com/articles/about-status-checks/) are passing <details><summary>What if the status checks are failing?</summary>If a status check is failing, and you believe that the failure is unrelated to your change, please leave a comment on the pull request explaining why you believe the failure is unrelated. A maintainer will re-run the status check for you. If we conclude that the failure was a false positive, then we will open an issue to track that problem with our status check suite.</details>

While the prerequisites above must be satisfied prior to having your pull request reviewed, the reviewer(s) may ask you to complete additional design work, tests, or other changes before your pull request can be ultimately accepted.

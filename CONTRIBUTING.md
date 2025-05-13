# How to Contribute

Welcome to the Resonate project! We appreciate your interest in helping us build reliable
and scalable distributed systems. To get started, follow these simple steps:

## Initial setup

We follow the fork and branch workflow. There will be three Git repositories involved:

1.  *upstream* - the [resonate](https://github.com/resonatehq/resonate) repository on GitHub.
2.  *origin* - your GitHub fork of `upstream`.
3.  *local* - your local clone of `origin`.

These steps are only needed once and not for subsequent changes you might want to make:

1. Fork the `resonate` repository on GitHub to create `origin`.
   Visit [resonate](https://github.com/resonatehq/resonate) GitHub repository and click the `Fork` button.

2. Clone your fork.

    ```shell
    git clone git@github.com:<your-user-name>/resonate.git
    ```

## Development workflow

<p align="center">
    <img height="500"src="./docs/img/contributing.jpg">
</p>

Here is a outline of the steps needed to make changes to the resonate
project.


1. Make a branch in your repo.

   ```shell
   git switch -c my-branch
   ```

2. Make changes and commit.

   ```shell
   git add .
   git commit -m "dead simple"
   ```

3. Push your branch to your forked repo.

   ```shell
   git push origin my-branch
   ```

5. Select the branch you are working on in the drop-down menu of branches in
   your fork. Then hit the `Contribute`, followed by the `Open pull request`
   button.

6. Double check your changes and hit the `Create pull request` button. A
   maintainer from Resonate will review your PR. Thank you for your
   contribution!

## What to contribute to?

Here are some areas where your contributions would be valuable:

* Bug fixes for existing packages.
* Refactoring efforts to improve code quality.
* Enhancements to our testing and reliability efforts.

Thank you for your contributions and support in building a better Resonate! 🚀

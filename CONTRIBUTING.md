# How to Contribute

Welcome to the Resonate project! We appreciate your interest in helping us build reliable 
and scalable distributed systems. To get started, follow these simple steps:

## MAKE A NEW RELEASE NOTE. 

## Initial setup

We follow the fork and branch workflow. There will be three Git repositories involved:

1.  *upstream* - the [resonate](https://github.com/resonatehq/resonate) repository on GitHub.
2.  *origin* - your GitHub fork of `upstream`. 
3.  *local* - your local clone of `origin`. 

These steps are only needed once and not for subsequent changes you might want to make:

1. Fork the `resonate` repository on GitHub to create `origin`.
   Visit [resonate](https://github.com/resonatehq/resonate) GitHub repository and click the `Fork` button.

2. Make a `local` clone of your fork.

    ```shell
    git clone git@github.com:<your-user-name>/resonate.git
    ```

3. Add a remote pointing from `local` to `upstream`.

    ```shell
    cd resonate
    git remote add upstream git@github.com:resonatehq/resonate.git
    ```
4. Double check the two remotes are referencing the expected url.

    ```shell
    git remote get-url origin   # git@github.com:<your-user-name>/resonate.git
    git remote get-url upstream # git@github.com:resonatehq/resonate.git
    ```

## Development workflow

<p align="center">
    <img height="500"src="./docs/img/contributing.jpg">
</p>

Here is a outline of the steps needed to make changes to the resonate
project.


1. Make a local branch in your clone and pull any recent changes into it.

   ```shell
   git switch -c awesome_branch  
   git pull upstream main
   ```

2. Make changes and commit to local branch.

   ```shell
   git add .
   git commit -m "dead simple"
   ```

3. Pull any changes that may have been made in the upstream repository
   main branch.

   ```shell
   git pull --rebase upstream main # may result in merge conflicts
   ```

4. Push your branch to the corresponding branch in your fork.

   ```shell
   git push origin awesome_branch
   ```

5. Select the branch you are working on in the drop-down menu of branches in
   your fork. Then hit the `Compare and pull request` button.

6. Once your pull request has been reviewed and approved by a maintainer, select 
   the `Squash and merge` option. Edit the commit message as appropriate for the 
   squashed commit.

7. Delete the branch from `origin`:

    ```
    git push origin --delete awesome_branch
    ```

8. Delete the branch from `local`:

    ```
    git switch main
    git branch -D awesome_branch
    ```

## What to contribute to?

Here are some areas where your contributions would be valuable:

* Bug fixes for existing packages.
* Refactoring efforts to improve code quality.
* Enhancements to our testing and reliability efforts.
* Add support for the following databases: MySQL, Elasticsearch, MongoDB.

Thank you for your contributions and support in building a better Resonate! 🚀

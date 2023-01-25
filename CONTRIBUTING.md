# How to contribute

First, thanks for taking the time to contribute to our project! There are many ways you can help out.

## Questions

If you have a question that needs an answer, [create an issue](https://help.github.com/articles/creating-an-issue/), and label it as a question.

## Issues for bugs or feature requests

If you encounter any bugs in the code, or want to request a new feature or enhancement, please create an issue to report it. Kindly add a label to indicate what type of issue it is.

## Contribute Code

Firstly, if you want to implement something new, we ask that you open a 
discussion first. For minor changes or bug fixes you can skip this part and go straight ahead to sending a contribution. Bear in mind that if you open a discussion first you can identify if the change will be accepted, as well as getting early feedback. When your code is ready to be submitted, [submit a pull request](https://help.github.com/articles/creating-a-pull-request/) to begin the code review process.

Here's a quick checklist for a good PR, more details below:

1. A discussion around the change (https://github.com/paulbares/aitm/discussions/categories/ideas)
2. A GitHub Issue with a good description associated with the PR
3. One feature/change per PR
4. PR rebased on main (`git rebase`, not `git pull`) 
5. [Good descriptive commit message, with link to issue](#commit-messages-and-issue-linking)
6. No changes to code not directly related to your PR
7. Includes functional/integration test
8. Includes documentation

Once you have submitted your PR please monitor it for comments/feedback. We reserve the right to close inactive PRs if you do not respond within 2 weeks (bear in mind you can always open a new PR if it is closed due to inactivity).

Also, please remember that we may not be able to respond to your PR immediately.

We only seek to accept code that you are authorized to contribute to the project. We have added a pull request template on our projects so that your contributions are made with the following confirmation:

> I confirm that this contribution is made under the terms of the license found in the root directory of this repository’s source tree and that I have the authority necessary to make this contribution on behalf of its copyright owner.

## Commit messages and issue linking

The format for a commit message should look like:

```
A brief descriptive summary

Optionally, more details around how it was implemented

Closes #1234
``` 

The very last part of the commit message should be a link to the GitHub issue, when done correctly GitHub will automatically link the issue with the PR. There are 3 alternatives provided by GitHub here:

* Closes: Issues in the same repository
* Fixes: Issues in a different repository (this shouldn't be used, as issues should be created in the correct repository instead)
* Resolves: When multiple issues are resolved (this should be avoided)

Although, GitHub allows alternatives (close, closed, fix, fixed), please only use the above formats.

Creating multi line commit messages with `git` can be done with:

```
git commit -m "Summary" -m "Optional description" -m "Closes #1234"
```

Alternatively, `shift + enter` can be used to add line breaks:

```
$ git commit -m "Summary
> 
> Optional description
> 
> Closes #1234"
```

For more information linking PRs to issues refer to the [GitHub Documentation](https://docs.github.com/en/issues/tracking-your-work-with-issues/linking-a-pull-request-to-an-issue).

## Code of Conduct

We encourage inclusive and professional interactions on our project. We welcome everyone to open an issue, improve the
documentation, report bug or submit a pull request. By participating in this project, you agree to abide by
the [SquashQL Code of Conduct](./CODE-OF-CONDUCT.md). If you feel there is a conduct issue related to this project, please
raise it per the Code of Conduct process, and we will address it.

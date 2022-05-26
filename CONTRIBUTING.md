# Contributing to Data Validator

## Fixing a bug you found

1. Check existing [issues](https://github.com/target/data-validator/issues) first to see if anyone else has reported the bug
2. Report the bug if no one else has reported it.
3. Fix the bug.
4. Submit a PR.
5. Be ready for some back-and-forth between maintainers and you.

## Creating new checks

1. Check existing [issues](https://github.com/target/data-validator/issues) first to see if anyone else has reported a desire for the check.
2. If it doesn't exist (it probably won't!) then create a new issue:
    1. Provide an example of how you would like the configuration for the check to look. Most of our rework requests are the result of an unclear vision for the interface to the check!
    2. Clearly state if you are intending to work on it or if you are simply asking for it. If you're going to work on it, please provide a timeline for delivery. If you're just asking for it, you're done after you've submitted the request issue.
3. Work on it!
    1. Abide by the style checker requirements.
    2. Include tests. Submissions without tests will not be considered. Test the following things:
        1. Configuration parsing
        2. Configuration sanity checking
        3. Variable substitution
        4. Actual check functionality

## Refactoring

Follow the new checks procedure, but instead of providing a configuration example, clearly explain:

1. How the current state of things negatively affects the extensibility of Data Validator.
2. How you intend to remedy the situation with the minimum amount of code changed

**Do not mix refactoring with the addition of a new check in the same pull request.** We will reject and ask that they be done in separate PRs to keep things manageable.

## Development Environment Setup

Developers on **macOS** should be able to clone, run `make deps build`, and be ready for a development cycle.
This assumes that [Homebrew](https://brew.sh/) is already installed, as is common for macOS developers.

Developers on **Linux or Windows** will need to install a Java 8 JDK, preferably 
the [Temurin JDK from the Adoptium Working Group](https://adoptium.net/) of the [Eclipse Foundation](https://www.eclipse.org)
or another JDK in the OpenJDK family.

Run `make help` to see common tasks. Make tasks are provided for those unfamiliar with
running `sbt` in console mode.
Those preferring `sbt` are assumed to know what they're doing but can get a quick refresher
by looking at the tasks in the `Makefile`.

# Contributing to Trading System

Thank you for your interest in improving Trading System! This guide walks you through the expectations for participating in the project.

## Getting Started
1. Fork the repository and clone your fork locally.
2. Create a feature branch named `feature/my-change` (or similar) for your work.
3. Keep your branch up to date with `main` to minimize merge conflicts.

## Code Style
- Format all Python code with **Black** (`poetry run fmt`).
- Keep imports tidy and error-free with **Ruff**; no unused imports are allowed (`poetry run lint`).
- Maintain complete and accurate type hints (`poetry run typecheck`).
- Configure your editor or pre-commit hooks to run these checks automatically before committing.

## Testing
- Run the project test suite with `poetry run tests` before submitting a pull request.
- Ensure coverage targets defined in the project settings are met.
- Provide new or updated tests when you add features or fix bugs.
- Use `poetry run ci` to mirror the continuous integration workflow when validating complex changes.

## Commit Messages
- Write commit messages in the imperative mood (e.g., `Fix bug in risk evaluation`).
- Keep messages concise while explaining the change and its intent.
- Separate unrelated work into distinct commits for clarity.

## Pull Request Guidelines
- Keep pull requests focused and as small as possible for easier review.
- Reference related issues or GitHub Project cards in the description.
- Include documentation and tests alongside new features or behavior changes.
- Fill in the PR template completely so reviewers understand the context.

## Project Management
- Use the [GitHub Project board](https://github.com/users/aryeko/projects/2) to align your work with the roadmap and ongoing initiatives.
- Consult the [project Wiki](https://github.com/aryeko/trading-system/wiki) for architecture, workflows, and design documentation before proposing changes.
- Synchronize any documentation updates with `poetry run docs-sync` when relevant.

## License Note
By contributing to Trading System, you agree that your contributions will be licensed under the terms of the MIT License.

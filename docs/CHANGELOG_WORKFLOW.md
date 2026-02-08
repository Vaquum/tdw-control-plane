# Changelog Workflow Usage

## How to Update the Changelog for Your PR

When you're ready to merge a PR that should update the changelog and version:

### Step 1: Add the Label
Add the `update-changelog` label to your PR.

You can do this:
- Via GitHub UI: Click "Labels" on the right sidebar and select `update-changelog`
- Via GitHub CLI: `gh pr edit <pr-number> --add-label update-changelog`

### Step 2: Wait for the Workflow
The "PR CHANGELOG Update" workflow will automatically:
1. Analyze your commits and file changes
2. Use Claude AI to generate an appropriate changelog entry
3. Determine the correct version bump (MAJOR, MINOR, or PATCH)
4. Update `CHANGELOG.md` and `pyproject.toml`
5. Commit these changes to your PR branch

This typically takes 30-60 seconds.

### Step 3: Review the Changes
The workflow will add a commit to your PR with the changelog and version updates. Review it to ensure:
- The version bump is appropriate
- The changelog entry accurately describes the changes
- The formatting follows the project standards

### Step 4: Merge Normally
Once you're happy with the automated changelog update, merge the PR as usual. The changelog and version are now part of your PR - no additional PRs or steps needed!

## When to Use This

Add the `update-changelog` label when:
- Your PR adds new features (MINOR version bump)
- Your PR fixes bugs (PATCH version bump)
- Your PR makes breaking changes (MAJOR version bump)

Don't add the label for:
- Documentation-only changes
- CI/tooling changes that don't affect the library
- Minor refactoring that doesn't change behavior

## Troubleshooting

### The workflow didn't run
- Make sure you added the exact label: `update-changelog`
- Check the Actions tab to see if there were any errors
- Ensure your PR targets the `main` branch

### The changelog entry is incorrect
- You can manually edit `CHANGELOG.md` and `pyproject.toml` after the automated commit
- Or remove the automated commit, adjust your commits, and re-trigger by removing and re-adding the label

### I need to re-run the workflow
If you push new commits after the workflow ran:
- The workflow automatically re-runs on each push (synchronize event)
- Or remove and re-add the `update-changelog` label

## Implementation Details

For details about why we use this approach and what alternatives were considered, see [CHANGELOG_WORKFLOW_SOLUTION.md](../.github/CHANGELOG_WORKFLOW_SOLUTION.md).

# GitHub Secrets Configuration

This document describes the GitHub secrets required for the workflows in this repository.

## Required Secrets

### ANTHROPIC_API_KEY

**Used by:** `.github/workflows/pr_post_changelog.yml`

**Purpose:** Enables automated CHANGELOG.md updates and version bumping using Claude AI after merges to main.

**Setup:**
1. Obtain an API key from [Anthropic Console](https://console.anthropic.com/)
2. Go to your repository Settings → Secrets and variables → Actions
3. Click "New repository secret"
4. Name: `ANTHROPIC_API_KEY`
5. Value: Your Anthropic API key
6. Click "Add secret"

**Note:** If this secret is not configured, the post-merge changelog workflow will be skipped automatically.

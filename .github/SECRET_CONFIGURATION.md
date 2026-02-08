# Configuring Organization Secrets for Workflows

## Issue: ANTHROPIC_API_KEY Not Available to Repository

If you see an error like:
```
ANTHROPIC_API_KEY secret is not available to this repository
```

This means the organization-level secret exists but is not accessible to this specific repository.

## Solution: Configure Organization Secret Access

### Option 1: Add Repository to Organization Secret (Recommended)

1. Go to **Organization Settings** (https://github.com/organizations/Vaquum/settings/secrets/actions)
2. Click on **Secrets and variables** → **Actions**
3. Find the `ANTHROPIC_API_KEY` secret in the list
4. Click on the secret name to edit it
5. Under **Repository access**, click **Update selection**
6. Add `tdw-control-plane` to the list of repositories with access
7. Click **Save changes**

### Option 2: Create Repository-Level Secret

If you prefer to manage secrets at the repository level:

1. Go to **Repository Settings** (https://github.com/Vaquum/tdw-control-plane/settings/secrets/actions)
2. Click **New repository secret**
3. Name: `ANTHROPIC_API_KEY`
4. Value: Your Anthropic API key from [Anthropic Console](https://console.anthropic.com/)
5. Click **Add secret**

## Verifying the Configuration

After configuring the secret, the workflow will:
1. Check if the secret is available
2. Display the length of the API key (without revealing its value)
3. Proceed with the changelog update

The verification step will fail with clear instructions if the secret is still not accessible.

## Related Workflows

This configuration is also needed for any other workflows that use the `ANTHROPIC_API_KEY` secret.

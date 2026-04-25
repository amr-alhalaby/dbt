# GitHub Actions CI/CD Setup

This repository uses GitHub Actions for automated testing and deployment of dbt models.

## Required GitHub Secrets

You need to configure the following secrets in your GitHub repository:

### Navigate to: Settings → Secrets and variables → Actions → New repository secret

Add these secrets:

1. **SNOW_ACCOUNT**
   - Value: `PMCCQSK-AQ69260`
   - Description: Your Snowflake account identifier

2. **SNOW_USER**
   - Value: `AMRALHALABY`
   - Description: Your Snowflake username

3. **SNOW_PRIVATE_KEY**
   - Value: Contents of `~/.ssh/snowflake_key.p8` (entire file including header/footer)
   - Description: RSA private key for Snowflake key-pair authentication

> **Note:** The public key must be registered in Snowflake:
> ```sql
> ALTER USER AMRALHALABY SET RSA_PUBLIC_KEY='<contents of ~/.ssh/snowflake_key.pub without header/footer>';
> ```

## GitHub Environments

The workflow uses two environments that need to be configured:

### 1. CI Environment
- Used for: Pull requests and pushes
- Schema: `CI_<run_id>` (unique per run, auto-deleted after tests)
- Approval: Not required

### 2. Production Environment
- Used for: Deployments to master/main branch
- Schema: `PROD`
- Approval: Required (recommended)

### To create environments:
1. Go to Settings → Environments
2. Click "New environment"
3. Name it `ci`
4. Click "New environment" again
5. Name it `production`
6. Enable "Required reviewers" for production (optional but recommended)

## Workflow Jobs

### 1. **dbt-ci** (Continuous Integration)
- Runs on: Pull requests and pushes to master/main
- Schema: `CI_<github.run_id>` — isolated per run
- Actions:
  - Install dependencies
  - Run dbt debug, deps, seed, run, test (excludes Elementary)
  - Generate documentation
  - Upload artifacts (7 days)
  - **Auto-drop CI schema after run** (even on failure)

### 2. **dbt-prod-deploy** (Production Deployment)
- Runs on: Merges to master/main branch only
- Requires: CI job to pass first
- Actions:
  - Full refresh of seeds
  - Deploy all models
  - Run all tests (excludes Elementary)
  - Generate production docs
  - Upload artifacts (30 days)

### 3. **dbt-daily-tests** (Scheduled)
- Runs on: Daily schedule (optional, currently disabled)
- Target: Production environment
- Actions:
  - Run data quality tests

## Workflow Features

- ✅ Automated testing on pull requests
- ✅ Per-run isolated CI schemas (auto-cleaned up)
- ✅ Separate CI and Production schemas
- ✅ Snowflake key-pair authentication (no passwords)
- ✅ Artifact upload for debugging
- ✅ Documentation generation
- ✅ Production deployment gated on CI passing

## Manual Workflow Trigger

You can also manually trigger the workflow:
1. Go to Actions tab
2. Select "dbt CI/CD Pipeline"
3. Click "Run workflow"
4. Select branch and run

## Local Development

Private key is stored at `~/.ssh/snowflake_key.p8` and referenced in `~/.dbt/profiles.yml`.
Environment variables are set permanently in `~/.zshrc`:
- `SNOW_ACCOUNT`
- `SNOW_USER`

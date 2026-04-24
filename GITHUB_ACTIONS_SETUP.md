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

3. **SNOW_USER_PASSWORD**
   - Value: Your Snowflake password
   - Description: Your Snowflake password

## GitHub Environments

The workflow uses two environments that need to be configured:

### 1. CI Environment
- Used for: Pull requests and testing
- Schema: `CI`
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
- Runs on: Pull requests and pushes
- Target: CI environment
- Actions:
  - Install dependencies
  - Run dbt debug, deps, seed, run, test
  - Generate documentation
  - Upload artifacts

### 2. **dbt-prod-deploy** (Production Deployment)
- Runs on: Merges to master/main branch
- Target: Production environment
- Requires: CI job to pass
- Actions:
  - Full refresh of seeds
  - Deploy all models
  - Run all tests
  - Generate production docs

### 3. **dbt-daily-tests** (Scheduled)
- Runs on: Daily schedule (optional)
- Target: Production environment
- Actions:
  - Run data quality tests

## Workflow Features

- ✅ Automated testing on pull requests
- ✅ Incremental builds using state comparison
- ✅ Separate CI and Production schemas
- ✅ Artifact upload for debugging
- ✅ Documentation generation
- ✅ Production deployment protection

## Manual Workflow Trigger

You can also manually trigger the workflow:
1. Go to Actions tab
2. Select "dbt CI/CD Pipeline"
3. Click "Run workflow"
4. Select branch and run

## Local Development

For local development, continue using your `.zshrc` environment variables:
- `SNOW_ACCOUNT`
- `SNOW_USER`
- `SNOW_USER_PASSWORD`

The local `~/.dbt/profiles.yml` is configured to use the `dev` target with `DEV` schema.

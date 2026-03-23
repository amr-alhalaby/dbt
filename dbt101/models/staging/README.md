# Staging Models

This directory contains staging models that clean and standardize raw data from the source tables.

## Source Tables

The raw data is loaded into Snowflake (`dbt_project.raw` schema) from CSV files using the automated ingestion script at `scripts/snowflake_ingest.py`.

### Available Sources:
- **candidates** - Candidate information with skills and job functions
- **employees** - Employee records with employment details
- **interviews** - Interview records and status tracking
- **job_functions** - Job function master data
- **skills** - Skills master data with hierarchical relationships

## Staging Models

Staging models (`stg_*`) perform light transformations:
- Rename columns to follow naming conventions
- Convert data types (e.g., microseconds → timestamps, strings → booleans)
- Clean and standardize field names
- Handle null values
- No business logic or joins

### Models:
- `stg_candidates.sql` - Cleaned candidate records
- `stg_employees.sql` - Cleaned employee records
- `stg_interviews.sql` - Cleaned interview records
- `stg_job_functions.sql` - Cleaned job function data
- `stg_skills.sql` - Cleaned skills data

## Usage

### Reference sources in models:
```sql
{{ source('raw', 'candidates') }}
```

### Reference staging models:
```sql
{{ ref('stg_candidates') }}
```

### Test source freshness:
```bash
dbt source freshness
```

### Build staging models:
```bash
# Build all staging models
dbt run --select staging

# Build specific model
dbt run --select stg_candidates
```

## Data Pipeline Flow

```
CSV Files (data/)
  → snowflake_ingest.py
  → Snowflake (raw schema)
  → dbt sources (sources.yml)
  → Staging models (stg_*.sql)
  → Marts/Analytics models
```

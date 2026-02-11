-- stg_jobs_acquisition__branch_reports.sql

-- This model is used to stage the raw branch updates data from S3.
-- It processes data from the last 2 days.
-- The branch data is uploaded to S3 every day with a date_dim_key of the 2 days before the current date.

-- this parameter is used for initial load or full refresh
{% set cutoff_date_start_date_dim_key = '20200101' %}

{{
  config(
    materialized='incremental',
    on_schema_change='sync_all_columns',
    incremental_strategy='append',
    partition_by='date_dim_key',
    tblproperties={
      'delta.minReaderVersion': 1,
      'delta.enableDeletionVectors': false
      },
    tags=['schedule__daily__morning'],
  )
}}

-- Find the max date by using only one column
{% if is_incremental() %}
{% set max_date_dim_key = run_query("select max(date_dim_key) from " ~ this ~ "") %}
{% endif %}

with source as (
    -- This ref will be auto-filtered when using the microbatch strategy.
    select *
    from {{ ref('src_s3_external_raw__branch_reports') }}
    where date_dim_key >= {{ cutoff_date_start_date_dim_key }}
        {% if is_incremental() %}
        and date_dim_key > '{{ max_date_dim_key[0][0] }}'
        {% endif %}
)

select
    *,
    {{ audit_columns() }}
from source

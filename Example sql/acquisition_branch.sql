---------------------------------------------------------------------------------------------------------------------------
-- model: jobs_acquisition__branch_jobs
-- domain: jobs_acquisition
--
--
-- This model was migrated from the smaug query
-- DO NOT REMOVE OR MODIFY THIS COMMENT BLOCK
-- cfg_smaug_query_name: branch_jobs.sql
-- cfg_smaug_schema: common
-- cfg_smaug_table: branch_jobs
-- END OF COMMENT BLOCK
---------------------------------------------------------------------------------------------------------------------------

{{
  config(
    materialized='incremental',
    on_schema_change='sync_all_columns',
    incremental_strategy='append',
    partition_by='date_dim_key',
    tblproperties={
      'delta.minReaderVersion': 1,
      'delta.enableDeletionVectors': false,
    },
    tags=['smaug', 'schedule__daily__morning'],
    alias='branch_jobs',
  )
}}

-- Find the max date by using only one column
{% if is_incremental() %}
{% set max_date_dim_key = run_query("select max(date_dim_key) from " ~ this ~ "") %}
{% endif %}

with base_query as (
    select
        last_attributed_touch_data_tilde_channel,
        cast(custom_data:job_id as int) as extracted_job_id,
        user_data_platform,
        date_dim_key,
        case when (lower(
                last_attributed_touch_data_tilde_channel) in (
                'licensedtradeswebsite',
                'get_quotes_ppc_form',
                'facebook',
                'display',
                'newscorp'
            ))
            or (
                lower(last_attributed_touch_data_tilde_channel) in ('apple app store')
                and last_attributed_touch_data_tilde_campaign = 'Non-Brand'
            )
                then 'Performance' when
                lower(last_attributed_touch_data_tilde_channel) in (
                    'email',
                    'sms',
                    'gqnewsletteredm',
                    'mobilepush'
                ) then 'Owned'
            else 'Direct'
        end as branch_channel
    from {{ ref("stg_jobs_acquisition__branch_reports") }}
    where
        name = 'PURCHASE'
        and custom_data like '%"job_id":%'
        {% if is_incremental() %}
        and date_dim_key > '{{ max_date_dim_key[0][0] }}'
        {% endif %}
)

select
    *,
    {{ audit_columns() }}
from base_query

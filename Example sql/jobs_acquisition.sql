------------------------------------------------------------------------------------------------------------------------
-- Model        : fact_jobs_acquisition
-- Description  : Marts layer that publishes directly into DBT
-- Domain       : Jobs acquisition
------------------------------------------------------------------------------------------------------------------------

{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='job_id',
        partition_by='fact_gq_job_last_modified_date',
        invalidate_hard_deletes=True,
        on_schema_change='sync_all_columns',
        tags=['schedule__daily__morning','schedule__daily__noon', 'schedule__daily__evening']
    )
}}
{% set last_modified_limit = '1900-01-01' %}

{% if is_incremental() %}
{% set watermark_query %}
        SELECT MAX(fact_gq_job_last_modified_ts) FROM {{ this }}
{% endset %}

{% if execute %}
{% set results = run_query(watermark_query) %}
{% set last_modified_limit = results.columns[0][0] if results.columns[0][0] is not none else '1900-01-01' %}
{% endif %}
{% endif %}

with jobs as (
    select distinct
        job_id,
        consumer_user_id,
        valid_indicator,
        number_of_claims,
        number_of_paid_claims,
        number_of_free_claims,
        total_claim_value,
        number_of_invited_businesses,
        number_of_invited_freemium_businesses,
        job_source_type,
        growth_channels,
        growth_channel_detail,
        reporting_channel,
        brand_job,
        fact_gq_job_last_modified_ts,
        posted_date_ts,
        effective_start_date,
        job_status,
        job_description,
        conversion_page,
        job_source,
        number_of_hires,
        job_indicator,
        closed_reason,
        closed_status,
        customer_indicator,
        posted_date_dim_key,
        date_dim_key,
        full_date,
        category_key,
        category_seo_key,
        category_id,
        parent_category_id,
        parent_category,
        category_group,
        category_grouping,
        grandparent_category,
        category_grouping_emergency,
        category,
        master_category,
        suburb,
        postcode,
        region,
        state,
        state_code,
        country,
        branch_channel,
        partnership_type,
        partnerpro
    from {{ ref('jobs_acquisition_jobs_dates_branch_category_geo') }}

    {% if is_incremental() %}
    -- Extends lookback buffer to 90-days to the dynamic watermark to catch late-arriving CDC data
    where fact_gq_job_last_modified_ts >= date_sub(cast('{{ last_modified_limit }}' as date), 90)
    {% endif %}
),

final as (
    select distinct
        job_id,
        consumer_user_id,
        valid_indicator,
        number_of_claims,
        number_of_paid_claims,
        number_of_free_claims,
        total_claim_value,
        number_of_invited_businesses,
        number_of_invited_freemium_businesses,
        job_source_type,
        case
            when branch_channel is not null then branch_channel
            when (growth_channel_detail in ('Organic NonBrand') or job_source in ('organic-non-brand')) then 'Non-Brand Organic'
            when
                (
                    growth_channel_detail in ('EDM New', 'Push', 'Transactional EDM Repeat', 'EDM Repeat')
                    or job_source in ('edm', 'push', 'transactional-edm')
                )
                then 'Owned'
            when
                (
                    growth_channel_detail in (
                        'App New',
                        'App Repeat',
                        'Direct',
                        'SEM Brand Repeat',
                        'SEM Brand New',
                        'Direct Brand Repeat',
                        'Direct Brand New',
                        'Organic Brand New',
                        'Organic Brand Repeat',
                        'Inbound Phone',
                        'Outbound Phone'
                    )
                    or job_source in ('sem-brand', 'organic-brand', 'direct')
                )
                then 'Direct'
            when
                (
                    growth_channel_detail in (
                        'SEM Google NonBrand',
                        'SEM Other NonBrand',
                        'SEM Bing NonBrand',
                        'Affiliate',
                        'Facebook Display',
                        'Organic Social',
                        'Start Local Partner'
                    )
                    or job_source in ('display', 'social', 'video', 'affiliate')
                )
                then 'Performance'
            when
                (
                    growth_channel_detail in ('Other Partners', 'Bunnings', 'NSW Department of Education')
                    or job_source in ('other-partners', 'bunnings', 'doe')
                )
                then 'Partners'
            else 'Direct'
        end as marketing_channel,
        growth_channels,
        growth_channel_detail,
        reporting_channel,
        brand_job,
        fact_gq_job_last_modified_ts,
        cast(fact_gq_job_last_modified_ts as date) as fact_gq_job_last_modified_date,
        posted_date_ts,
        effective_start_date,
        job_status,
        job_description,
        conversion_page,
        job_source,
        number_of_hires,
        job_indicator,
        closed_reason,
        closed_status,
        customer_indicator,
        posted_date_dim_key,
        date_dim_key,
        category_key,
        category_seo_key,
        category_id,
        parent_category_id,
        parent_category,
        category_group,
        category_grouping,
        grandparent_category,
        category_grouping_emergency,
        category,
        master_category,
        suburb,
        postcode,
        region,
        state,
        state_code,
        country,
        branch_channel,
        partnership_type,
        partnerpro
    from jobs
    where 1 = 1
        and jobs.valid_indicator = 'valid'
        and jobs.closed_reason != 'Test'
)

select * from final

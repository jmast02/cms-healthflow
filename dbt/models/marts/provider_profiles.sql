-- Mart: one row per provider with aggregated payment and utilisation metrics.
-- Consumed by the FastAPI gold.provider_profiles table and provider comparison UI.

with base as (
    select * from {{ ref('stg_providers') }}
),

aggregated as (
    select
        provider_npi,
        max(provider_full_name)                    as provider_name,
        max(provider_type)                         as provider_type,
        max(provider_state)                        as provider_state,
        max(provider_city)                         as provider_city,
        max(provider_zip5)                         as provider_zip,
        max(provider_gender)                       as provider_gender,
        bool_or(is_medicare_participant)           as medicare_participation,
        count(distinct hcpcs_code)                 as unique_hcpcs_codes,
        count(*)                                   as total_procedures,
        sum(total_beneficiaries)                   as total_beneficiaries,
        sum(total_services)                        as total_services,
        sum(avg_medicare_payment * total_services) as total_medicare_payment,
        avg(avg_medicare_payment)                  as avg_medicare_payment,
        avg(avg_submitted_charge)                  as avg_submitted_charge,
        max(dataset_year)                          as dataset_year
    from base
    group by provider_npi
),

ranked as (
    select
        *,
        rank() over (
            partition by provider_type
            order by avg_medicare_payment desc nulls last
        ) as specialty_rank,
        rank() over (
            partition by provider_state
            order by avg_medicare_payment desc nulls last
        ) as state_rank
    from aggregated
)

select * from ranked

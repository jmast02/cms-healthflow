-- Mart: procedure-level cost analytics broken down by state.
-- Powers the /api/v1/procedures/{hcpcs_code}/costs endpoint.

with base as (
    select * from {{ ref('stg_providers') }}
)

select
    hcpcs_code,
    max(hcpcs_description)                                   as hcpcs_description,
    provider_state,
    count(distinct provider_npi)                             as provider_count,
    sum(total_services)                                      as total_services,
    round(avg(avg_submitted_charge)::numeric, 2)             as avg_submitted_charge,
    round(avg(avg_medicare_payment)::numeric, 2)             as avg_medicare_payment,
    round(percentile_cont(0.5) within group (
        order by avg_medicare_payment
    )::numeric, 2)                                           as median_medicare_payment,
    round(min(avg_medicare_payment)::numeric, 2)             as min_medicare_payment,
    round(max(avg_medicare_payment)::numeric, 2)             as max_medicare_payment,
    round(stddev(avg_medicare_payment)::numeric, 2)          as stddev_medicare_payment,
    max(dataset_year)                                        as dataset_year
from base
group by hcpcs_code, provider_state

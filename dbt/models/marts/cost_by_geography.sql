-- Mart: average Medicare costs rolled up to state + ZIP.
-- Powers the geographic heatmap endpoint and Grafana cost maps.

with base as (
    select * from {{ ref('stg_providers') }}
)

select
    provider_state,
    provider_zip5                                        as provider_zip,
    count(distinct provider_npi)                         as total_providers,
    sum(total_services)                                  as total_services,
    round(avg(avg_medicare_payment)::numeric, 2)         as avg_medicare_payment,
    round(avg(avg_submitted_charge)::numeric, 2)         as avg_submitted_charge,
    max(dataset_year)                                    as dataset_year
from base
where provider_zip5 is not null
group by provider_state, provider_zip5
having count(distinct provider_npi) >= 3   -- suppress ZIPs with too few providers

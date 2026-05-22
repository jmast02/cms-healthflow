-- Staging model: clean and standardise bronze provider claims for mart consumption.
-- Source: bronze.provider_claims (loaded by PySpark normalize job)

with source as (
    select * from {{ source('bronze', 'provider_claims') }}
),

renamed as (
    select
        provider_npi,
        coalesce(provider_name, provider_first_name, 'Unknown')   as provider_full_name,
        provider_type,
        upper(provider_state)                                      as provider_state,
        provider_city,
        left(provider_zip, 5)                                      as provider_zip5,
        provider_gender,
        case
            when medicare_participation = 'Y' then true
            else false
        end                                                        as is_medicare_participant,
        hcpcs_code,
        hcpcs_description,
        cast(total_beneficiaries as bigint)                        as total_beneficiaries,
        cast(total_services as bigint)                             as total_services,
        cast(avg_submitted_charge as numeric(15,2))                as avg_submitted_charge,
        cast(avg_medicare_payment as numeric(15,2))                as avg_medicare_payment,
        cast(avg_medicare_standard as numeric(15,2))               as avg_medicare_standard,
        dataset_year
    from source
    where
        provider_npi is not null
        and trim(provider_npi) != ''
        and total_services > 0
)

select * from renamed

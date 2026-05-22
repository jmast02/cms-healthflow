-- Mart: hospital quality rankings from CMS Hospital Compare dataset.
-- Adds state and national ranks by overall star rating.

with source as (
    select * from {{ source('bronze', 'hospital_compare_raw') }}
    where "Facility ID" is not null
),

cleaned as (
    select
        "Facility ID"                                                     as facility_id,
        initcap("Facility Name")                                          as facility_name,
        "Address"                                                         as address,
        initcap("City/Town")                                              as city,
        upper("State")                                                    as state,
        "ZIP Code"                                                        as zip_code,
        "County/Parish"                                                   as county_name,
        "Hospital Type"                                                   as hospital_type,
        "Hospital Ownership"                                              as hospital_ownership,
        case
            when upper("Emergency Services") = 'YES' then true
            else false
        end                                                               as emergency_services,
        case
            when "Hospital overall rating" in ('Not Available', 'N/A', '')
            then null
            else "Hospital overall rating"::smallint
        end                                                               as overall_rating,
        "Readmission national comparison"                                 as readmission_national,
        "Mortality national comparison"                                   as mortality_national,
        "Safety of care national comparison"                              as safety_national,
        "Patient experience national comparison"                          as patient_experience
    from source
),

ranked as (
    select
        *,
        rank() over (
            partition by state
            order by overall_rating desc nulls last, facility_name
        )                                                                 as state_rank,
        rank() over (
            order by overall_rating desc nulls last, facility_name
        )                                                                 as national_rank
    from cleaned
)

select * from ranked

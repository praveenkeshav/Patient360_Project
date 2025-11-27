{{
  config(
    materialized = 'table',
    on_schema_change = 'fail'
    )
}}


with

src as (

    select
        patient_id,
        upper(trim(full_name)) as full_name,
        upper(trim(gender)) as gender,
        upper(trim(race)) as race,
        upper(trim(full_address)) as full_address,
        primary_contact,
        dob,
        death_date,
        effective_start_date,
        effective_end_date,
        age as age_src,
        load_ts as stg_load_ts

    from {{ ref("stg_patients") }}    
),

transformed as (

    select
        *,
        --prefer age from staging, otherwise compute from DOB
        coalesce(
            age_src,
            datediff(year, dob, current_date)
        ) as age_years
    from src    
),

final as (

    select

        {{ dbt_utils.generate_surrogate_key([
            'patient_id',
            'effective_start_date'
        ]) }} as patient_sk,

        patient_id,
        full_name,
        gender,
        race,
        full_address,
        primary_contact,
        dob,
        age_years,

        case
            when age_years is null then 'UNKNOWN'
            when age_years < 18 then '00-17'
            when age_years between 18 and 34 then '18-34'
            when age_years between 35 and 49 then '35-49'
            when age_years between 50 and 64 then '50-64'
            else '65+'
        end as age_band,

        death_date,
        effective_start_date,
        effective_end_date,

        case
            when death_date is not null then 1
            else 0
        end as is_deceased,

        case
            when effective_end_date is null
                or effective_end_date > current_date
            then 1
            else 0
        end as is_current,

        stg_load_ts

    from transformed            

)

select * from final


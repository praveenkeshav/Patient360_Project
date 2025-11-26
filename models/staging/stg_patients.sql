with

source as (

    select * from {{ source('ehr', 'patients_raw') }}

),

staged as (

    select
        patient_id,
        first_name || ' ' || last_name as full_name,
        date_of_birth as dob,
        case 
          when lower(trim(sex)) = 'male' then 'M'
          when lower(trim(sex)) = 'female' then 'F'
          else 'U'
        end as gender,
        {{ calculate_age('date_of_birth') }} as age,
        race,
        concat(address, ', ', city, ', ', state, ' ', postal_code) as full_address,
        phone as primary_contact,
        effective_start_date as effective_start_date,
        effective_end_date as effective_end_date,
        death_of_date as death_date,
        load_ts
    from source
)

select * from staged


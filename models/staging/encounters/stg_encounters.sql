with

source as (
    select * from {{ source('ehr', 'encounters_raw') }}
),

staged as (

    select

        upper(trim(encounter_id)) as encounter_id,
        upper(trim(patient_id)) as patient_id,
        admit_datetime,
        discharge_datetime,

        case lower(encounter_type)
            when 'inpatient' then 'inpatient'
            when 'observation' then 'observation'
            when 'er' then 'emergency'
            when 'outpatient' then 'outpatient'
            else 'other'
        end as encounter_type,

        case lower(discharge_disposition)
            when 'home' then 'home'
            when 'snf' then 'skilled_nursing_facility'
            when 'ama' then 'against_medical_advice'
            when 'rehab' then 'rehabilitation'
            when 'expired' then 'expired'
            else 'other'
        end as discharge_disposition,

        trim(facility_id) as facility_id,
        trim(provider_id) as provider_id,
        trim(payer_id) as payer_id,
        upper(trim(drg_code)) as drg_code,

        --helper date fields
        date_trunc('day', admit_datetime)::date as admit_date,
        date_trunc('day', discharge_datetime)::date as discharge_date,
        load_ts

    from source

)

select * from staged
            




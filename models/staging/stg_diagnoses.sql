with

source as (

select * from {{ source('ehr', 'diagnoses_raw') }}

),

staged as (

    select
        diagnosis_id,
        encounter_id,
        patient_id,
        diagnosis_code,
        trim(diagnosis_name) as diagnosis_name,
        upper(trim(diagnosis_type)) as diagnosis_type,
        upper(trim(present_on_admission)) as present_on_admission_raw,
        cast(diagnosis_datetime as timestamp_ntz) as diagnosis_datetime,
        current_timestamp() as load_ts

    from source
),

clean as (
    select
        diagnosis_id,
        encounter_id,
        patient_id,
        diagnosis_code,
        diagnosis_name,
        diagnosis_type,

        case
          when present_on_admission_raw in('Y', 'N', 'U') then present_on_admission_raw
          when present_on_admission_raw is null then 'U'
          else 'U'
        end as present_on_admission_flag,
        diagnosis_datetime,
        to_date(diagnosis_datetime) as diagnosis_date,
        load_ts
        
    from staged      
),

dedup as (
    select
        *,
        row_number() over(
            partition by diagnosis_id
            order by diagnosis_datetime desc
        ) rn
    from clean
)

select
    {{ dbt_utils.generate_surrogate_key(['diagnosis_id']) }} as diagnosis_sk,
    diagnosis_id,
    encounter_id,
    patient_id,
    diagnosis_code,
    diagnosis_name,
    diagnosis_type,
    present_on_admission_flag,
    diagnosis_datetime,
    diagnosis_date,
    load_ts

from dedup
where rn =  1





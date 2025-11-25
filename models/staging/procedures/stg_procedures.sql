with

source as (

    select * from {{ source('ehr', 'procedures_raw') }}

),

staged as (

    select

        procedure_id,
        encounter_id,
        patient_id,
        trim(procedure_code) as procedure_code,
        trim(code_system) as code_system,
        procedure_name,
        procedure_datetime,
        current_timestamp() as load_ts

    from source    
)

select * from staged
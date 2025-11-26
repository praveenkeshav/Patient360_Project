with 

source as (
    select * 
    from {{ source('ehr', 'lab_raw') }}

),

staged as (
    select
        --top level
        raw.data:patient_id::string as patient_id,
        --encounter level
        enc.value:encounter_id::string as encounter_id,

        --lab level
        lab.value:lab_result_id::string as lab_result_id,
        to_date(lab.value:test_date::string) as test_date,
        lab.value:test:loinc_code::string as loinc_code,
        lab.value:test:test_name::string as test_name,

        --result nested object
        lab.value:result.value::float as result_value,
        lab.value:result.unit::string as result_unit,
        lab.value:result.reference_range::string as reference_range,
        current_timestamp() as load_ts

    from source as raw,
         lateral flatten(input => raw.data:encounters) enc,
         lateral flatten(input => enc.value:labs) lab
)

select * from staged
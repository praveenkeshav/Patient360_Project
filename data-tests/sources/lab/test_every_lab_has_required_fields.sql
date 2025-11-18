-- Ensures every lab result has all required fields
with bad_labs as (
    select
        raw.load_ts
    from {{ source('ehr', 'lab_raw') }} as raw
    , lateral flatten(input => raw.data:encounters) as enc
    , lateral flatten(input => enc.value:labs) as lab
    where lab.value:lab_result_id is null
       or trim(lab.value:lab_result_id::string) = ''
       or lab.value:test_date is null
       or lab.value:test:loinc_code is null
       or trim(lab.value:test:loinc_code::string) = ''
       or lab.value:result:value is null
)

select * from bad_labs
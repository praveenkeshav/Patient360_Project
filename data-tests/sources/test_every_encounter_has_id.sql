-- Ensures every encounter in the JSON has an encounter_id
with flattened as (
    select
        raw.load_ts
    from {{ source('ehr', 'lab_raw') }} as raw
    , lateral flatten(input => raw.data:encounters) as enc
    where enc.value:encounter_id is null
      or trim(enc.value:encounter_id::string) = ''
)

select * from flattened
-- Every row must have a non-null patient_id at the root of the JSON
select *
from {{ source('ehr', 'lab_raw') }}
where data:patient_id is null
   or data:patient_id::string = ''
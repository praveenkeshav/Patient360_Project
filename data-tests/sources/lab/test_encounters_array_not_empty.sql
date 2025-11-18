-- Every document must have at least one encounter
select *
from {{ source('ehr', 'lab_raw') }}
where get(data:encounters, 0) is null  -- i.e. array is empty or missing
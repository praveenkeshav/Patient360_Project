select
    age
from {{ ref('stg_patients') }}
where age < 21 or age > 80
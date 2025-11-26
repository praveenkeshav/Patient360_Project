with

source as (

    select * from {{ source('ehr', 'claims_raw') }}
),

staged as (

    select
        claim_id,
        patient_id,
        encounter_id,
        claim_date,
        billed_amount,
        paid_amount,
        allowed_amount,
        current_timestamp() as load_ts
        
    from source    
)

select * from staged
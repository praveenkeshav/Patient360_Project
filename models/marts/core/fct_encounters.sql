with

enc as (

    select

        encounter_id,
        patient_id,
        provider_id,
        payer_id,
        facility_id,

        admit_date,
        admit_ts,
        discharge_date,
        discharge_ts,

        encounter_type,
        discharge_disposition,

        drg_code,
        drg_group,
        drg_mdc,
        drg_name,
        drg_severity,

        los_days,
        index_discharge_candidate,
        is_discharge,
        stg_load_ts

    from {{ ref("int_encounters")}}

),

enc_with_patient as (

    select

        e.*,
        dp.patient_sk,
    from enc e
    left join {{ ref("dim_patients") }} dp
        on e.patient_id = dp.patient_id
      and dp.is_current = 1      
),

enc_with_provider as (

    select
        ep.*,
        dpr.provider_sk
    from enc_with_patient ep
    left join {{ ref("dim_providers") }} dpr
        on ep.provider_id = dpr.provider_id

),

final as (

    select

        encounter_id,
        patient_id,
        provider_id,
        payer_id,
        facility_id,

        patient_sk,
        provider_sk,

        admit_date,
        admit_ts,
        discharge_date,
        discharge_ts,

        encounter_type,
        discharge_disposition,
        drg_code,
        drg_group,
        drg_mdc,
        drg_name,
        drg_severity,

        los_days,

        index_discharge_candidate,
        is_discharge,

        stg_load_ts

    from enc_with_provider
)

select * from final


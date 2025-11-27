with

dx as (

    select
        diagnosis_id,
        encounter_id,
        patient_id,

        diagnosis_code,
        diagnosis_code_norm,
        diagnosis_name,
        diagnosis_type,
        diagnosis_date,

        present_on_admission,
        is_present_on_admission,
        is_principal_dx,

        condition_group,
        charlson_condition_group,
        has_charlson_condition,
        charlson_weight,
        is_chronic,

        stg_load_ts as dx_stg_load_ts

    from {{ ref("int_diagnoses") }}

),

dx_with_encounter as (

    select
        dx.*,

        --from core fct_encounter
        fe.patient_sk,
        fe.provider_sk,

        fe.encounter_type,
        fe.discharge_disposition,
        fe.drg_code,
        fe.drg_group,
        fe.drg_mdc,
        fe.drg_name,
        fe.drg_severity,
        fe.los_days,

        fe.index_discharge_candidate,
        fe.is_discharge,
        fe.admit_date,
        fe.discharge_date,
        fe.facility_id,
        fe.payer_id,

        fe.stg_load_ts as enc_stg_load_ts

    from dx
    join {{ ref("fct_encounters") }} fe
      on dx.encounter_id = fe.encounter_id    
),

final as (

    select
        --natural and surrogate keys
        encounter_id,
        diagnosis_id,
        patient_id,
        patient_sk,
        provider_sk,

        --encounter context
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
        facility_id,
        payer_id,

        --diagnosis details
        diagnosis_code,
        diagnosis_code_norm,
        diagnosis_name,
        diagnosis_type,
        diagnosis_date,

        present_on_admission,
        is_present_on_admission,
        is_principal_dx,

        --condition or comorbidity info
        condition_group,
        charlson_condition_group,
        has_charlson_condition,
        charlson_weight,
        is_chronic,

        --lineage
        dx_stg_load_ts,
        enc_stg_load_ts

    from dx_with_encounter

)

select * from final


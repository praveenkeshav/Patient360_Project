{{ config(
    materialized     = 'table',
    on_schema_change = 'sync_all_columns'
) }}

with proc as (

    select
        -- procedure grain
        procedure_id,
        encounter_id,
        patient_id,

        procedure_code,
        code_system,
        procedure_name,
        procedure_datetime,

        procedure_category,
        is_initial_visit,
        is_inpatient_applicable,
        is_outpatient_applicable,
        is_planned_exclusion,
        is_surgical,
        stg_load_ts as proc_stg_load_ts

    from {{ ref('int_procedures') }}
),

proc_with_encounter as (

    select
        p.*,

        -- from core encounter fact
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

    from proc p
    left join {{ ref('fct_encounters') }} fe
      on p.encounter_id = fe.encounter_id
),

final as (

    select
        -- grain keys
        encounter_id,
        procedure_id,

        -- natural & surrogate keys
        patient_id,
        patient_sk,
        provider_sk,

        -- encounter context (minimal, needed for downstream KPIs)
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
        admit_date,
        discharge_date,
        facility_id,
        payer_id,

        -- procedure detail
        procedure_code,
        code_system,
        procedure_name,
        procedure_datetime,
        procedure_category,
        is_initial_visit,
        is_inpatient_applicable,
        is_outpatient_applicable,
        is_planned_exclusion,
        is_surgical,

        -- lineage
        proc_stg_load_ts,
        enc_stg_load_ts
    from proc_with_encounter
)

select * from final
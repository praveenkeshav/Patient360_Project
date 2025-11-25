{{
  config(
    materialized = 'incremental',
    unique_key = 'encounter_id',
    tags = ["int", "encounters"]
    )
}}

with

src as (

    select
        encounter_id,
        patient_id,

        admit_datetime as admit_ts,
        discharge_datetime as discharge_ts,
        admit_date,
        discharge_date,
        encounter_type,
        discharge_disposition,
        drg_code,
        payer_id,
        facility_id,
        provider_id,
        load_ts as stg_load_ts

    from {{ ref('stg_encounters') }} 
),

final as (

    select
        s.encounter_id,
        s.patient_id,
        s.admit_ts,
        s.discharge_ts,
        s.admit_date,
        s.discharge_date,

        case
          when s.admit_ts is not null and s.discharge_ts is not null
          then datediff(day, s.admit_ts, s.discharge_ts)
          else null
        end as los_days,

        s.encounter_type,
        s.discharge_disposition,
        s.drg_code,
        s.facility_id,
        s.provider_id,
        s.payer_id,

        drg.drg_name as drg_name,
        drg.drg_group as drg_group,
        drg.drg_severity as drg_severity,
        drg.mdc as drg_mdc,

        --simple flags
        case when s.discharge_ts is not null then true else false end as is_discharge,
        case when s.discharge_ts is not null then true else false end as index_discharge_candidate,

        s.stg_load_ts

    from src s

    left join {{ ref('drg_codes') }} drg
        on s.drg_code = drg.drg_code

)

select * from final

{% if is_incremental() %}
where coalesce(stg_load_ts, current_timestamp()) >= (
    select coalesce(max(stg_load_ts), '1900-01-01') from {{ this }}
)
{% endif %}
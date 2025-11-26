with

diagnoses as (

    select
        diagnosis_id,
        encounter_id,
        patient_id,
        diagnosis_code,
        diagnosis_name,
        diagnosis_type,
        present_on_admission_flag as present_on_admission,
        diagnosis_date,
        load_ts as stg_load_ts,
        upper(replace(diagnosis_code, '.', '')) as diagnosis_code_norm

    from {{ ref("stg_diagnoses") }}

),

--ICD to clinical grouping (Primary for ChronicCare)

icd_mapped as (

    select
        d.*,
        cg.condition_group
    from diagnoses d
    left join {{ ref("icd_to_condition_group") }} as cg
        on d.diagnosis_code_norm = upper(replace(cg.icd_code, '.', ''))   

),

with_charlson as (

    select
        i.*,
        cw.condition_group as charlson_condition_group,
        cw.weight as charlson_weight

    from icd_mapped i
    left join {{ ref("charlson_weights") }} cw
        on i.diagnosis_code_norm = upper(replace(cw.icd_codes, '.', ''))

),

final as (

    select
        diagnosis_id,
        encounter_id,
        patient_id,

        diagnosis_code,
        diagnosis_code_norm,
        diagnosis_name,
        diagnosis_type,
        present_on_admission,
        diagnosis_date,
        stg_load_ts,

--flags
        case
          when upper(diagnosis_type) = 'PRIMARY' then 1
          when upper(diagnosis_type) = 'SECONDARY' then 0
          else null
        end as is_principal_dx,

        case
          when present_on_admission in('Y', 'YES', '1') then 1
          when present_on_admission in('N', 'NO', '0') then 0
          else null
        end as is_present_on_admission,

        condition_group,

        case
          when condition_group in ('CHF', 'COPD', 'DM', 'CKD', 'HTN') then 1
          when condition_group is null then null
          else 0
        end as is_chronic,

        charlson_condition_group,
        charlson_weight,

        case
          when charlson_weight is not null
            and charlson_weight > 0
          then 1
          else 0
        end as has_charlson_condition

    from with_charlson
)

select * from final
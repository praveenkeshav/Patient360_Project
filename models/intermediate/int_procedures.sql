with

procedures as (

    select

        procedure_id,
        encounter_id,
        patient_id,

        upper(trim(procedure_code)) as procedure_code,
        upper(trim(code_system)) as code_system,
        upper(trim(procedure_name)) as procedure_name,

        procedure_datetime,
        load_ts as stg_load_ts

    from {{ ref("stg_procedures") }}    
),

transformed as (

    select
        p.*,
        c.procedure_category,
        c.is_initial_visit,
        c.is_inpatient_applicable,
        c.is_outpatient_applicable,
        c.is_planned_exclusion,
        c.is_surgical,
        c.short_description
    from procedures p
    left join {{ ref("cpt_hcpcs_codes") }} c
        on p.procedure_code = upper(trim(c.procedure_code))
       and p.code_system = upper(trim(c.code_system))   

),

final as (

    select

        procedure_id,
        encounter_id,
        patient_id,

        procedure_code,
        code_system,
        procedure_name,
        procedure_datetime,

        --reference/classification
        procedure_category,
        is_initial_visit,
        is_inpatient_applicable,
        is_outpatient_applicable,
        is_planned_exclusion,
        is_surgical,
        short_description,
        stg_load_ts
    from transformed
)

select * from final
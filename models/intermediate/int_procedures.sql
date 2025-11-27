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
        c.procedure_group,
        c.default_cost_estimate,
        c.default_place_of_service,
        c.is_initial_visit,
        c.is_inpatient_applicable,
        c.is_outpatient_applicable,
        c.is_planned_exclusion,
        c.is_surgical,
        c.rvu,
        c.short_description,
        c.long_description,
        c.source as cpt_source,
        c.updated_at as cpt_updated_at

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
        procedure_group,
        default_cost_estimate,
        default_place_of_service,
        is_initial_visit,
        is_inpatient_applicable,
        is_outpatient_applicable,
        is_planned_exclusion,
        is_surgical,
        rvu,
        short_description,
        long_description,
        cpt_source,
        cpt_updated_at,
        stg_load_ts,

    from transformed    

)

select * from final
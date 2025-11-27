with

labs as (

    select

        lab_result_id,
        encounter_id,
        patient_id,
        loinc_code,
        upper(trim(loinc_code)) as loinc_code_norm,
        test_name,
        test_date,
        result_value,
        result_unit,
        reference_range,
        load_ts as stg_load_ts,

    from {{ ref("stg_lab_results") }}    
),

--enrich with LOINC metadata (test name, class, normal ranges, canonical unit)

loinc_enriched as (

    select

        l.*,
        lc.test_name as loinc_test_name,
        lc.loinc_class,
        lc.property,
        lc.scale,
        lc.specimen,
        lc.canonical_unit as loinc_canonical_unit,
        lc.normal_range_low,
        lc.normal_range_high,
        lc.normal_range_unit,
        lc.ucum_unit as loinc_ucum_unit

    from labs l
    left join {{ ref("loinc_codes") }} lc
        on l.loinc_code_norm = upper(trim(lc.loinc))
),

--standardise result units using UCUM mapping

units_normalised as (

    select

        le.*,
        uu.canonical_unit as result_canonical_unit,
        uu.ucum_code,
        uu.is_preferred

    from loinc_enriched le
    left join {{ ref("ucum_units")}} uu
        on upper(trim(le.result_unit)) = upper(trim(uu.unit_raw))    
),

--derive result flag (HIGH/LOW/NORMAL) using normal ranges when available

flagged as (

    select

        lab_result_id,
        encounter_id,
        patient_id,
        loinc_code_norm as loinc_code,

        --prefer LOINC test name when available, else raw

        coalesce(loinc_test_name, test_name) as test_name,
        test_date,
        result_value,
        result_unit,
        result_canonical_unit,
        reference_range,
        stg_load_ts,
        loinc_class,
        property,
        scale,
        specimen,
        loinc_canonical_unit,
        loinc_ucum_unit,
        normal_range_low,
        normal_range_high,
        normal_range_unit,
        ucum_code,
        is_preferred,

        case
          when result_value is not null
           and normal_range_low is not null
           and normal_range_high is not null
           --basic unit compatibility check; can refine later
           and (
                 coalesce(result_canonical_unit, result_unit)
                 = coalesce(normal_range_unit, loinc_canonical_unit)
                or normal_range_unit is null
              )
            then case
                   when result_value < normal_range_low then 'LOW'
                   when result_value > normal_range_high then 'HIGH'
                   else 'NORMAL'
                end
            else null
        end as result_flag

    from units_normalised

),

final as (

    select

        lab_result_id,
        encounter_id,
        patient_id,
        loinc_code,
        test_name,
        test_date,
        result_value,
        result_unit,
        result_canonical_unit,
        reference_range,
        stg_load_ts,

        loinc_class,
        property,
        scale,
        specimen,
        loinc_canonical_unit,
        normal_range_low,
        normal_range_high,
        normal_range_unit,
        loinc_ucum_unit,
        is_preferred,

        result_flag,
        case
          when result_flag in('HIGH', 'LOW') then 1
          when result_flag = 'NORMAL' then 0
          else null
        end as is_abnormal

    from flagged
)

select * from final

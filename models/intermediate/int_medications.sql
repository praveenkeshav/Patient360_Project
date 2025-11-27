with

medications as (

    select

        medication_sk,
        medication_id,
        patient_id,
        encounter_id,
        prescribing_provider_id,

        --dates and quantities
        fill_date,
        quantity,
        days_supply,
        refills,

        --drug text fields (normalized)
        upper(trim(generic_name)) as generic_name,
        upper(trim(brand_name)) as brand_name,
        upper(trim(dosage)) as dosage,
        upper(trim(dosage_form)) as dosage_form,
        upper(trim(dosage_unit)) as dosage_unit,
        upper(trim(route)) as route,

        --coding system
        upper(trim(atc_code)) as atc_code_source,
        rxcui as rxcui_raw,
        try_to_number(rxcui) as rxcui_num,

        --metadata
        load_ts as stg_load_ts

    from {{ ref("stg_medications") }}    

),

--Enrich from RxNorm basic reference (normalize generic name)
rxnorm_enriched as (

    select

        m.*,
        rc.generic_name as rxnorm_generic_name

    from medications m
    left join {{ ref("rxnorm_codes") }} rc
        on m.rxcui_num = rc.rxcui

),

--map RXCUI to ATC (if ATC missing or to add ATC attributes)
rxcui_atc as (

    select

        r.*,

        --prefer ATC from staging; if null, take from mapping
        coalesce(r.atc_code_source, ram.atc_code) as atc_code_final,

        ram.atc_level,
        ram.atc_level_1,
        ram.atc_level_2,
        ram.atc_level_3,
        ram.atc_level_4,
        ram.atc_level_5,
        ram.atc_name as atc_name_from_map

    from rxnorm_enriched r
    left join {{ ref("rxcui_atc_mapping") }} ram
        on r.rxcui_num = ram.rxcui

),

--Join to ATC master for high-level class names

with_atc_dim as (

    select
        a.*,
        ac.atc_level1 as atc_level1_name,
        ac.atc_level2 as atc_level2_name,
        ac.atc_name as atc_name_dim

    from rxcui_atc a
    left join {{ ref("atc_codes") }} ac
        on a.atc_code_final = ac.atc_code

),

final as (

    select

        --keys
        medication_sk,
        medication_id,
        patient_id,
        encounter_id,
        prescribing_provider_id,

        --dates & quantities
        fill_date,
        quantity,
        days_supply,
        refills,

        --drug identity
        generic_name,
        brand_name,
        dosage,
        dosage_form,
        dosage_unit,
        route,

        --coding and reference
        rxcui_raw,
        rxcui_num,
        rxnorm_generic_name,

        atc_code_source,
        atc_code_final,
        atc_level,
        atc_level_1,
        atc_level_2,
        atc_level_3,
        atc_level_4,
        atc_level_5,
        atc_name_from_map,
        atc_level1_name,
        atc_level2_name,
        atc_name_dim,

        --metadata
        stg_load_ts

    from with_atc_dim    
)

select * from final
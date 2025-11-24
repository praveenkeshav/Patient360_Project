with

source as (

    select * from {{ source('ehr', 'medications_raw') }}
),

staged as (

        select
            upper(trim(medication_id)) as medication_id,
            upper(trim(patient_id)) as patient_id,
            upper(trim(encounter_id)) as encounter_id,

            cast(fill_date as date) as fill_date,

            trim(rxcui) as rxcui,
            trim(ndc)::varchar as ndc,
            upper(trim(ATC_code)) as atc_code,

            trim(generic_name) as generic_name,
            trim(brand_name) as brand_name,

            upper(trim(dosage_form)) as dosage_form,

            split_part(strength, ' ', 1) as dosage,
            split_part(strength, ' ', 2) as dosage_unit,
            upper(trim(route)) as route,

            cast(quantity as number) as quantity,
            cast(days_supply as number) as days_supply,
            cast(refills as number) as refills,

            upper(trim(prescribing_provider_id)) as prescribing_provider_id,
            current_timestamp as load_ts
        from source    
),

dedup as (

    select
        *,
        row_number() over(
            partition by medication_id
            order by fill_date desc
        ) as rn
    from staged    
)

select
    {{ dbt_utils.default__generate_surrogate_key(['medication_id']) }} as medication_sk,
     
     medication_id,
     patient_id,
     encounter_id,
     fill_date,
     rxcui,
     atc_code,
     generic_name,
     brand_name,
     dosage_form,
     dosage,
     dosage_unit,
     route,
     quantity,
     days_supply,
     refills,
     prescribing_provider_id,
     load_ts
from dedup
where rn = 1

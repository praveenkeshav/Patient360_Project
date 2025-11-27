{{
    config(
        materialized = 'table'
    )
}}

with

src as (

    select
        provider_id,
        upper(trim(full_name)) as full_name,
        upper(trim(specialty)) as specialty,
        facility_id,
        npi,
        npi_valid,
        is_active,
        source_id,
        upper(trim(source_system)) as source_system,
        loaded_at,
        record_hash as src_record_hash,
        provider_sk as src_provider_sk

    from {{ ref("providers") }}

),

final as (

    select

        {{ dbt_utils.generate_surrogate_key(['provider_id']) }} as provider_sk,
        provider_id,
        full_name,
        specialty,
        facility_id,
        npi,
        npi_valid,
        is_active,
        source_id,
        source_system,
        loaded_at,
        src_record_hash,
        src_provider_sk
        
    from src     
)

select * from final
{{
    config(
        materialized='incremental',
        unique_key=['tenant_id', 'legacy_id'],
        alias='person',
        on_schema_change='append_new_columns'
    )
}}

-- Incremental dbt model: merge staging to person
-- Preserves modernized_only field during updates

WITH staging_records AS (
    SELECT
        tenant_id,
        source_id as legacy_id,
        firstname,
        surname,
        date_of_birth,
        city,
        -- Map data_2 (Legacy A) or data_c (Legacy B) to extra_field
        COALESCE(data_2, data_c) as extra_field,
        created_at,
        CURRENT_TIMESTAMP as synced_at
    FROM person_staging
    WHERE processed = FALSE
)

SELECT
    s.tenant_id,
    s.legacy_id,
    s.firstname,
    s.surname,
    s.date_of_birth,
    s.city,
    s.extra_field,
    s.synced_at,
    s.created_at,
    CURRENT_TIMESTAMP as updated_at,
    -- Preserve modernized_only from existing record, or NULL for new records
    {% if is_incremental() %}
        COALESCE(p.modernized_only, NULL) as modernized_only
    {% else %}
        NULL::date as modernized_only
    {% endif %}
FROM staging_records s
{% if is_incremental() %}
    LEFT JOIN {{ this }} p
    ON s.tenant_id = p.tenant_id AND s.legacy_id = p.legacy_id
{% endif %}

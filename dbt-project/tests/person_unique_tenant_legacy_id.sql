-- Fails if (tenant_id, legacy_id), the incremental key of person_unified, appears more than once
SELECT
    tenant_id,
    legacy_id,
    COUNT(*) AS row_count
FROM {{ ref('person_unified') }}
GROUP BY tenant_id, legacy_id
HAVING COUNT(*) > 1

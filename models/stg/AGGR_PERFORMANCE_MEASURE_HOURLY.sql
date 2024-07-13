with all_data as (
    select * from {{ ref('marts__aggregated')}}
),
final as (
    SELECT
    DATE_TRUNC('hour', FIRST_ATTEMPT_DT_TM) AS START_HOURLY_SLOT_DT_TM,
    COUNT(CASE WHEN STATUS = 'failures' THEN 1 END) AS FAILURE_COUNT,
    COUNT(CASE WHEN STATUS = 'success' THEN 1 END) AS SUCCESS_COUNT,
    COUNT(CASE WHEN STATUS = 'exceptions' THEN 1 END) AS EXCEPTION_COUNT,
    COUNT(CASE WHEN STATUS = 'exclusions' THEN 1 END) AS EXCLUSION_COUNT,
    '{{invocation_id}}' as BATCH_FILE_AUDIT_KEY
    FROM all_data
    GROUP BY DATE_TRUNC('hour', FIRST_ATTEMPT_DT_TM)
)
select * from final
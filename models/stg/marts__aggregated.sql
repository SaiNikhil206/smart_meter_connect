with success as (
    select * from {{ ref('int__pm_s')}}
),
failures as (
    select * from {{ ref('int__pm_failures')}}
),
exclusions as (
    select * from {{ ref('int__pm_exclusions')}}
),
exceptions as (
    select * from {{ ref('int__pm_exceptions')}}
),
final as (
    select commshub_key,LATEST_VALID_CHSU_REPORT_KEY,PM_EVENT_KEY_DT as FIRST_ATTEMPT_DT_TM,INCIDENT_NUMBER,COMMSHUB_CD,REGION,PM_FLAG,exception_cd, 'success' as status from success
    UNION all
    select commshub_key,LATEST_VALID_CHSU_REPORT_KEY,PM_EVENT_KEY_DT as FIRST_ATTEMPT_DT_TM,INCIDENT_NUMBER,COMMSHUB_CD,REGION,PM_FLAG,exception_cd, 'exceptions' as status from exceptions
    UNION all
    select commshub_key,LATEST_VALID_CHSU_REPORT_KEY,PM_EVENT_KEY_DT as FIRST_ATTEMPT_DT_TM,INCIDENT_NUMBER,COMMSHUB_CD,REGION,PM_FLAG,exception_cd, 'failures' as status from failures
    UNION all
    select commshub_key,LATEST_VALID_CHSU_REPORT_KEY,PM_EVENT_KEY_DT as FIRST_ATTEMPT_DT_TM,INCIDENT_NUMBER,COMMSHUB_CD,REGION,PM_FLAG,exception_cd, 'exclusions' as status from exclusions
    
)
select * from final
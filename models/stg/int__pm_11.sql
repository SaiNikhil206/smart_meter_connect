with pm_f as (
    select * from {{ ref('int__pm_f')}}
),
commshub as (
    select * from {{ ref('stg__commshub')}}
),
exception as (
    select * from {{ ref('stg__exception')}} 
),
overrides as (
    select * from {{ ref('stg__override')}}
),
adhoc_hub as (
    select 
    c.COMMSHUB_KEY, 
    e.exception_code as adhoc_hub_exc_code 
    from commshub c join exception e on c.commshub_key=e.commshub_key where (affected_pm = 'PM 11') and FIRST_VALID_CHSU_RECEIVED_DATE between e.effective_from_dt_tm and e.effective_to_dt_tm and DELETE_FLAG = 'N'
),
bau_hub as (
     select 
     c.COMMSHUB_KEY,o.exception_code as bau_hub_exc_code from commshub c 
     join overrides o on c.commshub_key=o.commshub_key where (affected_pm = 'PM 11') and FIRST_VALID_CHSU_RECEIVED_DATE between o.effective_from_dt_tm and o.effective_to_dt_tm
),
joined as (
    SELECT 
    pm_f.COMMSHUB_KEY,
    pm_f.COMMSHUB_CD,
	FIRST_CONNECTED_DATE,
	FIRST_VALID_CHSU_RECEIVED_DATE,
	TEST_HUB,
	NO_OF_CHSUS,
	NO_OF_VALID_CHSUS,
	LATEST_CHSU_REPORT_KEY,
	LATEST_VALID_CHSU_REPORT_KEY,
	FIRST_VALID_CHSU_REPORT_KEY,
	FIRST_CHSU_REPORT_KEY,
	CHSU_KEY,
	LATEST_VALID_JOB_TYPE,
	INSTALLED_DATE,
	FIRST_CHSU_RECEIVED_DATE,
	CHSU_REPORT_KEY,
	LOAD_DT_TM,
	REGION,
	PM_EVENT_KEY_DT,
	INCIDENT_NUMBER,
     CASE
            WHEN adhoc_hub_exc_code IS NOT NULL THEN adhoc_hub_exc_code
            WHEN bau_hub_exc_code IS NOT NULL THEN bau_hub_exc_code
            ELSE NULL
        END AS exception_cd
    FROM 
        pm_f
    LEFT JOIN 
        adhoc_hub ah 
        ON pm_f.commshub_key = ah.commshub_key
    LEFT JOIN 
        bau_hub bh
        ON pm_f.commshub_key = bh.commshub_key
),
final as (
    SELECT
        *,
        CASE
            WHEN exception_cd IS NOT NULL THEN 'E'
            ELSE 'F'
        END AS PM_FLAG
    FROM
        joined
)
select * from final

with chsu_data as (
    select * from {{ source('smart_meters_files','chsu')}}
),
final as (
    select CHSU_KEY, JOB_TYPE, INSTALLED_DATE, CHSU_RECEIVED_DATE, INCIDENT_NUMBER from chsu_data
)
select * from final
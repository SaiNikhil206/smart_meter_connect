with commshub as (
    select * from {{ source('smart_meters_files','commshub')}}
),
final as (
    select COMMSHUB_KEY,COMMSHUB_CD, FIRST_CONNECTED_DATE, FIRST_VALID_CHSU_RECEIVED_DATE,REGION, TEST_HUB, NO_OF_CHSUS,NO_OF_VALID_CHSUS, LATEST_CHSU_REPORT_KEY, LATEST_VALID_CHSU_REPORT_KEY,FIRST_VALID_CHSU_REPORT_KEY,FIRST_CHSU_REPORT_KEY from commshub
)
select * from final
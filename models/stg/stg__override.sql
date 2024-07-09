with exception as (
    select * from {{ source('smart_meters_files','override')}}
)
select * from exception
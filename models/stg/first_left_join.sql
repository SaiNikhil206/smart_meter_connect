{% set report_start_date = "date_trunc('month', current_date)" %}
{% set report_end_date = "date_trunc('month',current_date) + interval '1 month' - interval '1 day'" %}

with commshub as (
    select * from {{ ref('stg__commshub')}}
),
chsu as (
    select * from {{ ref('stg__chsu')}}
),
joined as (
    select *, ch.CHSU_RECEIVED_DATE as FIRST_CHSU_RECEIVED_DATE,  from commshub c LEFT JOIN CHSU ch on coalesce(c.FIRST_VALID_CHSU_REPORT_KEY,c.FIRST_CHSU_REPORT_KEY) = ch.CHSU_KEY where ch.chsu_key is NOT NULL
),
revised as (
    select COMMSHUB_KEY,
        INSTALLED_DATE,
        FIRST_CHSU_RECEIVED_DATE as FIRST_VALID_CHSU_RECEIVED_DATE,
        FIRST_CONNECTED_DATE,
        REGION,
        TEST_HUB,
        NO_OF_CHSUS,
        NO_OF_VALID_CHSUS,
        LATEST_CHSU_REPORT_KEY,
        coalesce(LATEST_VALID_CHSU_REPORT_KEY,LATEST_CHSU_REPORT_KEY) as LATEST_VALID_CHSU_REPORT_KEY,
        coalesce(FIRST_VALID_CHSU_REPORT_KEY,FIRST_CHSU_REPORT_KEY) as FIRST_VALID_CHSU_REPORT_KEY,
        FIRST_CHSU_REPORT_KEY,
        CHSU_KEY,
        JOB_TYPE,
        FIRST_CHSU_RECEIVED_DATE 
        from joined
)
select * from revised
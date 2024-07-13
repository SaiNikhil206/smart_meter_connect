with criteria as (
    SELECT * from {{ ref('int__data_transform')}}
),
final as (
    select *,
        case
            WHEN DATE_TRUNC('second',CAST(first_connected_date AS TIMESTAMP_TZ)) <= NVL(DATE_TRUNC('day',CAST(installed_date AS TIMESTAMP_TZ)), DATE_TRUNC('second',CAST(FIRST_CHSU_RECEIVED_DATE AS TIMESTAMP_TZ))) THEN 'S'
            ELSE 'F'
            END AS PM_FLAG
    from criteria
)
select * from final
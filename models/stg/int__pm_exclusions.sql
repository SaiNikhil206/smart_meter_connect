with flag_s as (
    SELECT * from {{ ref('int__exclusions')}}
),
final as (
    select * from flag_s where PM_FLAG is NOT NULL
)
select * from final
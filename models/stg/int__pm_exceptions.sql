with flag_f as (
    SELECT * from {{ ref('int__pm_1_1')}}
),
final as (
    select * from flag_f where PM_FLAG = 'E'
)
select * from final
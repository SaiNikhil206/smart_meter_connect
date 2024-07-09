with flag_s as (
    SELECT * from {{ ref('int__criteria')}}
),
final as (
    select * from flag_s where PM_FLAG = 'S'
)
select * from final
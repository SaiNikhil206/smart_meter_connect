with flag_f as (
    SELECT * from {{ ref('int__criteria')}}
),
final as (
    select * from flag_f where PM_FLAG = 'F'
)
select * from final
with flag_f as (
    SELECT * from {{ ref('int__adhoc_except')}}
),
final as (
    select * from flag_f where PM_FLAG = 'E'
)
select * from final
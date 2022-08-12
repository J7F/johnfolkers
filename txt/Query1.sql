with

datemeta_i as (
select
logdatetime,
extract(epoch from logdatetime) as epoch,
e1,a,b,c
from edge_portalauth_junejulyaug2022),

datemeta_ii as (
select
logdatetime,epoch,e1,a,b,c,
extract(timezone_hour from logdatetime) as tz,
extract(year from logdatetime) as year,
extract(quarter from logdatetime) as quarter,
extract(week from logdatetime) as week,
extract(doy from logdatetime) as doy,
extract(month from logdatetime) as month,
extract(day from logdatetime) as day,
extract(dow from logdatetime) as dow,
extract(hour from logdatetime) as hour,
extract(minute from logdatetime) as minute,
case extract(dow from logdatetime)
    when 0 then 'Sun'
    when 1 then 'Mon'
    when 2 then 'Tue'
    when 3 then 'Wed'
    when 4 then 'Thu'
    when 5 then 'Fri'
    when 6 then 'Sat'
end downame,
case 
    when extract(dow from logdatetime) > 0
        and extract(dow from logdatetime) < 6
    then true else false
end as isweekday,
case 
    when extract(dow from logdatetime) = 0
        or extract(dow from logdatetime) = 6
    then true else false
end as isweekend,
row_number() over (partition by epoch,a,b,c order by epoch) dupcount
from datemeta_i),

rownumbers_i as (
select
logdatetime,epoch,e1,a,b,c,

first_value(epoch) over (partition by a order by epoch) a_epochfirst,
last_value(epoch) over (partition by a order by epoch rows between current row and unbounded following) a_epochlast,
first_value(epoch) over (partition by a,week order by epoch) a_epochfirst_byweek,
last_value(epoch) over (partition by a,week order by epoch rows between current row and unbounded following) a_epochlast_byweek,
first_value(epoch) over (partition by a,doy order by epoch) a_epochfirst_bydoy,
last_value(epoch) over (partition by a,doy order by epoch rows between current row and unbounded following) a_epochlast_bydoy,
first_value(epoch) over (partition by a,hour order by epoch) a_epochfirst_byhour,
last_value(epoch) over (partition by a,hour order by epoch rows between current row and unbounded following) a_epochlast_byhour,

lag(epoch) over (partition by a order by epoch) a_epochprev,
lead(epoch) over (partition by a order by epoch) a_epochnext,

lag(epoch) over (partition by b order by epoch) b_epochprev,
lead(epoch) over (partition by b order by epoch) b_epochnext,

lag(epoch) over (partition by c order by epoch) c_epochprev,
lead(epoch) over (partition by c order by epoch) c_epochnext,

lag(epoch) over (partition by a,b order by epoch) ab_epochprev,
lead(epoch) over (partition by a,b order by epoch) ab_epochnext,

lag(epoch) over (partition by a,c order by epoch) ac_epochprev,
lead(epoch) over (partition by a,c order by epoch) ac_epochnext,

lag(epoch) over (partition by b,c order by epoch) bc_epochprev,
lead(epoch) over (partition by b,c order by epoch) bc_epochnext,

tz,year,quarter,week,doy,month,day,dow,hour,minute,downame,isweekday,isweekend,

case when
    downame='Sat' or downame='Sun'
    then 0 else 6 - dow
end as daystillweekend,

row_number() over (partition by a, year, week order by epoch) a_week_count,
row_number() over (partition by a, year, week order by epoch desc) - 1 a_week_remaining_count,
row_number() over (partition by a, year, doy order by epoch) a_doy_count,
row_number() over (partition by a, year, doy order by epoch desc) - 1 a_doy_remaining_count,
row_number() over (partition by a, year, month order by epoch) a_month_count,
row_number() over (partition by a, year, month order by epoch desc) - 1 a_month_remaining_count,
row_number() over (partition by a, year order by epoch) a_count,
row_number() over (partition by a, year order by epoch desc) - 1 a_remaining_count,

row_number() over (partition by b, year, week order by epoch) b_week_count,
row_number() over (partition by b, year, week order by epoch desc) - 1 b_week_remaining_count,
row_number() over (partition by b, year, doy order by epoch) b_doy_count,
row_number() over (partition by b, year, doy order by epoch desc) - 1 b_doy_remaining_count,
row_number() over (partition by b, year, month order by epoch) b_month_count,
row_number() over (partition by b, year, month order by epoch desc) - 1 b_month_remaining_count,
row_number() over (partition by b, year order by epoch) b_count,
row_number() over (partition by b, year order by epoch desc) - 1 b_remaining_count,

row_number() over (partition by c, year, week order by epoch) c_week_count,
row_number() over (partition by c, year, week order by epoch desc) - 1 c_week_remaining_count,
row_number() over (partition by c, year, doy order by epoch) c_doy_count,
row_number() over (partition by c, year, doy order by epoch desc) - 1 c_doy_remaining_count,
row_number() over (partition by c, year, month order by epoch) c_month_count,
row_number() over (partition by c, year, month order by epoch desc) - 1 c_month_remaining_count,
row_number() over (partition by c, year order by epoch) c_count,
row_number() over (partition by c, year order by epoch desc) - 1 c_remaining_count

from datemeta_ii
where dupcount=1),

rownumbers_ii as (
select
logdatetime,epoch,e1,a,b,c,
a_epochfirst,a_epochlast,
a_epochfirst_byweek,a_epochlast_byweek,
a_epochfirst_bydoy,a_epochlast_bydoy,
a_epochfirst_byhour,a_epochlast_byhour,

a_epochprev,
epoch - a_epochprev as a_epochprev_delta,
a_epochnext,
a_epochnext - epoch as a_epochnext_delta,

b_epochprev,
epoch - b_epochprev as b_epochprev_delta,
b_epochnext,
b_epochnext - epoch as b_epochnext_delta,

c_epochprev,
epoch - c_epochprev as c_epochprev_delta,
c_epochnext,
c_epochnext - epoch as c_epochnext_delta,

ab_epochprev,
epoch - ab_epochprev as ab_epochprev_delta,
ab_epochnext,
ab_epochnext - epoch as ab_epochnext_delta,

ac_epochprev,
epoch - ac_epochprev as ac_epochprev_delta,
ac_epochnext,
ac_epochnext - epoch as ac_epochnext_delta,

bc_epochprev,
epoch - bc_epochprev as bc_epochprev_delta,
bc_epochnext,
bc_epochnext - epoch as bc_epochnext_delta,

tz,year,quarter,week,doy,month,day,dow,hour,minute,downame,isweekday,isweekend,

daystillweekend,

a_week_count,a_week_remaining_count,
a_doy_count,a_doy_remaining_count,
a_month_count,a_month_remaining_count,
a_count,a_remaining_count,

b_week_count,b_week_remaining_count,
b_doy_count,b_doy_remaining_count,
b_month_count,b_month_remaining_count,
b_count,b_remaining_count,

c_week_count,c_week_remaining_count,
c_doy_count,c_doy_remaining_count,
c_month_count,c_month_remaining_count,
c_count,c_remaining_count


from rownumbers_i
)
select * from rownumbers_ii;
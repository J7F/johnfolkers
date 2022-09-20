with

datemeta_i as (
select
logdatetimetz,
extract(epoch from logdatetimetz) as epoch,
v,a,b,c,d,e,f
from edge_portalauth_0601_0913_2022),

datemeta_ii as (
select
logdatetimetz,epoch,v,a,b,c,d,e,f,
extract(timezone_hour from logdatetimetz) as tz,
extract(year from logdatetimetz) as year,
extract(quarter from logdatetimetz) as quarter,
extract(week from logdatetimetz) as week,
extract(doy from logdatetimetz) as doy,
extract(month from logdatetimetz) as month,
extract(day from logdatetimetz) as day,
extract(dow from logdatetimetz) as dow,
extract(hour from logdatetimetz) as hour,
extract(minute from logdatetimetz) as minute,
case extract(dow from logdatetimetz)
    when 0 then 'Sun'
    when 1 then 'Mon'
    when 2 then 'Tue'
    when 3 then 'Wed'
    when 4 then 'Thu'
    when 5 then 'Fri'
    when 6 then 'Sat'
end downame,
case 
    when extract(dow from logdatetimetz) > 0
        and extract(dow from logdatetimetz) < 6
    then 1 else 0
end as isweekday,
case 
    when extract(dow from logdatetimetz) = 0
        or extract(dow from logdatetimetz) = 6
    then 1 else 0
end as isweekend,
row_number() over (partition by epoch,a,b,c,d,e,f order by epoch) dupcount
from datemeta_i),

rownumbers_i as (
select
logdatetimetz,epoch,v,a,b,c,d,e,f,

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

row_number() over (partition by a, year, doy order by epoch) a_doy_count,
row_number() over (partition by a, year, doy order by epoch desc) - 1 a_doy_remaining_count,
row_number() over (partition by a, year, week order by epoch) a_week_count,
row_number() over (partition by a, year, week order by epoch desc) - 1 a_week_remaining_count,
row_number() over (partition by a, year, month order by epoch) a_month_count,
row_number() over (partition by a, year, month order by epoch desc) - 1 a_month_remaining_count,
row_number() over (partition by a, year order by epoch) a_count,
row_number() over (partition by a, year order by epoch desc) - 1 a_remaining_count,

row_number() over (partition by b, year, doy order by epoch) b_doy_count,
row_number() over (partition by b, year, doy order by epoch desc) - 1 b_doy_remaining_count,
row_number() over (partition by b, year, week order by epoch) b_week_count,
row_number() over (partition by b, year, week order by epoch desc) - 1 b_week_remaining_count,
row_number() over (partition by b, year, month order by epoch) b_month_count,
row_number() over (partition by b, year, month order by epoch desc) - 1 b_month_remaining_count,
row_number() over (partition by b, year order by epoch) b_count,
row_number() over (partition by b, year order by epoch desc) - 1 b_remaining_count,

row_number() over (partition by c, year, doy order by epoch) c_doy_count,
row_number() over (partition by c, year, doy order by epoch desc) - 1 c_doy_remaining_count,
row_number() over (partition by c, year, week order by epoch) c_week_count,
row_number() over (partition by c, year, week order by epoch desc) - 1 c_week_remaining_count,
row_number() over (partition by c, year, month order by epoch) c_month_count,
row_number() over (partition by c, year, month order by epoch desc) - 1 c_month_remaining_count,
row_number() over (partition by c, year order by epoch) c_count,
row_number() over (partition by c, year order by epoch desc) - 1 c_remaining_count,

row_number() over (partition by c, year, doy, a order by epoch) ac_doy_count,
row_number() over (partition by c, year, doy, a order by epoch desc) ac_doy_remaining_count,
row_number() over (partition by c, year, week, a order by epoch) ac_week_count,
row_number() over (partition by c, year, week, a order by epoch desc) ac_week_remaining_count,
row_number() over (partition by c, year, month, a order by epoch) ac_month_count,
row_number() over (partition by c, year, month, a order by epoch desc) ac_month_remaining_count,
row_number() over (partition by c, year, a order by epoch) ac_count,
row_number() over (partition by c, year, a order by epoch desc) ac_remaining_count,

row_number() over (partition by d, year, doy, a order by epoch) ad_doy_count,
row_number() over (partition by d, year, week, a order by epoch) ad_week_count,
row_number() over (partition by d, year, month, a order by epoch) ad_month_count,
row_number() over (partition by d, year, a order by epoch) ad_count,

case when d = 'Cookie' then row_number() over (partition by d, year, doy, a order by epoch) end as ad_doy_cookie_count,
case when d = 'Cookie' then row_number() over (partition by d, year, week, a order by epoch) end as ad_week_cookie_count,
case when d = 'Cookie' then row_number() over (partition by d, year, month, a order by epoch) end as ad_month_cookie_count,
case when d = 'Cookie' then row_number() over (partition by d, year, a order by epoch) end as ad_cookie_count,

case when d = 'SAML' then row_number() over (partition by d, year, doy, a order by epoch) end as ad_doy_saml_count,
case when d = 'SAML' then row_number() over (partition by d, year, week, a order by epoch) end as ad_week_saml_count,
case when d = 'SAML' then row_number() over (partition by d, year, month, a order by epoch) end as ad_month_saml_count,
case when d = 'SAML' then row_number() over (partition by d, year, a order by epoch) end as ad_saml_count,

row_number() over (partition by e, year, doy, a order by epoch) ae_doy_count,
row_number() over (partition by e, year, week, a order by epoch) ae_week_count,
row_number() over (partition by e, year, month, a order by epoch) ae_month_count,
row_number() over (partition by e, year, a order by epoch) ae_count,

row_number() over (partition by f, year, doy, a order by epoch) af_doy_count,
row_number() over (partition by f, year, week, a order by epoch) af_week_count,
row_number() over (partition by f, year, month, a order by epoch) af_month_count,
row_number() over (partition by f, year, a order by epoch) af_count

from datemeta_ii
where dupcount=1),

rownumbers_ii as (
select
logdatetimetz,epoch,v,a,b,c,d,e,f,
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

a_doy_count,a_doy_remaining_count,max(a_doy_count) over (partition by a,doy) as a_doy_sum,
a_week_count,a_week_remaining_count,max(a_week_count) over (partition by a,week) as a_week_sum,
a_month_count,a_month_remaining_count,max(a_month_count) over (partition by a,month) as a_month_sum,
a_count,a_remaining_count,max(a_count) over (partition by a) as a_sum,

b_doy_count,b_doy_remaining_count,max(b_doy_count) over (partition by b,doy) as b_doy_sum,
b_week_count,b_week_remaining_count,max(b_week_count) over (partition by b,week) as b_week_sum,
b_month_count,b_month_remaining_count,max(b_month_count) over (partition by b,month) as b_month_sum,
b_count,b_remaining_count,max(b_count) over (partition by b) as b_sum,

c_doy_count,c_doy_remaining_count,max(c_doy_count) over (partition by c,doy) as c_doy_sum,
c_week_count,c_week_remaining_count,max(c_week_count) over (partition by c,week) as c_week_sum,
c_month_count,c_month_remaining_count,max(c_month_count) over (partition by c,month) as c_month_sum,
c_count,c_remaining_count,max(c_count) over (partition by c) as c_sum,

ac_doy_count,ac_doy_remaining_count,max(ac_doy_count) over (partition by a,c,doy) as ac_doy_sum,
ac_week_count,ac_week_remaining_count,max(ac_week_count) over (partition by a,c,week) as ac_week_sum,
ac_month_count,ac_month_remaining_count,max(ac_month_count) over (partition by a,c,month) as ac_month_sum,
ac_count,ac_remaining_count,max(ac_count) over (partition by a,c) as ac_sum,

ad_doy_count,max(ad_doy_count) over (partition by a,d,doy) as ad_doy_sum,
ad_week_count,max(ad_week_count) over (partition by a,d,week) as ad_week_sum,
ad_month_count,max(ad_month_count) over (partition by a,d,month) as ad_month_sum,
ad_count,max(ad_count) over (partition by a,d) as ad_sum,

ad_doy_cookie_count,max(ad_doy_cookie_count) over (partition by a,doy) as ad_doy_cookie_sum,
ad_week_cookie_count,max(ad_week_cookie_count) over (partition by a,week) as ad_week_cookie_sum,
ad_month_cookie_count,max(ad_month_cookie_count) over (partition by a,month) as ad_month_cookie_sum,
ad_cookie_count,max(ad_cookie_count) over (partition by a) as ad_cookie_sum,

ad_doy_saml_count,max(ad_doy_saml_count) over (partition by a,doy) as ad_doy_saml_sum,
ad_week_saml_count,max(ad_week_saml_count) over (partition by a,week) as ad_week_saml_sum,
ad_month_saml_count,max(ad_month_saml_count) over (partition by a,month) as ad_month_saml_sum,
ad_saml_count,max(ad_saml_count) over (partition by a) as ad_saml_sum,

ae_doy_count,max(ae_doy_count) over (partition by a,e,doy) as ae_doy_sum,
ae_week_count,max(ae_week_count) over (partition by a,e,week) as ae_week_sum,
ae_month_count,max(ae_month_count) over (partition by a,e,month) as ae_month_sum,
ae_count,max(ae_count) over (partition by a,e) as ae_sum,

af_doy_count,max(af_doy_count) over (partition by a,f,doy) as af_doy_sum,
af_week_count,max(af_week_count) over (partition by a,f,week) as af_week_sum,
af_month_count,max(af_month_count) over (partition by a,f,month) as af_month_sum,
af_count,max(af_count) over (partition by a,f) as af_sum

from rownumbers_i
),

rownumbers_iii as (
select
logdatetimetz,epoch,v,a,b,c,d,e,f,
a_epochfirst,a_epochlast,
a_epochfirst_byweek,a_epochlast_byweek,
a_epochfirst_bydoy,a_epochlast_bydoy,
a_epochfirst_byhour,a_epochlast_byhour,

a_epochprev,
a_epochprev_delta,
a_epochnext,
a_epochnext_delta,

b_epochprev,
b_epochprev_delta,
b_epochnext,
b_epochnext_delta,

c_epochprev,
c_epochprev_delta,
c_epochnext,
c_epochnext_delta,

ab_epochprev,
ab_epochprev_delta,
ab_epochnext,
ab_epochnext_delta,

ac_epochprev,
ac_epochprev_delta,
ac_epochnext,
ac_epochnext_delta,

bc_epochprev,
bc_epochprev_delta,
bc_epochnext,
bc_epochnext_delta,

tz,year,quarter,week,doy,month,day,dow,hour,minute,downame,isweekday,isweekend,

daystillweekend,

a_doy_count,a_doy_remaining_count,a_doy_sum,
a_week_count,a_week_remaining_count,a_week_sum,
a_month_count,a_month_remaining_count,a_month_sum,
a_count,a_remaining_count,a_sum,

b_doy_count,b_doy_remaining_count,b_doy_sum,
b_week_count,b_week_remaining_count,b_week_sum,
b_month_count,b_month_remaining_count,b_month_sum,
b_count,b_remaining_count,b_sum,

c_doy_count,c_doy_remaining_count,c_doy_sum,
c_week_count,c_week_remaining_count,c_week_sum,
c_month_count,c_month_remaining_count,c_month_sum,
c_count,c_remaining_count,c_sum,

ac_doy_count,ac_doy_remaining_count,ac_doy_sum,
ac_week_count,ac_week_remaining_count,ac_week_sum,
ac_month_count,ac_month_remaining_count,ac_month_sum,
ac_count,ac_remaining_count,ac_sum,

ad_doy_count,ad_doy_sum,
ad_week_count,ad_week_sum,
ad_month_count,ad_month_sum,
ad_count,ad_sum,

case when ad_doy_cookie_count is null then 0 else ad_doy_cookie_count end as ad_doy_cookie_count,
case when ad_doy_cookie_sum is null then 0 else ad_doy_cookie_sum end as ad_doy_cookie_sum,

case when ad_week_cookie_count is null then 0 else ad_week_cookie_count end as ad_week_cookie_count,
case when ad_week_cookie_sum is null then 0 else ad_week_cookie_sum end as ad_week_cookie_sum,

case when ad_month_cookie_count is null then 0 else ad_month_cookie_count end as ad_month_cookie_count,
case when ad_month_cookie_sum is null then 0 else ad_month_cookie_sum end as ad_month_cookie_sum,

case when ad_cookie_count is null then 0 else ad_cookie_count end as ad_cookie_count,
case when ad_cookie_sum is null then 0 else ad_cookie_sum end as ad_cookie_sum,

case when ad_doy_saml_count is null then 0 else ad_doy_saml_count end as ad_doy_saml_count,
case when ad_doy_saml_sum is null then 0 else ad_doy_saml_sum end as ad_doy_saml_sum,

case when ad_week_saml_count is null then 0 else ad_week_saml_count end as ad_week_saml_count,
case when ad_week_saml_sum is null then 0 else ad_week_saml_sum end as ad_week_saml_sum,

case when ad_month_saml_count is null then 0 else ad_month_saml_count end as ad_month_saml_count,
case when ad_month_saml_sum is null then 0 else ad_month_saml_sum end as ad_month_saml_sum,

case when ad_saml_count is null then 0 else ad_saml_count end as ad_saml_count,
case when ad_saml_sum is null then 0 else ad_saml_sum end as ad_saml_sum,

ae_doy_count,ae_doy_sum,
ae_week_count,ae_week_sum,
ae_month_count,ae_month_sum,
ae_count,ae_sum,

af_doy_count,af_doy_sum,
af_week_count,af_week_sum,
af_month_count,af_month_sum,
af_count,af_sum

from rownumbers_ii
)

select * into edge_portalauth_0601_0913_2022_extended from rownumbers_iii;
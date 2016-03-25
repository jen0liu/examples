-- overall impact
set time zone 'UTC';

-- efficacy - show

create temp table filter_tweets as
select t.*, client_filter_score,(select array_agg(series_num) from series_to_fav_ids where series_to_fav_ids.fav_ids && favs) as show_ids from client_user_score i join :vertical_tweets t 
on (i.user_id = t.user_id and extract(year from t.utc) = extract(year from i.utc) and extract(week from t.utc) = extract(week from i.utc))
where  t.utc > :'before_start_date' and t.utc < :'after_end_date'  and is_consumer_tweet;

create temp table imp_by_telecast as
select 
case when t1.show_id>0 then t1.show_id else t2.show_id end as show_id, 
case when char_length(t1.telecast) > 0 then t1.telecast else t2.telecast end as telecast, 
t1.imp1 as imp1, t2.imp2 as imp2 from
(select ap.series_num as show_id, t.title as telecast, sum(ap.avg_audience) as imp1 from telecasts t join adplacements ap on t.id=ap.telecast_id
join :filter_table a on :filter_id = a.id where a.title ~* :'advertiser' and utc between :'before_start_date' and :'before_end_date' group by 1,2) as t1 full outer join
(select ap.series_num as show_id, t.title as telecast, sum(ap.avg_audience) as imp2 from telecasts t join adplacements ap on t.id=ap.telecast_id
join :filter_table a on :filter_id = a.id where a.title ~* :'advertiser' and utc between :'after_start_date' and :'after_end_date' group by 1,2) as t2 on t1.show_id = t2.show_id;

create temp table filter_by_show as
select 
case when t1.show_id>0 then t1.show_id else t2.show_id end as show_id, 
t1.users1 as users1, t2.users2 as users2 from
(select show_id,sum(client_filter_score*gen_female) as users1 from filter_tweets,unnest(show_ids) as show_id 
where utc > :'before_start_date' and utc < :'before_end_date' group by show_id) as t1 full outer join
(select show_id,sum(client_filter_score*gen_female) as users2 from filter_tweets,unnest(show_ids) as show_id 
where utc > :'after_start_date' and utc < :'after_end_date' group by show_id) as t2 on t1.show_id = t2.show_id;

create temp table efficacy_by_show as 
select telecast,imp_before,filter_before,imp_after,filter_after,
imp_after/(:'after_end_date'::date - :'after_start_date'::date)-imp_before/(:'before_end_date'::date - :'before_start_date'::date) as imp_change, 
filter_after/(:'after_end_date'::date - :'after_start_date'::date)-filter_before/(:'before_end_date'::date - :'before_start_date'::date) as filter_change 
from
(select imp.telecast as telecast,
case when sum(imp1)> 0 then log(sum(imp1)) else 0 end as imp_before, 
case when sum(filter.users1)> 0 then sum(filter.users1) else 0 end as filter_before, 
log(sum(imp2)) as imp_after, sum(filter.users2) as filter_after
from imp_by_telecast imp left outer join filter_by_show filter on imp.show_id=filter.show_id
group by 1) as t order by 4 desc;

-- efficacy - network

create temp table imp_by_network as
select 
case when t1.show_id>0 then t1.show_id else t2.show_id end as show_id, 
case when char_length(t1.network) > 0 then t1.network else t2.network end as network, 
t1.imp1 as imp1, t2.imp2 as imp2 from
(select ap.series_num as show_id, n.title as network, sum(ap.avg_audience) as imp1 from networks n join adplacements ap on n.id=ap.network_id
join :filter_table a on :filter_id = a.id where a.title ~* :'advertiser' and utc between :'before_start_date' and :'before_end_date' group by 1,2 order by 2) as t1 full outer join
(select ap.series_num as show_id, n.title as network, sum(ap.avg_audience) as imp2 from networks n join adplacements ap on n.id=ap.network_id
join :filter_table a on :filter_id = a.id where a.title ~* :'advertiser' and utc between :'after_start_date' and :'after_end_date' group by 1,2 order by 2) as t2 on t1.show_id = t2.show_id order by 2;

create temp table efficacy_by_network as 
select network,imp_before,filter_before,imp_after,filter_after,
imp_after/(:'after_end_date'::date - :'after_start_date'::date)-imp_before/(:'before_end_date'::date - :'before_start_date'::date) as imp_change,
filter_after/(:'after_end_date'::date - :'after_start_date'::date)-filter_before/(:'before_end_date'::date - :'before_start_date'::date) as filter_change 
from
(select imp.network as network, 
case when sum(imp1)> 0 then log(sum(imp1)) else 0 end as imp_before, 
sum(filter.users1) as filter_before, 
case when sum(imp2)> 0 then log(sum(imp2)) else 0 end as imp_after, 
sum(filter.users2) as filter_after
from imp_by_network imp left outer join filter_by_show filter on imp.show_id=filter.show_id
group by 1) as t order by 4 desc ;

-- efficacy - demo

create temp table imp_demo_before as
select sum(gen_male*impressions) as gen_male, sum(gen_female*impressions) as gen_female, sum(eth_white*impressions) as eth_white, sum(eth_hispanic*impressions) as eth_hispanic, sum(eth_black*impressions) as eth_black, 
sum(eth_asian*impressions) as eth_asian, sum(age_18to24*impressions) as age_18to24, 
sum((age_25to34)*impressions) as age_25to34,sum((age_35to44 + age_45to54)*impressions) as age_35to49,sum((age_55to64 + age_65plus)*impressions) as age_50to99  
from (select series_num,sum(avg_audience) as impressions from adplacements ap 
join :filter_table a on :filter_id = a.id where a.title ~* :'advertiser' and utc > :'before_start_date' and utc < :'before_end_date' 
group by 1) as sum_audience inner join tv_series_demo 
using(series_num);

create temp table imp_demo_after as
select sum(gen_male*impressions) as gen_male, sum(gen_female*impressions) as gen_female, sum(eth_white*impressions) as eth_white, sum(eth_hispanic*impressions) as eth_hispanic, sum(eth_black*impressions) as eth_black, 
sum(eth_asian*impressions) as eth_asian, sum(age_18to24*impressions) as age_18to24, 
sum((age_25to34)*impressions) as age_25to34,sum((age_35to44 + age_45to54)*impressions) as age_35to49,sum((age_55to64 + age_65plus)*impressions) as age_50to99  
from (select series_num,sum(avg_audience) as impressions from adplacements ap 
join :filter_table a on :filter_id = a.id where a.title ~* :'advertiser' and utc > :'after_start_date' and utc < :'after_end_date' 
group by 1) as sum_audience inner join tv_series_demo 
using(series_num);

create temp table filter_demo_before as
select sum(gen_male*client_filter_score) as gen_male,sum(gen_female*client_filter_score) as gen_female,sum(eth_white*client_filter_score) as eth_white,sum(eth_black*client_filter_score) as eth_black,
sum(eth_hispanic*client_filter_score) as eth_hispanic,sum(eth_asian*client_filter_score) as eth_asian,sum(eth_other*client_filter_score) as eth_other,
sum(age_18to24*client_filter_score) as age_18to24,sum(age_25to29*client_filter_score)+sum(age_30to34*client_filter_score) as age_25to34,sum(age_35to39*client_filter_score)+sum(age_40to49*client_filter_score) as age_35to49,
sum(age_50to99*client_filter_score) as age_50to99 from filter_tweets where utc > :'before_start_date' and utc < :'before_end_date';

create temp table filter_demo_after as
select sum(gen_male*client_filter_score) as gen_male,sum(gen_female*client_filter_score) as gen_female,sum(eth_white*client_filter_score) as eth_white,sum(eth_black*client_filter_score) as eth_black,
sum(eth_hispanic*client_filter_score) as eth_hispanic,sum(eth_asian*client_filter_score) as eth_asian,sum(eth_other*client_filter_score) as eth_other,
sum(age_18to24*client_filter_score) as age_18to24,sum(age_25to29*client_filter_score)+sum(age_30to34*client_filter_score) as age_25to34,sum(age_35to39*client_filter_score)+sum(age_40to49*client_filter_score) as age_35to49,
sum(age_50to99*client_filter_score) as age_50to99 from filter_tweets where utc > :'after_start_date' and utc < :'after_end_date';

create temp table efficacy_by_demo as
select t1.demo,log(imp_before) as imp_before,filter_before*weight as filter_before,log(imp_after) as imp_after,filter_after*weight as filter_after,
log(imp_after/(:'after_end_date'::date - :'after_start_date'::date))-log(imp_before/(:'before_end_date'::date - :'before_start_date'::date)) as imp_change,
filter_after*weight/(:'after_end_date'::date - :'after_start_date'::date)-filter_before*weight/(:'before_end_date'::date - :'before_start_date'::date) as filter_change  
from (select unnest(array['gen_male','gen_female','eth_white','eth_black','eth_hispanic','eth_asian','age_18to24','age_25to34','age_35to49','age_50to99']) as demo,
unnest(array[gen_male,gen_female,eth_white,eth_black,eth_hispanic,eth_asian,age_18to24,age_25to34,age_35to49,age_50to99]) as imp_before
from imp_demo_before) as t1
join 
(select unnest(array['gen_male','gen_female','eth_white','eth_black','eth_hispanic','eth_asian','age_18to24','age_25to34','age_35to49','age_50to99']) as demo,
unnest(array[gen_male,gen_female,eth_white,eth_black,eth_hispanic,eth_asian,age_18to24,age_25to34,age_35to49,age_50to99]) as imp_after
from imp_demo_after) as t2 on t1.demo=t2.demo
join 
(select unnest(array['gen_male','gen_female','eth_white','eth_black','eth_hispanic','eth_asian','age_18to24','age_25to34','age_35to49','age_50to99']) as demo,
unnest(array[gen_male,gen_female,eth_white,eth_black,eth_hispanic,eth_asian,age_18to24,age_25to34,age_35to49,age_50to99]) as filter_before
from filter_demo_before) as t3 on t2.demo=t3.demo
join 
(select unnest(array['gen_male','gen_female','eth_white','eth_black','eth_hispanic','eth_asian','age_18to24','age_25to34','age_35to49','age_50to99']) as demo,
unnest(array[gen_male,gen_female,eth_white,eth_black,eth_hispanic,eth_asian,age_18to24,age_25to34,age_35to49,age_50to99]) as filter_after
from filter_demo_after) as t4 on t3.demo=t4.demo
join twitter_bias b on t4.demo=b.segment
order by 1 desc;

insert into media_efficacy
(select cast(:'vertical' as text) as vertical, cast(:'property' as text) as brand, cast(:'before_start_date'||' TO '||:'before_end_date'||' TO '||:'after_start_date'||' TO '||:'after_end_date' as text) as date,cast('network' as text) as metric, * from efficacy_by_network where filter_after>0) 
union all
(select cast(:'vertical' as text) as vertical, cast(:'property' as text) as brand, cast(:'before_start_date'||' TO '||:'before_end_date'||' TO '||:'after_start_date'||' TO '||:'after_end_date' as text) as date,cast('show' as text) as metric, * from efficacy_by_show where filter_after>0)
union all
(select cast(:'vertical' as text) as vertical, cast(:'property' as text) as brand, cast(:'before_start_date'||' TO '||:'before_end_date'||' TO '||:'after_start_date'||' TO '||:'after_end_date' as text) as date,cast('demo' as text) as metric, * from efficacy_by_demo);

create temp table media_efficacy0 as 
select * from media_efficacy where vertical ~* :'vertical' and filter_before >= 10 and filter_after >= 10 and metric in ('network','show'); 

\copy media_efficacy0 to 'media_efficacy.csv' DELIMITER ',' CSV HEADER


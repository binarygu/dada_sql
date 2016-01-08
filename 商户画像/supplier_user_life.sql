drop table if exists dw_tmp.bian_user_start_dt; 
create table dw_tmp.bian_user_start_dt as 
select 
	user_id,
	min(create_dt) as start_dt
from 
	dw.dw_plq_ord_order_head
where 
	city_id = 19 and 
	create_dt < curdate()
group by 
	user_id;


drop table if exists dw_tmp.bian_user_day_user;
create table dw_tmp.bian_user_day_user as 
select
	TO_DAYS(curdate()) - TO_DAYS(start_dt) as  user_day,
	count(user_id) as user_sum
from 
	dw_tmp.bian_user_start_dt
group by
	1;



drop table if exists dw_tmp.bian_user_day_user_num;
create table dw_tmp.bian_user_day_user_num as 
select 
	a.user_day as user_day,
	sum(b.user_sum)  as cum
from
	dw_tmp.bian_user_day_user a
join 
	dw_tmp.bian_user_day_user b
ON a.user_day <= b.user_day
group by 
	1
order by 
	user_day desc;

select 
	dt,
	mm/cum
from 
	(select
		TO_DAYS(create_dt) - TO_DAYS(start_dt) + 1 as dt,
		count(order_id) as mm
	from 
		dw.dw_plq_ord_order_head a 
	inner join 
		dw_tmp.bian_user_start_dt b 
	on a.user_id = b.user_id
	where 
		create_dt < curdate() and 
		city_id = 19
	group by 
		1
	) t1
inner join 
	dw_tmp.bian_user_day_user_num  t2
on t1.dt = t2.user_day
order by 
	dt;


select
	TO_DAYS(create_dt) - TO_DAYS(start_dt) + 1 as dt,
	count(order_id) as mm
from 
	dw.dw_plq_ord_order_head a 
inner join 
	dw_tmp.bian_user_start_dt b 
on a.user_id = b.user_id
where 
	create_dt < curdate() and 
	city_id = 19
group by 
	1;

-- -------------------------------------



drop table if exists dw_tmp.bian_supplier_start_dt; 
create table dw_tmp.bian_supplier_start_dt as 
select 
	supplier_id,
	min(create_dt) as start_dt
from 
	dw.dw_plq_ord_order_head
where 
	city_id = 19 and 
	create_dt < curdate()
group by 
	supplier_id;


drop table if exists dw_tmp.bian_supplier_day_supplier;
create table dw_tmp.bian_supplier_day_supplier as 
select
	TO_DAYS(curdate()) - TO_DAYS(start_dt) as supplier_day,
	count(supplier_id) as supplier_sum
from 
	dw_tmp.bian_supplier_start_dt
group by
	1;



drop table if exists dw_tmp.bian_supplier_day_supplier_num;
create table dw_tmp.bian_supplier_day_supplier_num as 
select 
	a.supplier_day as supplier_day,
	sum(b.supplier_sum) as cum
from
	dw_tmp.bian_supplier_day_supplier a
join 
	dw_tmp.bian_supplier_day_supplier b
ON a.supplier_day <= b.supplier_day
group by 
	1
order by 
	supplier_day desc;

select 
	dt,
	mm/cum
from
	(select
		TO_DAYS(create_dt) - TO_DAYS(start_dt) + 1 as dt,
		count(order_id) as mm
	from 
		dw.dw_plq_ord_order_head a 
	inner join 
		dw_tmp.bian_supplier_start_dt b 
	on a.supplier_id = b.supplier_id
	where create_dt < curdate() and city_id = 19
	group by 
		1
	) t1
inner join 
	dw_tmp.bian_supplier_day_supplier_num  t2
on t1.dt = t2.supplier_day
order by 
	dt;

select
	TO_DAYS(create_dt) - TO_DAYS(start_dt) + 1 as dt,
	count(order_id) as mm
from 
	dw.dw_plq_ord_order_head a 
inner join 
	dw_tmp.bian_supplier_start_dt b 
on a.supplier_id = b.supplier_id
where create_dt < curdate() and city_id = 19
group by 
	1;

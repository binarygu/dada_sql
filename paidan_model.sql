-- 训练数据
-- base:南京
-- time:2015-11-30

-- ------------------------------------------------------------------
-- ------------------------------------------------------------------
-- --------------设置时间和城市的用户变量----------------------------
set @mydate = '2015-12-14';
set @mycity = 4;

-- ------------------------------------------------------------------
-- ------------------------------------------------------------------
-- ----------------------派单静态数据--------------------------------
-- 派单订单数据 
drop table if exists dw_tmp.bian_paidan_model_order;
create table dw_tmp.bian_paidan_model_order as
select
	create_dt,
	finish_dt,
	create_time,
	accept_time,
	fetch_time,
	finish_time,
	order_id,
	order_group_id,
	transporter_id,
	supplier_id,
	order_source_from,
	is_cargo_advance_needed,
	tips_amt,
	allowance_amt,
	deliver_fee_amt,
	block_id,
	supplier_lng,
	supplier_lat,
	receiver_lng,
	receiver_lat,
	is_finished,
	city_id
from
	dw.dw_tsp_order
where
	create_dt between date_sub(@mydate,interval 15 day) and @mydate and  
	city_id = @mycity and 
	order_type_id <> 2;
create index idx1 on dw_tmp.bian_paidan_model_order(order_group_id);
create index idx2 on dw_tmp.bian_paidan_model_order(order_id);
create index idx3 on dw_tmp.bian_paidan_model_order(transporter_id);
create index idx4 on dw_tmp.bian_paidan_model_order(supplier_id);


-- 派单日志数据
drop table if exists dw_tmp.bian_paidan_model_transporter;
create table dw_tmp.bian_paidan_model_transporter as
select
	cal_dt,
	a.create_time as create_time,
	a.task_id as task_id,
	log_type_id,
	transporter_id,
	lat as transporter_lat, 
	lng as transporter_lng,
	running_order_cnt,
	order_id,
	order_group_id
from
	bak.dw_log_task_pool a
left join
	dw.dw_tsp_task c 
on a.task_id = c.task_id
where
	cal_dt between date_sub(@mydate,interval 15 day) and @mydate and  
	a.city_id = @mycity and 
	log_type_id = 1;


-- 派单静态数据
drop table if exists dw_tmp.bian_paidan_model_transporter_order;
create table dw_tmp.bian_paidan_model_transporter_order as
select
	cal_dt,
	create_dt,
	case 
		when a.create_time > b.create_time then b.create_time 
		else a.create_time 
	end as paidan_time,
	b.create_time as jiedan_time,
	finish_dt,
	a.transporter_id as transporter_id,
	running_order_cnt,
	supplier_id,
	transporter_lat,
	transporter_lng,
	a.order_id as order_id,
	a.order_group_id as order_group_id,
	order_source_from,
	is_cargo_advance_needed,
	tips_amt,
	allowance_amt,
	deliver_fee_amt,
	block_id,
	supplier_lng,
	supplier_lat,
	receiver_lng,
	receiver_lat,
	dw_tmp.get_geo_distance(transporter_lng,transporter_lat,supplier_lng,supplier_lat) as transporter_supplier_distance,
	dw_tmp.get_geo_distance(supplier_lng,supplier_lat,receiver_lng,receiver_lat) as supplier_receiver_distance,
	case 
		when a.transporter_id = b.transporter_id then 1
		when a.transporter_id <> b.transporter_id then -1 
	end as label
from
	dw_tmp.bian_paidan_model_transporter a
inner join
	dw_tmp.bian_paidan_model_order b
on a.order_id = b.order_id
where
	a.order_id <> -99

union 

select 
	cal_dt,
	create_dt,
	case 
		when a.create_time > b.create_time then b.create_time 
		else a.create_time 
	end as paidan_time,
	b.create_time as jiedan_time,
	finish_dt,
	a.transporter_id as transporter_id,
	running_order_cnt,
	supplier_id,
	transporter_lat,
	transporter_lng,
	a.order_id as order_id,
	a.order_group_id as order_group_id,
	order_source_from,
	is_cargo_advance_needed,
	tips_amt,
	allowance_amt,
	deliver_fee_amt,
	block_id,
	supplier_lng,
	supplier_lat,
	receiver_lng,
	receiver_lat,
	dw_tmp.get_geo_distance(transporter_lng,transporter_lat,supplier_lng,supplier_lat) as transporter_supplier_distance,
	dw_tmp.get_geo_distance(supplier_lng,supplier_lat,receiver_lng,receiver_lat) as supplier_receiver_distance,
	case 
		when a.transporter_id = b.transporter_id then 1
		when a.transporter_id <> b.transporter_id then -1 
	end as label
from
	dw_tmp.bian_paidan_model_transporter a 
inner join
	dw_tmp.bian_paidan_model_order b
on a.order_group_id = b.order_group_id
where
	a.order_id = -99;
create index idx1 on dw_tmp.bian_paidan_model_transporter_order(order_group_id);
create index idx2 on dw_tmp.bian_paidan_model_transporter_order(order_id);
create index idx3 on dw_tmp.bian_paidan_model_transporter_order(transporter_id);
create index idx4 on dw_tmp.bian_paidan_model_transporter_order(supplier_id);

-- ------------------------------------------------------------------
-- ------------------------------------------------------------------
-- ---------------------派单动态数据---------------------------------
-- 达达动态派单数据
drop table if exists dw_tmp.bian_transporter_dongtai_paidan;
create table dw_tmp.bian_transporter_dongtai_paidan as
select
	@mydate as create_dt,
	transporter_id,
	sum(case when create_dt = date_sub(@mydate,interval 1 day) then 1 else 0 end) transporter_before_one_day_paidan_num,
	sum(case when create_dt = date_sub(@mydate,interval 1 day) and label = 1 then 1 else 0 end) transporter_before_one_day_paidan_success_num,
	sum(case when create_dt = date_sub(@mydate,interval 1 day) and label = 1 then 1 else 0 end) * 1.0 / sum(case when create_dt = date_sub(@mydate,interval 1 day) then 1 else 0 end) transporter_before_one_day_paidan_lv,
	sum(case when create_dt > date_sub(@mydate,interval 8 day) then 1 else 0 end) transporter_before_seven_day_paidan_num,
	sum(case when create_dt > date_sub(@mydate,interval 8 day) and label = 1 then 1 else 0 end) transporter_before_seven_day_paidan_success_num,
	sum(case when create_dt > date_sub(@mydate,interval 8 day) and label = 1 then 1 else 0 end) * 1.0 / sum(case when create_dt > date_sub(@mydate,interval 8 day) then 1 else 0 end) transporter_before_seven_day_paidan_lv,
	sum(1) transporter_before_fourteen_day_paidan_num,
	sum(case when label = 1 then 1 else 0 end) transporter_before_fourteen_day_paidan_success_num,
	sum(case when label = 1 then 1 else 0 end) * 1.0 / sum(1) transporter_before_fourteen_day_paidan_lv
from 
	dw_tmp.bian_paidan_model_transporter_order
where
	create_dt < @mydate
group by 
	1,2;
create index idx1 on dw_tmp.bian_transporter_dongtai_paidan(transporter_id);

-- 达达动态接单数据
drop table if exists dw_tmp.bian_transporter_dongtai_jiedan;
create table dw_tmp.bian_transporter_dongtai_jiedan as
select
	@mydate as create_dt,
	transporter_id,
	sum(case when create_dt = date_sub(@mydate,interval 1 day) then 1 else 0 end) transporter_before_one_day_jiedan_num,
	sum(case when create_dt = date_sub(@mydate,interval 1 day) and is_finished = 1 then 1 else 0 end) transporter_before_one_day_jiedan_success_num,
	sum(case when create_dt = date_sub(@mydate,interval 1 day) and is_finished = 1 then 1 else 0 end) * 1.0 / sum(case when create_dt = date_sub(@mydate,interval 1 day) then 1 else 0 end) transporter_before_one_day_jiedan_lv,
	sum(case when create_dt > date_sub(@mydate,interval 8 day) then 1 else 0 end) transporter_before_seven_day_jiedan_num,
	sum(case when create_dt > date_sub(@mydate,interval 8 day) and is_finished = 1 then 1 else 0 end) transporter_before_seven_day_jiedan_success_num,
	sum(case when create_dt > date_sub(@mydate,interval 8 day) and is_finished = 1 then 1 else 0 end) * 1.0 / sum(case when create_dt > date_sub(@mydate,interval 8 day) then 1 else 0 end) transporter_before_seven_day_jiedan_lv,
	sum(1) transporter_before_fourteen_day_jiedan_num,
	sum(case when is_finished = 1 then 1 else 0 end) transporter_before_fourteen_day_jiedan_success_num,
	sum(case when is_finished = 1 then 1 else 0 end) * 1.0 / sum(1) transporter_before_fourteen_day_jiedan_lv
from 
	dw_tmp.bian_paidan_model_order
where
	create_dt < @mydate
group by 
	1,2;
create index idx1 on dw_tmp.bian_transporter_dongtai_jiedan(transporter_id);

-- 商家动态派单数据
drop table if exists dw_tmp.bian_supplier_dongtai_paidan;
create table dw_tmp.bian_supplier_dongtai_paidan as
select
	@mydate as create_dt,
	supplier_id,
	sum(case when create_dt = date_sub(@mydate,interval 1 day) then 1 else 0 end) supplier_before_one_day_paidan_num,
	sum(case when create_dt = date_sub(@mydate,interval 1 day) and label = 1 then 1 else 0 end) supplier_before_one_day_paidan_success_num,
	sum(case when create_dt = date_sub(@mydate,interval 1 day) and label = 1 then 1 else 0 end) * 1.0 / sum(case when create_dt = date_sub(@mydate,interval 1 day) then 1 else 0 end) supplier_before_one_day_paidan_lv,
	sum(case when create_dt > date_sub(@mydate,interval 8 day) then 1 else 0 end) supplier_before_seven_day_paidan_num,
	sum(case when create_dt > date_sub(@mydate,interval 8 day) and label = 1 then 1 else 0 end) supplier_before_seven_day_paidan_success_num,
	sum(case when create_dt > date_sub(@mydate,interval 8 day) and label = 1 then 1 else 0 end) * 1.0 / sum(case when create_dt > date_sub(@mydate,interval 8 day) then 1 else 0 end) supplier_before_seven_day_paidan_lv,
	sum(1) supplier_before_fourteen_day_paidan_num,
	sum(case when label = 1 then 1 else 0 end) supplier_before_fourteen_day_paidan_success_num,
	sum(case when label = 1 then 1 else 0 end) * 1.0 / sum(1) supplier_before_fourteen_day_paidan_lv
from
	dw_tmp.bian_paidan_model_transporter_order
where
	create_dt < @mydate
group by
	1,2;
create index idx1 on dw_tmp.bian_supplier_dongtai_paidan(supplier_id);


-- 商家动态接单数据
drop table if exists dw_tmp.bian_supplier_dongtai_jiedan;
create table dw_tmp.bian_supplier_dongtai_jiedan as
select
	@mydate as create_dt,
	supplier_id,
	sum(case when create_dt = date_sub(@mydate,interval 1 day) then 1 else 0 end) supplier_before_one_day_jiedan_num,
	sum(case when create_dt = date_sub(@mydate,interval 1 day) and is_finished = 1 then 1 else 0 end) supplier_before_one_day_jiedan_success_num,
	sum(case when create_dt = date_sub(@mydate,interval 1 day) and is_finished = 1 then 1 else 0 end) * 1.0 / sum(case when create_dt = date_sub(@mydate,interval 1 day) then 1 else 0 end) supplier_before_one_day_jiedan_lv,
	sum(case when create_dt > date_sub(@mydate,interval 8 day) then 1 else 0 end) supplier_before_seven_day_jiedan_num,
	sum(case when create_dt > date_sub(@mydate,interval 8 day) and is_finished = 1 then 1 else 0 end) supplier_before_seven_day_jiedan_success_num,
	sum(case when create_dt > date_sub(@mydate,interval 8 day) and is_finished = 1 then 1 else 0 end) * 1.0 / sum(case when create_dt > date_sub(@mydate,interval 8 day) then 1 else 0 end) supplier_before_seven_day_jiedan_lv,
	sum(1) supplier_before_fourteen_day_jiedan_num,
	sum(case when is_finished = 1 then 1 else 0 end) supplier_before_fourteen_day_jiedan_success_num,
	sum(case when is_finished = 1 then 1 else 0 end) * 1.0 / sum(1) supplier_before_fourteen_day_jiedan_lv
from 
	dw_tmp.bian_paidan_model_order
where
	create_dt < @mydate
group by
	1,2;
create index idx1 on dw_tmp.bian_supplier_dongtai_jiedan(supplier_id);

-- 达达商家交互动态派单数据
drop table if exists dw_tmp.bian_transporter_supplier_dongtai_paidan;
create table dw_tmp.bian_transporter_supplier_dongtai_paidan as
select
	@mydate as create_dt,
	transporter_id,
	supplier_id,
	sum(case when create_dt = date_sub(@mydate,interval 1 day) then 1 else 0 end) transporter_supplier_before_one_day_paidan_num,
	sum(case when create_dt = date_sub(@mydate,interval 1 day) and label = 1 then 1 else 0 end) transporter_supplier_before_one_day_paidan_success_num,
	sum(case when create_dt = date_sub(@mydate,interval 1 day) and label = 1 then 1 else 0 end) * 1.0 / sum(case when create_dt = date_sub(@mydate,interval 1 day) then 1 else 0 end) transporter_supplier_before_one_day_paidan_lv,
	sum(case when create_dt > date_sub(@mydate,interval 8 day) then 1 else 0 end) transporter_supplier_before_seven_day_paidan_num,
	sum(case when create_dt > date_sub(@mydate,interval 8 day) and label = 1 then 1 else 0 end) transporter_supplier_before_seven_day_paidan_success_num,
	sum(case when create_dt > date_sub(@mydate,interval 8 day) and label = 1 then 1 else 0 end) * 1.0 / sum(case when create_dt > date_sub(@mydate,interval 8 day) then 1 else 0 end) transporter_supplier_before_seven_day_paidan_lv,
	sum(1) transporter_supplier_before_fourteen_day_paidan_num,
	sum(case when label = 1 then 1 else 0 end) transporter_supplier_before_fourteen_day_paidan_success_num,
	sum(case when label = 1 then 1 else 0 end) * 1.0 / sum(1) transporter_supplier_before_fourteen_day_paidan_lv
from
	dw_tmp.bian_paidan_model_transporter_order
where
	create_dt < @mydate
group by
	1,2,3;
create index idx1 on dw_tmp.bian_transporter_supplier_dongtai_paidan(transporter_id);
create index idx2 on dw_tmp.bian_transporter_supplier_dongtai_paidan(supplier_id);

-- 达达商家交互动态接单数据
drop table if exists dw_tmp.bian_transporter_supplier_dongtai_jiedan;
create table dw_tmp.bian_transporter_supplier_dongtai_jiedan as
select
	@mydate as create_dt,
	transporter_id,
	supplier_id,
	sum(case when create_dt = date_sub(@mydate,interval 1 day) then 1 else 0 end) transporter_supplier_before_one_day_jiedan_num,
	sum(case when create_dt = date_sub(@mydate,interval 1 day) and is_finished = 1 then 1 else 0 end) transporter_supplier_before_one_day_jiedan_success_num,
	sum(case when create_dt = date_sub(@mydate,interval 1 day) and is_finished = 1 then 1 else 0 end) * 1.0 / sum(case when create_dt = date_sub(@mydate,interval 1 day) then 1 else 0 end) transporter_supplier_before_one_day_jiedan_lv,
	sum(case when create_dt > date_sub(@mydate,interval 8 day) then 1 else 0 end) transporter_supplier_before_seven_day_jiedan_num,
	sum(case when create_dt > date_sub(@mydate,interval 8 day) and is_finished = 1 then 1 else 0 end) transporter_supplier_before_seven_day_jiedan_success_num,
	sum(case when create_dt > date_sub(@mydate,interval 8 day) and is_finished = 1 then 1 else 0 end) * 1.0 / sum(case when create_dt > date_sub(@mydate,interval 8 day) then 1 else 0 end) transporter_supplier_before_seven_day_jiedan_lv,
	sum(1) transporter_supplier_before_fourteen_day_jiedan_num,
	sum(case when is_finished = 1 then 1 else 0 end) transporter_supplier_before_fourteen_day_jiedan_success_num,
	sum(case when is_finished = 1 then 1 else 0 end) * 1.0 / sum(1) transporter_supplier_before_fourteen_day_jiedan_lv
from
	dw_tmp.bian_paidan_model_order
where
	create_dt < @mydate
group by
	1,2,3;
create index idx1 on dw_tmp.bian_transporter_supplier_dongtai_jiedan(transporter_id);
create index idx2 on dw_tmp.bian_transporter_supplier_dongtai_jiedan(supplier_id);

-- 派单板块匹配数据
drop table if exists dw_tmp.bian_block_pipei_dongtai_paidan;
create table dw_tmp.bian_block_pipei_dongtai_paidan as
select
	@mydate as create_dt,
	transporter_id,
	block_id,
	(select 
		count(*) 
	from 
		(select
			transporter_id,
			block_id,
			sum(1) as sum_num
		from 
			dw_tmp.bian_paidan_model_transporter_order
		where
			create_dt < @mydate and 
			label = 1
		group by
			transporter_id,
			block_id
		) a
	where
		transporter_id = t.transporter_id and
		sum_num >= t.sum_num
	) as rank
from
	(select
		transporter_id,
		block_id,
		sum(1) as sum_num
	from 
		dw_tmp.bian_paidan_model_transporter_order
	where
		create_dt < @mydate and 
		label = 1
	group by
		transporter_id,
		block_id
	order by
		transporter_id,
		sum_num desc
	) t
having rank = 1;
create index idx1 on dw_tmp.bian_block_pipei_dongtai_paidan(transporter_id);

-- 接单板块匹配数据
drop table if exists dw_tmp.bian_block_pipei_dongtai_jiedan;
create table dw_tmp.bian_block_pipei_dongtai_jiedan as
select
	@mydate as create_dt,
	transporter_id,
	block_id,
	(select 
		count(*) 
	from 
		(select
			transporter_id,
			block_id,
			sum(1) as sum_num
		from 
			dw_tmp.bian_paidan_model_order
		where
			create_dt < @mydate
		group by
			transporter_id,
			block_id
		) a
	where
		transporter_id = t.transporter_id and
		sum_num >= t.sum_num
	) as rank
from
	(select
		transporter_id,
		block_id,
		sum(1) as sum_num
	from 
		dw_tmp.bian_paidan_model_order
	where
		create_dt < @mydate
	group by
		transporter_id,
		block_id
	order by
		transporter_id,
		sum_num desc
	) t
having rank = 1;
create index idx1 on dw_tmp.bian_block_pipei_dongtai_jiedan(transporter_id);

-- ------------------------------------------------------------------
-- ------------------------------------------------------------------
-- ----------------------派单实时数据--------------------------------
-- 派单时手中订单数据
drop table if exists dw_tmp.bian_transporter_shishi_paidan_num;
create table dw_tmp.bian_transporter_shishi_paidan_num as
select
	a.order_id as order_id,
	a.order_group_id as order_group_id,
	a.transporter_id as transporter_id,
	sum(case when finish_time < paidan_time then 1 else 0 end) as today_finish_order_num,
	sum(case when finish_time > paidan_time then 1 else 0 end) as today_never_finish_order_num,
	sum(case when fetch_time < paidan_time and finish_time > paidan_time then 1 else 0 end) as today_fetch_order_num,
	sum(case when fetch_time > paidan_time and finish_time > paidan_time then 1 else 0 end) as today_never_fetch_order_num 
from
	dw_tmp.bian_paidan_model_transporter_order a
inner join
	dw_tmp.bian_paidan_model_order b
on a.transporter_id = b.transporter_id
where
	a.create_dt = @mydate and
	b.create_dt = @mydate and
	accept_time < paidan_time
group by
	1,2,3;
create index idx1 on dw_tmp.bian_transporter_shishi_paidan_num(order_id);
create index idx2 on dw_tmp.bian_transporter_shishi_paidan_num(order_group_id);
create index idx3 on dw_tmp.bian_transporter_shishi_paidan_num(transporter_id);
	
-- 派单时距离数据(到接受者的距离)
drop table if exists dw_tmp.bian_transporter_shishi_paidan_receiver_distance;
create table dw_tmp.bian_transporter_shishi_paidan_receiver_distance as
select 
	order_id,
	order_group_id,
	transporter_id,
	min(paidan_supplier_distance_jiedan_receiver) as paidan_supplier_distance_jiedan_receiver_min,
	max(paidan_supplier_distance_jiedan_receiver) as paidan_supplier_distance_jiedan_receiver_max,
	min(paidan_receiver_distance_jiedan_receiver) as paidan_receiver_distance_jiedan_receiver_min,
	max(paidan_receiver_distance_jiedan_receiver) as paidan_receiver_distance_jiedan_receiver_max
from
	(select
		a.order_id as order_id,
		a.order_group_id as order_group_id,
		a.transporter_id as transporter_id,
		dw_tmp.get_geo_distance(a.supplier_lng,a.supplier_lat,b.receiver_lng,b.receiver_lat) as paidan_supplier_distance_jiedan_receiver,
		dw_tmp.get_geo_distance(a.receiver_lng,a.receiver_lat,b.receiver_lng,b.receiver_lat) as paidan_receiver_distance_jiedan_receiver

	from
		dw_tmp.bian_paidan_model_transporter_order a
	inner join
		dw_tmp.bian_paidan_model_order b
	on a.transporter_id = b.transporter_id
	where
		a.create_dt = @mydate and
		b.create_dt = @mydate and
		accept_time < paidan_time and
		finish_time > paidan_time
	) as a
group by
	1,2,3;
create index idx1 on dw_tmp.bian_transporter_shishi_paidan_receiver_distance(order_id);
create index idx2 on dw_tmp.bian_transporter_shishi_paidan_receiver_distance(order_group_id);
create index idx3 on dw_tmp.bian_transporter_shishi_paidan_receiver_distance(transporter_id);

-- 派单时距离数据(到商家的距离)
drop table if exists dw_tmp.bian_transporter_shishi_paidan_supplier_distance;
create table dw_tmp.bian_transporter_shishi_paidan_supplier_distance as
select 
	order_id,
	order_group_id,
	transporter_id, 
	min(paidan_supplier_distance_jiedan_supplier) as paidan_supplier_distance_jiedan_supplier_min,
	max(paidan_supplier_distance_jiedan_supplier) as paidan_supplier_distance_jiedan_supplier_max,
	min(paidan_receiver_distance_jiedan_supplier) as paidan_receiver_distance_jiedan_supplier_min,
	max(paidan_receiver_distance_jiedan_supplier) as paidan_receiver_distance_jiedan_supplier_max
from
	(select 
		a.order_id as order_id,
		a.order_group_id as order_group_id,
		a.transporter_id as transporter_id,
		dw_tmp.get_geo_distance(a.supplier_lng,a.supplier_lat,b.supplier_lng,b.supplier_lat) as paidan_supplier_distance_jiedan_supplier,
		dw_tmp.get_geo_distance(a.receiver_lng,a.receiver_lat,b.supplier_lng,b.supplier_lat) as paidan_receiver_distance_jiedan_supplier
	from
		dw_tmp.bian_paidan_model_transporter_order a
	inner join
		dw_tmp.bian_paidan_model_order b
	on a.transporter_id = b.transporter_id
	where
		a.create_dt = @mydate and
		b.create_dt = @mydate and
		accept_time < paidan_time and
		finish_time > paidan_time and
		fetch_time > paidan_time
	) a
group by
	1,2,3;
create index idx1 on dw_tmp.bian_transporter_shishi_paidan_supplier_distance(order_id);
create index idx2 on dw_tmp.bian_transporter_shishi_paidan_supplier_distance(order_group_id);
create index idx3 on dw_tmp.bian_transporter_shishi_paidan_supplier_distance(transporter_id);


-- ------------------------------------------------------------------
-- ------------------------------------------------------------------
-- -----------------------数据宽表-----------------------------------
-- drop table if exists dw_test.bian_paidan_model_data;
-- create table dw_test.bian_paidan_model_data as
insert into dw_test.bian_paidan_model_data
select 
	a.create_dt as create_dt,
	concat(a.order_id,'_',a.order_group_id,'_',a.supplier_id,'_',a.transporter_id) as order_supplier_transporter,
	hour(paidan_time) as paidan_hour,
	case when order_source_from = 'Android' then 0 else 1 end as order_source_from,
	is_cargo_advance_needed,
	tips_amt,
	allowance_amt,
	deliver_fee_amt,
	allowance_amt + deliver_fee_amt as fee_sum,
	transporter_before_one_day_paidan_num,
 	transporter_before_one_day_paidan_success_num,
 	transporter_before_one_day_paidan_lv,
	transporter_before_seven_day_paidan_num,
	transporter_before_seven_day_paidan_success_num,
 	transporter_before_seven_day_paidan_lv,
 	transporter_before_fourteen_day_paidan_num,
 	transporter_before_fourteen_day_paidan_success_num,
 	transporter_before_fourteen_day_paidan_lv,
 	transporter_before_one_day_jiedan_num,
 	transporter_before_one_day_jiedan_success_num,
 	transporter_before_one_day_jiedan_lv,
 	transporter_before_seven_day_jiedan_num,
 	transporter_before_seven_day_jiedan_success_num,
 	transporter_before_seven_day_jiedan_lv,
 	transporter_before_fourteen_day_jiedan_num,
 	transporter_before_fourteen_day_jiedan_success_num,
	transporter_before_fourteen_day_jiedan_lv,
	supplier_before_one_day_paidan_num,
 	supplier_before_one_day_paidan_success_num,
 	supplier_before_one_day_paidan_lv,
	supplier_before_seven_day_paidan_num,
	supplier_before_seven_day_paidan_success_num,
 	supplier_before_seven_day_paidan_lv,
 	supplier_before_fourteen_day_paidan_num,
 	supplier_before_fourteen_day_paidan_success_num,
 	supplier_before_fourteen_day_paidan_lv,
 	supplier_before_one_day_jiedan_num,
 	supplier_before_one_day_jiedan_success_num,
 	supplier_before_one_day_jiedan_lv,
 	supplier_before_seven_day_jiedan_num,
 	supplier_before_seven_day_jiedan_success_num,
 	supplier_before_seven_day_jiedan_lv,
 	supplier_before_fourteen_day_jiedan_num,
 	supplier_before_fourteen_day_jiedan_success_num,
	supplier_before_fourteen_day_jiedan_lv,
	transporter_supplier_before_one_day_paidan_num,
 	transporter_supplier_before_one_day_paidan_success_num,
 	transporter_supplier_before_one_day_paidan_lv,
	transporter_supplier_before_seven_day_paidan_num,
	transporter_supplier_before_seven_day_paidan_success_num,
 	transporter_supplier_before_seven_day_paidan_lv,
 	transporter_supplier_before_fourteen_day_paidan_num,
 	transporter_supplier_before_fourteen_day_paidan_success_num,
 	transporter_supplier_before_fourteen_day_paidan_lv,
 	transporter_supplier_before_one_day_jiedan_num,
 	transporter_supplier_before_one_day_jiedan_success_num,
 	transporter_supplier_before_one_day_jiedan_lv,
 	transporter_supplier_before_seven_day_jiedan_num,
 	transporter_supplier_before_seven_day_jiedan_success_num,
 	transporter_supplier_before_seven_day_jiedan_lv,
 	transporter_supplier_before_fourteen_day_jiedan_num,
 	transporter_supplier_before_fourteen_day_jiedan_success_num,
	transporter_supplier_before_fourteen_day_jiedan_lv,
	case when a.block_id = h.block_id then 1 else 0 end as paidan_block_pipei,
	case when a.block_id = i.block_id then 1 else 0 end as jiedan_block_pipei,
	today_finish_order_num,
	today_never_finish_order_num,
	today_fetch_order_num,
	today_never_fetch_order_num,
	transporter_supplier_distance,
	supplier_receiver_distance,
	paidan_supplier_distance_jiedan_receiver_min,
	paidan_supplier_distance_jiedan_receiver_max,
	paidan_receiver_distance_jiedan_receiver_min,
	paidan_receiver_distance_jiedan_receiver_max,
	paidan_supplier_distance_jiedan_supplier_min,
	paidan_supplier_distance_jiedan_supplier_max,
	paidan_receiver_distance_jiedan_supplier_min,
	paidan_receiver_distance_jiedan_supplier_max,
	label
from 
	dw_tmp.bian_paidan_model_transporter_order a
left join
	dw_tmp.bian_transporter_dongtai_paidan b
on a.transporter_id = b.transporter_id
left join
	dw_tmp.bian_transporter_dongtai_jiedan c
on a.transporter_id = c.transporter_id
left join
	dw_tmp.bian_supplier_dongtai_paidan d 
on a.supplier_id = d.supplier_id
left join
	dw_tmp.bian_supplier_dongtai_jiedan e 
on a.supplier_id = e.supplier_id
left join
	dw_tmp.bian_transporter_supplier_dongtai_paidan f
on a.transporter_id = f.transporter_id and a.supplier_id = f.supplier_id
left join
	dw_tmp.bian_transporter_supplier_dongtai_jiedan g 
on a.transporter_id = g.transporter_id and a.supplier_id = g.supplier_id
left join
	dw_tmp.bian_block_pipei_dongtai_paidan h 
on a.transporter_id = h.transporter_id
left join
	dw_tmp.bian_block_pipei_dongtai_jiedan i 
on a.transporter_id = i.transporter_id
left join 
	dw_tmp.bian_transporter_shishi_paidan_num j 
on a.order_id = j.order_id and a.order_group_id = j.order_group_id and a.transporter_id = j.transporter_id
left join
	dw_tmp.bian_transporter_shishi_paidan_receiver_distance k
on a.order_id = k.order_id and a.order_group_id = k.order_group_id and a.transporter_id = k.transporter_id
left join 
	dw_tmp.bian_transporter_shishi_paidan_supplier_distance m
on a.order_id = m.order_id and a.order_group_id = m.order_group_id and a.transporter_id = m.transporter_id
where
	a.create_dt = @mydate;
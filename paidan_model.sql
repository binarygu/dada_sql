--训练数据
--base:南京
--time:2015-11-30

--------------------------------------------------------------------
--------------------------------------------------------------------
----------------设置时间和城市的用户变量----------------------------
set @mydate = '2015-11-30'
set @mycity = 4

--------------------------------------------------------------------
--------------------------------------------------------------------
------------------------派单静态数据--------------------------------
--派单订单数据 
drop table if exists dw_tmp.bian_paidan_model_order;
create table dw_tmp.bian_paidan_model_order as
select
	create_dt,
	finish_dt,
	finish_time,
	order_id,
	order_group_id,
	transporter_id,
	supplier_id,
	create_time,
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
	city_id = @mycity;
create index idx1 on dw_tmp.bian_paidan_model_order(order_group_id);
create index idx2 on dw_tmp.bian_paidan_model_order(order_id);
create index idx1 on dw_tmp.bian_paidan_model_order(transporter_id);
create index idx2 on dw_tmp.bian_paidan_model_order(supplier_id);


--派单日志数据
drop table if exists dw_tmp.bian_paidan_model_transporter;
create table dw_tmp.bian_paidan_model_transporter as
select
	cal_dt,
	create_time,
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


--派单静态数据
drop table if exists dw_tmp.bian_paidan_model_transporter_order;
create table dw_tmp.bian_paidan_model_transporter_order as
--成功静态数据
select
	cal_dt,
	create_dt,
	a.create_time as paidan_time,
	b.create_time as jiedan_time,
	finish_dt,
	finish_time,
	task_id,
	log_type_id,
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
	order_id <> -99

union 

select 
	cal_dt,
	create_dt,
	a.create_time as paidan_time,
	b.create_time as jiedan_time,
	finish_dt,
	task_id,
	log_type_id,
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
create index idx1 on dw_tmp.bian_paidan_model_transporter_order(transporter_id);
create index idx2 on dw_tmp.bian_paidan_model_transporter_order(supplier_id);


--数据验证
select sum(case when label = 1 then 1 else 0 end) / sum(1) from dw_tmp.bian_paidan_model_transporter_order ;
select * from dw_tmp.bian_paidan_model_transporter_order;
select count(distinct transporter_id) from dw_tmp.bian_paidan_model_transporter_order;


--------------------------------------------------------------------
--------------------------------------------------------------------
-----------------------派单动态数据---------------------------------
--达达动态派单数据
drop table if exists dw_tmp.bian_transporter_dongtai_paidan;
create table dw_tmp.bian_transporter_dongtai_paidan as
select
	@mydate as create_dt,
	transporter_id,
	sum(case when create_dt = date_sub(@mydate,interval 1 day) then 1 else 0 end) transporter_before_one_day_paidan_num,
	sum(case when create_dt = date_sub(@mydate,interval 1 day) and label = 1 then 1 else 0 end) transporter_before_one_day_paidan_success_num,
	sum(case when create_dt = date_sub(@mydate,interval 1 day) and label = 1 then 1 else 0 end) * 1.0 / sum(case when create_dt = date_sub(@mydate,interval 1 day) then 1 else 0 end) transporter_before_one_day_paidan_lv,
	sum(case when create_dt > date_sub(@mydate,interval 7 day) then 1 else 0 end) transporter_before_seven_day_paidan_num,
	sum(case when create_dt > date_sub(@mydate,interval 7 day) and label = 1 then 1 else 0 end) transporter_before_seven_day_paidan_success_num,
	sum(case when create_dt > date_sub(@mydate,interval 7 day) and label = 1 then 1 else 0 end) * 1.0 / sum(case when create_dt > date_sub(@mydate,interval 7 day) then 1 else 0 end) transporter_before_seven_day_paidan_lv,
	sum(1) transporter_before_fourteen_day_paidan_num,
	sum(case when label = 1 then 1 else 0 end) transporter_before_fourteen_day_paidan_success_num,
	sum(case when label = 1 then 1 else 0 end) * 1.0 / sum(1) transporter_before_fourteen_day_paidan_lv
from 
	dw_tmp.bian_paidan_model_transporter_order
where
	create_dt < @mydate
group by 
	1,2;

--达达动态接单数据
drop table if exists dw_tmp.bian_transporter_dongtai_jiedan;
create table dw_tmp.bian_transporter_dongtai_jiedan as
select
	@mydate as create_dt,
	transporter_id,
	sum(case when create_dt = date_sub(@mydate,interval 1 day) then 1 else 0 end) transporter_before_one_day_paidan_num,
	sum(case when create_dt = date_sub(@mydate,interval 1 day) and is_finished = 1 then 1 else 0 end) transporter_before_one_day_paidan_success_num,
	sum(case when create_dt = date_sub(@mydate,interval 1 day) and is_finished = 1 then 1 else 0 end) * 1.0 / sum(case when create_dt = date_sub(@mydate,interval 1 day) then 1 else 0 end) transporter_before_one_day_paidan_lv,
	sum(case when create_dt > date_sub(@mydate,interval 7 day) then 1 else 0 end) transporter_before_seven_day_paidan_num,
	sum(case when create_dt > date_sub(@mydate,interval 7 day) and is_finished = 1 then 1 else 0 end) transporter_before_seven_day_paidan_success_num,
	sum(case when create_dt > date_sub(@mydate,interval 7 day) and is_finished = 1 then 1 else 0 end) * 1.0 / sum(case when create_dt > date_sub(@mydate,interval 7 day) then 1 else 0 end) transporter_before_seven_day_paidan_lv,
	sum(1) transporter_before_fourteen_day_paidan_num,
	sum(case when is_finished = 1 then 1 else 0 end) transporter_before_fourteen_day_paidan_success_num,
	sum(case when is_finished = 1 then 1 else 0 end) * 1.0 / sum(1) transporter_before_fourteen_day_paidan_lv
from 
	dw_tmp.bian_paidan_model_order
where
	create_dt < @mydate
group by 
	1,2;

--商家动态派单数据
drop table if exists dw_tmp.bian_supplier_dongtai_paidan;
create table dw_tmp.bian_supplier_dongtai_paidan as
select
	@mydate as create_dt,
	supplier_id,
	sum(case when create_dt = date_sub(@mydate,interval 1 day) then 1 else 0 end) supplier_before_one_day_paidan_num,
	sum(case when create_dt = date_sub(@mydate,interval 1 day) and label = 1 then 1 else 0 end) supplier_before_one_day_paidan_success_num,
	sum(case when create_dt = date_sub(@mydate,interval 1 day) and label = 1 then 1 else 0 end) * 1.0 / sum(case when create_dt = date_sub(@mydate,interval 1 day) then 1 else 0 end) supplier_before_one_day_paidan_lv,
	sum(case when create_dt > date_sub(@mydate,interval 7 day) then 1 else 0 end) supplier_before_seven_day_paidan_num,
	sum(case when create_dt > date_sub(@mydate,interval 7 day) and label = 1 then 1 else 0 end) supplier_before_seven_day_paidan_success_num,
	sum(case when create_dt > date_sub(@mydate,interval 7 day) and label = 1 then 1 else 0 end) * 1.0 / sum(case when create_dt > date_sub(@mydate,interval 7 day) then 1 else 0 end) supplier_before_seven_day_paidan_lv,
	sum(1) supplier_before_fourteen_day_paidan_num,
	sum(case when label = 1 then 1 else 0 end) supplier_before_fourteen_day_paidan_success_num,
	sum(case when label = 1 then 1 else 0 end) * 1.0 / sum(1) supplier_before_fourteen_day_paidan_lv
from
	dw_tmp.bian_paidan_model_transporter_order
where
	create_dt < @mydate
group by
	1,2;


--商家动态接单数据
drop table if exists dw_tmp.bian_supplier_dongtai_jiedan;
create table dw_tmp.bian_supplier_dongtai_jiedan as
select
	@mydate as create_dt,
	supplier_id,
	sum(case when create_dt = date_sub(@mydate,interval 1 day) then 1 else 0 end) supplier_before_one_day_paidan_num,
	sum(case when create_dt = date_sub(@mydate,interval 1 day) and is_finished = 1 then 1 else 0 end) supplier_before_one_day_paidan_success_num,
	sum(case when create_dt = date_sub(@mydate,interval 1 day) and is_finished = 1 then 1 else 0 end) * 1.0 / sum(case when create_dt = date_sub(@mydate,interval 1 day) then 1 else 0 end) supplier_before_one_day_paidan_lv,
	sum(case when create_dt > date_sub(@mydate,interval 7 day) then 1 else 0 end) supplier_before_seven_day_paidan_num,
	sum(case when create_dt > date_sub(@mydate,interval 7 day) and is_finished = 1 then 1 else 0 end) supplier_before_seven_day_paidan_success_num,
	sum(case when create_dt > date_sub(@mydate,interval 7 day) and is_finished = 1 then 1 else 0 end) * 1.0 / sum(case when create_dt > date_sub(@mydate,interval 7 day) then 1 else 0 end) supplier_before_seven_day_paidan_lv,
	sum(1) supplier_before_fourteen_day_paidan_num,
	sum(case when is_finished = 1 then 1 else 0 end) supplier_before_fourteen_day_paidan_success_num,
	sum(case when is_finished = 1 then 1 else 0 end) * 1.0 / sum(1) supplier_before_fourteen_day_paidan_lv
from 
	dw_tmp.bian_paidan_model_order
where
	create_dt < @mydate
group by
	1,2;

--达达商家交互动态派单数据
drop table if exists dw_tmp.bian_transporter_supplier_dongtai_paidan;
create table dw_tmp.bian_transporter_supplier_dongtai_paidan as
select
	@mydate as create_dt,
	transporter_id,
	supplier_id,
	sum(case when create_dt = date_sub(@mydate,interval 1 day) then 1 else 0 end) transporter_supplier_before_one_day_paidan_num,
	sum(case when create_dt = date_sub(@mydate,interval 1 day) and label = 1 then 1 else 0 end) transporter_supplier_before_one_day_paidan_success_num,
	sum(case when create_dt = date_sub(@mydate,interval 1 day) and label = 1 then 1 else 0 end) * 1.0 / sum(case when create_dt = date_sub(@mydate,interval 1 day) then 1 else 0 end) transporter_supplier_before_one_day_paidan_lv,
	sum(case when create_dt > date_sub(@mydate,interval 7 day) then 1 else 0 end) transporter_supplier_before_seven_day_paidan_num,
	sum(case when create_dt > date_sub(@mydate,interval 7 day) and label = 1 then 1 else 0 end) transporter_supplier_before_seven_day_paidan_success_num,
	sum(case when create_dt > date_sub(@mydate,interval 7 day) and label = 1 then 1 else 0 end) * 1.0 / sum(case when create_dt > date_sub(@mydate,interval 7 day) then 1 else 0 end) transporter_supplier_before_seven_day_paidan_lv,
	sum(1) transporter_supplier_before_fourteen_day_paidan_num,
	sum(case when label = 1 then 1 else 0 end) transporter_supplier_before_fourteen_day_paidan_success_num,
	sum(case when label = 1 then 1 else 0 end) * 1.0 / sum(1) transporter_supplier_before_fourteen_day_paidan_lv
from
	dw_tmp.bian_paidan_model_transporter_order
where
	create_dt < @mydate
group by
	1,2,3;

--达达商家交互动态接单数据
drop table if exists dw_tmp.bian_transporter_supplier_dongtai_jiedan;
create table dw_tmp.bian_transporter_supplier_dongtai_jiedan as
select
	@mydate as create_dt,
	transporter_id,
	supplier_id,
	sum(case when create_dt = date_sub(@mydate,interval 1 day) then 1 else 0 end) transporter_supplier_before_one_day_paidan_num,
	sum(case when create_dt = date_sub(@mydate,interval 1 day) and is_finished = 1 then 1 else 0 end) transporter_supplier_before_one_day_paidan_success_num,
	sum(case when create_dt = date_sub(@mydate,interval 1 day) and is_finished = 1 then 1 else 0 end) * 1.0 / sum(case when create_dt = date_sub(@mydate,interval 1 day) then 1 else 0 end) transporter_supplier_before_one_day_paidan_lv,
	sum(case when create_dt > date_sub(@mydate,interval 7 day) then 1 else 0 end) transporter_supplier_before_seven_day_paidan_num,
	sum(case when create_dt > date_sub(@mydate,interval 7 day) and is_finished = 1 then 1 else 0 end) transporter_supplier_before_seven_day_paidan_success_num,
	sum(case when create_dt > date_sub(@mydate,interval 7 day) and is_finished = 1 then 1 else 0 end) * 1.0 / sum(case when create_dt > date_sub(@mydate,interval 7 day) then 1 else 0 end) transporter_supplier_before_seven_day_paidan_lv,
	sum(1) transporter_supplier_before_fourteen_day_paidan_num,
	sum(case when is_finished = 1 then 1 else 0 end) transporter_supplier_before_fourteen_day_paidan_success_num,
	sum(case when is_finished = 1 then 1 else 0 end) * 1.0 / sum(1) transporter_supplier_before_fourteen_day_paidan_lv
from
	dw_tmp.bian_paidan_model_order
where
	create_dt < @mydate
group by
	1,2,3;

--派单板块匹配数据
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

----接单板块匹配数据
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

--------------------------------------------------------------------
--------------------------------------------------------------------
------------------------派单实时数据--------------------------------



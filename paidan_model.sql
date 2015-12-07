--训练数据
--base:南京
--time:2015-11-30

--------------------------------------------------------------------
--------------------------------------------------------------------
--设置时间用户变量
set @mydate = '2015-11-30'
set @mycity = 4

--派单订单数据
create table dw_tmp.bian_paidan_model_order_bian as
select
	finish_dt,
	order_id,
	order_group_id,
	transporter_id,
	supplier_id,
	create_dt,
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
	create_dt between '2015-11-01' and '2015-11-30' and  
	city_id = 4;


--派单成功日志数据
drop table dw_tmp.paidan_model_transporter_success_bian;
create table dw_tmp.paidan_model_transporter_success_bian as
select
	cal_dt,
	a.task_id as task_id,
	log_type_id,
	transporter_id,
	lat as transporter_lat, 
	lng as transporter_lng,
	order_id,
	order_group_id
from
	(select 
		cal_dt,
		log_type_id,
		transporter_id,
		task_id,
		lat,
		lng
	from 
		bak.dw_log_task_pool
	where
		cal_dt between '2015-11-01' and '2015-11-30' and 
		city_id = 4 and
		log_type_id = 1
	) a

left join
	dw.dw_tsp_task c 
on a.task_id = c.task_id;



--派单失败日志数据
drop table dw_tmp.paidan_model_transporter_fail_bian;
create table dw_tmp.paidan_model_transporter_fail_bian as
select
	cal_dt,
	a.task_id as task_id,
	log_type_id,
	transporter_id,
	lat as transporter_lat, 
	lng as transporter_lng,
	order_id,
	order_group_id
from
	(select 
		cal_dt,
		log_type_id,
		transporter_id,
		task_id,
		lat,
		lng
	from 
		bak.dw_log_task_pool
	where
		cal_dt between '2015-11-01' and '2015-11-30' and 
		city_id = 4 and
		log_type_id = 3
	) a

left join
	dw.dw_tsp_task c 
on a.task_id = c.task_id;


--派单静态数据
create table dw_tmp.paidan_model_transporter_order_bian as
--成功静态数据
select
	cal_dt,
	create_dt,
	create_time,
	finish_dt,
	task_id,
	log_type_id,
	a.transporter_id as transporter_id,
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
	1 as label
from
	(select
		*
	from 
		dw_tmp.paidan_model_transporter_success_bian
	where
		order_id <> -99
	) a
inner join
	dw_tmp.paidan_model_order_bian b
on a.order_id = b.order_id and a.transporter_id = b.transporter_id

union 

select 
	cal_dt,
	create_dt,
	create_time,
	finish_dt,
	task_id,
	log_type_id,
	a.transporter_id as transporter_id,
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
	1 as label
from
	(select
		*
	from 
		dw_tmp.paidan_model_transporter_success_bian 
	where
		order_id = -99
	) a
inner join
	dw_tmp.paidan_model_order_bian b
on a.order_group_id = b.order_group_id and a.transporter_id = b.transporter_id

union
--失败静态数据
select
	cal_dt,
	create_dt,
	create_time,
	finish_dt,
	task_id,
	log_type_id,
	a.transporter_id as transporter_id,
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
	-1 as label
from
	(select
		*
	from 
		dw_tmp.paidan_model_transporter_fail_bian 
	where
		order_id <> -99
	) a
inner join
	dw_tmp.paidan_model_order_bian b
on a.order_id = b.order_id 
where a.transporter_id <> b.transporter_id

union 

select 
	cal_dt,
	create_dt,
	create_time,
	finish_dt,
	task_id,
	log_type_id,
	a.transporter_id as transporter_id,
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
	-1 as label
from
	(select
		*
	from 
		dw_tmp.paidan_model_transporter_fail_bian 
	where
		order_id = -99
	) a
inner join
	dw_tmp.paidan_model_order_bian b
on a.order_group_id = b.order_group_id
where
	a.transporter_id <> b.transporter_id;

--数据验证
select sum(case when label = 1 then 1 else 0 end) / sum(1) from dw_tmp.paidan_model_transporter_order_bian ;
select * from dw_tmp.paidan_model_transporter_order_bian;
select count(distinct transporter_id) from dw_tmp.paidan_model_transporter_order_bian;

--------------------------------------------------------------------
--------------------------------------------------------------------
--达达动态派单数据
create table dw_tmp.transporter_dongtai_paidan_bian as
select
	a.create_dt as create_dt,
	a.transporter_id,
	sum(case when b.create_dt = '2015-11-29' then 1 else 0 end) before_one_day_paidan_num,
	sum(case when b.create_dt = '2015-11-29' and label = 1 then 1 else 0 end) before_one_day_paidan_success_num,
	sum(case when b.create_dt = '2015-11-29' and label = 1 then 1 else 0 end) * 1.0 / sum(case when b.create_dt = '2015-11-29' then 1 else 0 end) before_one_day_paidan_lv,
	sum(case when b.create_dt > '2015-11-22' then 1 else 0 end) before_seven_day_paidan_num,
	sum(case when b.create_dt > '2015-11-22' and label = 1 then 1 else 0 end) before_seven_day_paidan_success_num,
	sum(case when b.create_dt > '2015-11-22' and label = 1 then 1 else 0 end) * 1.0 / sum(case when b.create_dt > '2015-11-22' then 1 else 0 end) before_seven_day_paidan_lv,
	sum(case when b.create_dt > '2015-11-14' then 1 else 0 end) before_fourteen_day_paidan_num,
	sum(case when b.create_dt > '2015-11-14' and label = 1 then 1 else 0 end) before_fourteen_day_paidan_success_num,
	sum(case when b.create_dt > '2015-11-14' and label = 1 then 1 else 0 end) * 1.0 / sum(case when b.create_dt > '2015-11-14' then 1 else 0 end) before_fourteen_day_paidan_lv
from 
	(select
		distinct
		create_dt,
		transporter_id
	from 
		dw_tmp.paidan_model_transporter_order_bian
	where
		create_dt = '2015-11-30'
	) a
left join
	(select
		create_dt,
		transporter_id,
		label
	from
		dw_tmp.paidan_model_transporter_order_bian
	where
		create_dt between '2015-11-15' and '2015-11-29'
	) b
on a.transporter_id = b.transporter_id
group by 
	1,2;

--达达动态接单数据
create table dw_tmp.transporter_dongtai_jiedan_bian as
select
	a.create_dt as create_dt,
	a.transporter_id,
	sum(case when c.create_dt = '2015-11-29' then 1 else 0 end) before_one_day_jiedan_num,
	sum(case when c.create_dt = '2015-11-29' and is_finished = 1 then 1 else 0 end) before_one_day_jiedan_success_num,
	sum(case when c.create_dt = '2015-11-29' and is_finished = 1 then 1 else 0 end) * 1.0 / sum(case when c.create_dt = '2015-11-29' then 1 else 0 end) before_one_day_jiedan_lv,
	sum(case when c.create_dt > '2015-11-22' then 1 else 0 end) before_seven_day_jiedan_num,
	sum(case when c.create_dt > '2015-11-22' and is_finished = 1 then 1 else 0 end) before_seven_day_jiedan_success_num,
	sum(case when c.create_dt > '2015-11-22' and is_finished = 1 then 1 else 0 end) * 1.0 / sum(case when c.create_dt > '2015-11-22' then 1 else 0 end) before_seven_day_jiedan_lv,
	sum(case when c.create_dt > '2015-11-14' then 1 else 0 end) before_fourteen_day_jiedan_num,
	sum(case when c.create_dt > '2015-11-14' and is_finished = 1 then 1 else 0 end) before_fourteen_day_jiedan_success_num,
	sum(case when c.create_dt > '2015-11-14' and is_finished = 1 then 1 else 0 end) * 1.0 / sum(case when c.create_dt > '2015-11-14' then 1 else 0 end) before_fourteen_day_jiedan_lv
from 
	(select
		distinct
		create_dt,
		transporter_id
	from 
		dw_tmp.paidan_model_transporter_order_bian
	where
		create_dt = '2015-11-30'
	) a
left join
	(select
		create_dt,
		transporter_id,
		is_finished
	from
		dw_tmp.paidan_model_order_bian
	where
		create_dt between '2015-11-15' and '2015-11-29' and
		city_id = 4
	) c
on a.transporter_id = c.transporter_id
group by 
	1,2;


--商家动态派单数据
create table dw_tmp.supplier_dongtai_paidan_bian as
select
	a.create_dt as create_dt,
	a.supplier_id as supplier_id,
	sum(case when b.create_dt = '2015-11-29' then 1 else 0 end) before_one_day_paidan_num,
	sum(case when b.create_dt = '2015-11-29' and label = 1 then 1 else 0 end) before_one_day_paidan_success_num,
	sum(case when b.create_dt = '2015-11-29' and label = 1 then 1 else 0 end) * 1.0 / sum(case when b.create_dt = '2015-11-29' then 1 else 0 end) before_one_day_paidan_lv,
	sum(case when b.create_dt > '2015-11-22' then 1 else 0 end) before_seven_day_paidan_num,
	sum(case when b.create_dt > '2015-11-22' and label = 1 then 1 else 0 end) before_seven_day_paidan_success_num,
	sum(case when b.create_dt > '2015-11-22' and label = 1 then 1 else 0 end) * 1.0 / sum(case when b.create_dt > '2015-11-22' then 1 else 0 end) before_seven_day_paidan_lv,
	sum(case when b.create_dt > '2015-11-14' then 1 else 0 end) before_fourteen_day_paidan_num,
	sum(case when b.create_dt > '2015-11-14' and label = 1 then 1 else 0 end) before_fourteen_day_paidan_success_num,
	sum(case when b.create_dt > '2015-11-14' and label = 1 then 1 else 0 end) * 1.0 / sum(case when b.create_dt > '2015-11-14' then 1 else 0 end) before_fourteen_day_paidan_lv
from
	(select
		distinct
		create_dt,
		supplier_id
	from 
		dw_tmp.paidan_model_transporter_order_bian
	where
		create_dt = '2015-11-30'
	) a
left join
	(select
		create_dt,
		supplier_id,
		label
	from
		dw_tmp.paidan_model_transporter_order_bian
	where
		create_dt between '2015-11-15' and '2015-11-29'
	) b
on a.supplier_id = b.supplier_id 
group by
	1,2;


--商家动态接单数据
create table dw_tmp.supplier_dongtai_jiedan_bian as
select
	a.create_dt as create_dt,
	a.supplier_id as supplier_id,
	sum(case when c.create_dt = '2015-11-29' then 1 else 0 end) before_one_day_jiedan_num,
	sum(case when c.create_dt = '2015-11-29' and is_finished = 1 then 1 else 0 end) before_one_day_jiedan_success_num,
	sum(case when c.create_dt = '2015-11-29' and is_finished = 1 then 1 else 0 end) * 1.0 / sum(case when c.create_dt = '2015-11-29' then 1 else 0 end) before_one_day_jiedan_lv,
	sum(case when c.create_dt > '2015-11-22' then 1 else 0 end) before_seven_day_jiedan_num,
	sum(case when c.create_dt > '2015-11-22' and is_finished = 1 then 1 else 0 end) before_seven_day_jiedan_success_num,
	sum(case when c.create_dt > '2015-11-22' and is_finished = 1 then 1 else 0 end) * 1.0 / sum(case when c.create_dt > '2015-11-22' then 1 else 0 end) before_seven_day_jiedan_lv,
	sum(case when c.create_dt > '2015-11-14' then 1 else 0 end) before_fourteen_day_jiedan_num,
	sum(case when c.create_dt > '2015-11-14' and is_finished = 1 then 1 else 0 end) before_fourteen_day_jiedan_success_num,
	sum(case when c.create_dt > '2015-11-14' and is_finished = 1 then 1 else 0 end) * 1.0 / sum(case when c.create_dt > '2015-11-14' then 1 else 0 end) before_fourteen_day_jiedan_lv
from 
	(select
		distinct
		create_dt,
		supplier_id
	from 
		dw_tmp.paidan_model_transporter_order_bian
	where
		create_dt = '2015-11-30'
	) a
left join
	(select
		create_dt,
		supplier_id,
		is_finished
	from
		dw_tmp.paidan_model_order_bian
	where
		create_dt between '2015-11-15' and '2015-11-29' and
		city_id = 4
	) c
on a.supplier_id = c.supplier_id 
group by
	1,2;

--达达商家交互动态派单数据
create table dw_tmp.transporter_supplier_dongtai_paidan_bian as
select
	a.create_dt as create_dt,
	a.transporter_id as transporter_id,
	a.supplier_id as supplier_id,
	sum(case when b.create_dt = '2015-11-29' then 1 else 0 end) before_one_day_paidan_num,
	sum(case when b.create_dt = '2015-11-29' and label = 1 then 1 else 0 end) before_one_day_paidan_success_num,
	sum(case when b.create_dt = '2015-11-29' and label = 1 then 1 else 0 end) * 1.0 / sum(case when b.create_dt = '2015-11-29' then 1 else 0 end) before_one_day_paidan_lv,
	sum(case when b.create_dt > '2015-11-22' then 1 else 0 end) before_seven_day_paidan_num,
	sum(case when b.create_dt > '2015-11-22' and label = 1 then 1 else 0 end) before_seven_day_paidan_success_num,
	sum(case when b.create_dt > '2015-11-22' and label = 1 then 1 else 0 end) * 1.0 / sum(case when b.create_dt > '2015-11-22' then 1 else 0 end) before_seven_day_paidan_lv,
	sum(case when b.create_dt > '2015-11-14' then 1 else 0 end) before_fourteen_day_paidan_num,
	sum(case when b.create_dt > '2015-11-14' and label = 1 then 1 else 0 end) before_fourteen_day_paidan_success_num,
	sum(case when b.create_dt > '2015-11-14' and label = 1 then 1 else 0 end) * 1.0 / sum(case when b.create_dt > '2015-11-14' then 1 else 0 end) before_fourteen_day_paidan_lv
from
	(select
		distinct
		create_dt,
		transporter_id,
		supplier_id
	from 
		dw_tmp.paidan_model_transporter_order_bian
	where
		create_dt = '2015-11-30'
	) a
left join
	(select
		create_dt,
		transporter_id,
		supplier_id,
		label
	from
		dw_tmp.paidan_model_transporter_order_bian
	where
		create_dt between '2015-11-15' and '2015-11-29'
	) b
on a.transporter_id = b.transporter_id and a.supplier_id = b.supplier_id 
group by
	1,2,3;

--达达商家交互动态接单数据
create table dw_tmp.transporter_supplier_dongtai_jiedan_bian as
select
	a.create_dt as create_dt,
	a.transporter_id as transporter_id,
	a.supplier_id as supplier_id,
	sum(case when c.create_dt = '2015-11-29' then 1 else 0 end) before_one_day_jiedan_num,
	sum(case when c.create_dt = '2015-11-29' and is_finished = 1 then 1 else 0 end) before_one_day_jiedan_success_num,
	sum(case when c.create_dt = '2015-11-29' and is_finished = 1 then 1 else 0 end) * 1.0 / sum(case when c.create_dt = '2015-11-29' then 1 else 0 end) before_one_day_jiedan_lv,
	sum(case when c.create_dt > '2015-11-22' then 1 else 0 end) before_seven_day_jiedan_num,
	sum(case when c.create_dt > '2015-11-22' and is_finished = 1 then 1 else 0 end) before_seven_day_jiedan_success_num,
	sum(case when c.create_dt > '2015-11-22' and is_finished = 1 then 1 else 0 end) * 1.0 / sum(case when c.create_dt > '2015-11-22' then 1 else 0 end) before_seven_day_jiedan_lv,
	sum(case when c.create_dt > '2015-11-14' then 1 else 0 end) before_fourteen_day_jiedan_num,
	sum(case when c.create_dt > '2015-11-14' and is_finished = 1 then 1 else 0 end) before_fourteen_day_jiedan_success_num,
	sum(case when c.create_dt > '2015-11-14' and is_finished = 1 then 1 else 0 end) * 1.0 / sum(case when c.create_dt > '2015-11-14' then 1 else 0 end) before_fourteen_day_jiedan_lv
from
	(select
		distinct
		create_dt,
		transporter_id,
		supplier_id
	from 
		dw_tmp.paidan_model_transporter_order_bian
	where
		create_dt = '2015-11-30'
	) a
left join
	(select
		create_dt,
		transporter_id,
		supplier_id,
		is_finished
	from
		dw_tmp.paidan_model_order_bian
	where
		create_dt between '2015-11-15' and '2015-11-29' and
		city_id = 4
	) c
on a.transporter_id = c.transporter_id and a.supplier_id = c.supplier_id 
group by
	1,2,3;



--板块匹配数据
select
	transporter_id,
	max(sum_num)
from
	(select
		a.transporter_id,
		b.block_id,
		sum(1) as sum_num
	from
		(select
			distinct
			transporter_id
		from 
			dw_tmp.paidan_model_transporter_order_bian
		where
			create_dt = '2015-11-30'
		) a
	left join
		(select
			transporter_id,
			block_id
		from
			dw_tmp.paidan_model_transporter_order_bian
		where
			create_dt between '2015-11-15' and '2015-11-29' and
			label = 1
		) b
	on a.transporter_id = b.transporter_id
	) t





left join
	(select
		transporter_id,
		block_id
	from
		dw.dw_tsp_order
	where
		create_dt between '2015-11-15' and '2015-11-29' and
		city_id = 4 and
		is_finished = 1
	) b
on a.transporter_id = b.transporter_id
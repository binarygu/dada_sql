#################################
# @负责人: bianwenbing@imdada.cn
# @描述: 派单模型15天内派单（联合订单）综合数据（临时表,供后边的表使用，第二天删除重新写入）
# @创建日期: 2015-12-17
# @备注: 每天运行之前15天内的订单和派单数据
# @目标表: dw_api.api_paidan_model_transporter_order_history
# @来源表: dw_api.api_paidan_model_transporter_history
#          dw_api.api_paidan_model_order_history
#################################


-- ------------------------------------------------------------------
-- 派单综合历史数据
drop table if exists dw_api.api_paidan_model_transporter_order_history;
create table dw_api.api_paidan_model_transporter_order_history as
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
	dw_api.api_paidan_model_transporter_history a
inner join
	dw_api.api_paidan_model_order_history b
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
	dw_api.api_paidan_model_transporter_history a 
inner join
	dw_api.api_paidan_model_order_history b
on a.order_group_id = b.order_group_id
where
	a.order_id = -99;
create index idx1 on dw_api.api_paidan_model_transporter_order_history(order_group_id);
create index idx2 on dw_api.api_paidan_model_transporter_order_history(order_id);
create index idx3 on dw_api.api_paidan_model_transporter_order_history(transporter_id);
create index idx4 on dw_api.api_paidan_model_transporter_order_history(supplier_id);
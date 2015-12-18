#################################
# @负责人: bianwenbing@imdada.cn
# @描述: 派单模型15天内的订单数据（临时表,供后边的表使用，第二天删除重新写入）
# @创建日期: 2015-12-17
# @备注: 每天运行之前15天内的订单数据
# @目标表: dw_api.api_paidan_model_order_history
# @来源表: dw.dw_tsp_order     
#################################

-- -------------------------------------------------------------
-- 订单历史数据 
drop table if exists dw_api.api_paidan_model_order_history;
create table dw_api.api_paidan_model_order_history as
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
	create_dt between date_sub(curdate(),interval 15 day) and curdate() and 
	order_type_id <> 2;
create index idx1 on dw_api.api_paidan_model_order_history(order_group_id);
create index idx2 on dw_api.api_paidan_model_order_history(order_id);
create index idx3 on dw_api.api_paidan_model_order_history(transporter_id);
create index idx4 on dw_api.api_paidan_model_order_history(supplier_id);
-- ---------------------------------------------------------------------------








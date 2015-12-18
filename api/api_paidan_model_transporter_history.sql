#################################
# @负责人: bianwenbing@imdada.cn
# @描述: 派单模型15天内的派单数据（临时表,供后边的表使用，第二天删除重新写入）
# @创建日期: 2015-12-17
# @备注: 每天运行之前15天内的派单数据
# @目标表: dw_api.api_paidan_model_transporter_history
# @来源表: bak.dw_log_task_pool
#          dw.dw_tsp_task  
#################################

-- ------------------------------------------------------------
-- 派单日志数据
drop table if exists dw_api.api_paidan_model_transporter_history;
create table dw_api.api_paidan_model_transporter_history as
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
	cal_dt between date_sub(curdate(),interval 15 day) and curdate() and  
	log_type_id = 1;
create index idx1 on dw_api.api_paidan_model_transporter_history(order_group_id);
create index idx2 on dw_api.api_paidan_model_transporter_history(order_id);
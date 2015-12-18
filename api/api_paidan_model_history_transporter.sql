#################################
# @负责人: bianwenbing@imdada.cn
# @描述: 派单模型15天内达达数据
# @创建日期: 2015-12-18
# @备注: 每天运行之前15天内达达数据
# @目标表: dw_api.api_paidan_model_history_transporter
# @来源表: dw_api.api_paidan_model_transporter_order_history
#   		dw_api.api_paidan_model_order_history
#################################


-- 达达15天内的派单数据
drop table if exists dw_api.api_paidan_model_history_transporter_paidan;
create table dw_api.api_paidan_model_history_transporter_paidan as
select
	transporter_id,
	sum(case when create_dt = date_sub(curdate(),interval 1 day) then 1 else 0 end) transporter_before_one_day_paidan_num,
	sum(case when create_dt = date_sub(curdate(),interval 1 day) and label = 1 then 1 else 0 end) transporter_before_one_day_paidan_success_num,
	sum(case when create_dt = date_sub(curdate(),interval 1 day) and label = 1 then 1 else 0 end) * 1.0 / sum(case when create_dt = date_sub(curdate(),interval 1 day) then 1 else 0 end) transporter_before_one_day_paidan_lv,
	sum(case when create_dt > date_sub(curdate(),interval 8 day) then 1 else 0 end) transporter_before_seven_day_paidan_num,
	sum(case when create_dt > date_sub(curdate(),interval 8 day) and label = 1 then 1 else 0 end) transporter_before_seven_day_paidan_success_num,
	sum(case when create_dt > date_sub(curdate(),interval 8 day) and label = 1 then 1 else 0 end) * 1.0 / sum(case when create_dt > date_sub(curdate(),interval 8 day) then 1 else 0 end) transporter_before_seven_day_paidan_lv,
	sum(1) transporter_before_fourteen_day_paidan_num,
	sum(case when label = 1 then 1 else 0 end) transporter_before_fourteen_day_paidan_success_num,
	sum(case when label = 1 then 1 else 0 end) * 1.0 / sum(1) transporter_before_fourteen_day_paidan_lv
from 
	dw_api.api_paidan_model_transporter_order_history
where
	create_dt < curdate()
group by 
	1;
create index idx1 on dw_api.api_paidan_model_history_transporter_paidan(transporter_id);


-- 达达15天内的接单数据
drop table if exists dw_api.api_paidan_model_history_transporter_jiedan;
create table dw_api.api_paidan_model_history_transporter_jiedan as
select
	transporter_id,
	sum(case when create_dt = date_sub(curdate(),interval 1 day) then 1 else 0 end) transporter_before_one_day_jiedan_num,
	sum(case when create_dt = date_sub(curdate(),interval 1 day) and is_finished = 1 then 1 else 0 end) transporter_before_one_day_jiedan_success_num,
	sum(case when create_dt = date_sub(curdate(),interval 1 day) and is_finished = 1 then 1 else 0 end) * 1.0 / sum(case when create_dt = date_sub(curdate(),interval 1 day) then 1 else 0 end) transporter_before_one_day_jiedan_lv,
	sum(case when create_dt > date_sub(curdate(),interval 8 day) then 1 else 0 end) transporter_before_seven_day_jiedan_num,
	sum(case when create_dt > date_sub(curdate(),interval 8 day) and is_finished = 1 then 1 else 0 end) transporter_before_seven_day_jiedan_success_num,
	sum(case when create_dt > date_sub(curdate(),interval 8 day) and is_finished = 1 then 1 else 0 end) * 1.0 / sum(case when create_dt > date_sub(curdate(),interval 8 day) then 1 else 0 end) transporter_before_seven_day_jiedan_lv,
	sum(1) transporter_before_fourteen_day_jiedan_num,
	sum(case when is_finished = 1 then 1 else 0 end) transporter_before_fourteen_day_jiedan_success_num,
	sum(case when is_finished = 1 then 1 else 0 end) * 1.0 / sum(1) transporter_before_fourteen_day_jiedan_lv
from 
	dw_api.api_paidan_model_order_history
where
	create_dt < curdate()
group by 
	1;
create index idx1 on dw_api.api_paidan_model_history_transporter_jiedan(transporter_id);



delete from dw_api.api_paidan_model_history_transporter where create_dt = curdate();
insert into dw_api.api_paidan_model_history_transporter
(create_dt,
transporter_id,
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
transporter_before_fourteen_day_jiedan_lv)

select
	curdate(),
	a.transporter_id,
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
	transporter_before_fourteen_day_jiedan_lv
from 
	dw_api.api_paidan_model_history_transporter_jiedan a
left join 
	dw_api.api_paidan_model_history_transporter_paidan b
on a.transporter_id = b.transporter_id

union 

select
	curdate(),
	a.transporter_id,
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
	transporter_before_fourteen_day_jiedan_lv
from 
	dw_api.api_paidan_model_history_transporter_paidan a
left join 
	dw_api.api_paidan_model_history_transporter_jiedan b
on a.transporter_id = b.transporter_id;







-- create table if not exists dw_api.api_paidan_model_history_transporter(
--  id bigint(20) not null primary key auto_increment,
--  create_dt date,
--  transporter_id bigint(20),
--  transporter_before_one_day_paidan_num decimal(10, 2),
--  transporter_before_one_day_paidan_success_num decimal(10, 2),
-- 	transporter_before_one_day_paidan_lv decimal(10, 6),
-- 	transporter_before_seven_day_paidan_num decimal(10, 2),
--  transporter_before_seven_day_paidan_success_num decimal(10, 2),
--  transporter_before_seven_day_paidan_lv decimal(10, 6),
-- 	transporter_before_fourteen_day_paidan_num decimal(10, 2),
--  transporter_before_fourteen_day_paidan_success_num decimal(10, 2),
--  transporter_before_fourteen_day_paidan_lv decimal(10, 6),

-- 	transporter_before_one_day_jiedan_num decimal(10, 2),
-- 	transporter_before_one_day_jiedan_success_num decimal(10, 2),
-- 	transporter_before_one_day_jiedan_lv decimal(10, 6),
--  transporter_before_seven_day_jiedan_num decimal(10, 2),
--  transporter_before_seven_day_jiedan_success_num decimal(10, 2),
--  transporter_before_seven_day_jiedan_lv decimal(10, 6),
-- 	transporter_before_fourteen_day_jiedan_num decimal(10, 2),
--  transporter_before_fourteen_day_jiedan_success_num decimal(10, 2),
--  transporter_before_fourteen_day_jiedan_lv decimal(10, 6)
-- );
-- create index idx1 on dw_api.api_paidan_model_history_transporter(transporter_id);
-- create index idx2 on dw_api.api_paidan_model_history_transporter(create_dt);



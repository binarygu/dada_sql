#################################
# @负责人: bianwenbing@imdada.cn
# @描述: 派单模型15天内达达派单数据
# @创建日期: 2015-12-17
# @备注: 每天运行之前15天内达达派单数据
# @目标表: dw_api.api_paidan_model_history_transporter_paidan
# @来源表: dw_api.api_paidan_model_transporter_order_history
#################################

create table if not exists dw_api.api_paidan_model_history_transporter_paidan(
    id bigint(20) not null primary key auto_increment,
    create_dt date,
    transporter_id bigint(20),
	transporter_before_one_day_paidan_num decimal(10, 2),
	transporter_before_one_day_paidan_success_num decimal(10, 2),
	transporter_before_one_day_paidan_lv decimal(10, 6),
 	transporter_before_seven_day_paidan_num decimal(10, 2),
 	transporter_before_seven_day_paidan_success_num decimal(10, 2),
 	transporter_before_seven_day_paidan_lv decimal(10, 6),
	transporter_before_fourteen_day_paidan_num decimal(10, 2),
 	transporter_before_fourteen_day_paidan_success_num decimal(10, 2),
 	transporter_before_fourteen_day_paidan_lv decimal(10, 6)
);
create index idx1 on dw_api.api_paidan_model_history_transporter_paidan(transporter_id);
create index idx2 on dw_api.api_paidan_model_history_transporter_paidan(create_dt);


-- 达达15天内的派单数据
insert into dw_api.api_paidan_model_history_transporter_paidan
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
transporter_before_fourteen_day_paidan_lv)
select
	curdate(),
	transporter_id,
	sum(case when create_dt = date_sub(curdate(),interval 1 day) then 1 else 0 end),
	sum(case when create_dt = date_sub(curdate(),interval 1 day) and label = 1 then 1 else 0 end),
	sum(case when create_dt = date_sub(curdate(),interval 1 day) and label = 1 then 1 else 0 end) * 1.0 / sum(case when create_dt = date_sub(curdate(),interval 1 day) then 1 else 0 end),
	sum(case when create_dt > date_sub(curdate(),interval 8 day) then 1 else 0 end),
	sum(case when create_dt > date_sub(curdate(),interval 8 day) and label = 1 then 1 else 0 end),
	sum(case when create_dt > date_sub(curdate(),interval 8 day) and label = 1 then 1 else 0 end) * 1.0 / sum(case when create_dt > date_sub(curdate(),interval 8 day) then 1 else 0 end),
	sum(1),
	sum(case when label = 1 then 1 else 0 end),
	sum(case when label = 1 then 1 else 0 end) * 1.0 / sum(1)
from 
	dw_api.api_paidan_model_transporter_order_history
where
	create_dt < curdate()
group by 
	1,2;


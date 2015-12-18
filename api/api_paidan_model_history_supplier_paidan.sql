#################################
# @负责人: bianwenbing@imdada.cn
# @描述: 派单模型15天内商家派单数据
# @创建日期: 2015-12-17
# @备注: 每天运行之前15天内商家派单数据
# @目标表: dw_api.api_paidan_model_history_supplier_paidan
# @来源表: dw_api.api_paidan_model_transporter_order_history
#################################

create table if not exists dw_api.api_paidan_model_history_supplier_paidan(
    id bigint(20) not null primary key auto_increment,
    create_dt date,
    supplier_id bigint(20),
	supplier_before_one_day_paidan_num decimal(10, 2),
	supplier_before_one_day_paidan_success_num decimal(10, 2),
	supplier_before_one_day_paidan_lv decimal(10, 6),
 	supplier_before_seven_day_paidan_num decimal(10, 2),
 	supplier_before_seven_day_paidan_success_num decimal(10, 2),
 	supplier_before_seven_day_paidan_lv decimal(10, 6),
	supplier_before_fourteen_day_paidan_num decimal(10, 2),
 	supplier_before_fourteen_day_paidan_success_num decimal(10, 2),
 	supplier_before_fourteen_day_paidan_lv decimal(10, 6)
);
create index idx1 on dw_api.api_paidan_model_history_supplier_paidan(supplier_id);
create index idx2 on dw_api.api_paidan_model_history_supplier_paidan(create_dt);


-- 商家15天内的派单数据
insert into dw_api.api_paidan_model_history_supplier_paidan
(create_dt,
supplier_id,
supplier_before_one_day_paidan_num,
supplier_before_one_day_paidan_success_num,
supplier_before_one_day_paidan_lv,
supplier_before_seven_day_paidan_num,
supplier_before_seven_day_paidan_success_num,
supplier_before_seven_day_paidan_lv,
supplier_before_fourteen_day_paidan_num,
supplier_before_fourteen_day_paidan_success_num,
supplier_before_fourteen_day_paidan_lv)
select
	curdate(),
	supplier_id,
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

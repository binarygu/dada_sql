#################################
# @负责人: bianwenbing@imdada.cn
# @描述: 派单模型15天内达达商家接单数据
# @创建日期: 2015-12-17
# @备注: 每天运行之前15天内达达商家接单数据
# @目标表: dw_api.api_paidan_model_history_transporter_supplier_jiedan
# @来源表: dw_api.api_paidan_model_order_history
#################################

create table if not exists dw_api.api_paidan_model_history_transporter_supplier_jiedan(
    id bigint(20) not null primary key auto_increment,
    create_dt date,
    transporter_id bigint(20),
    supplier_id bigint(20),
	transporter_supplier_before_one_day_jiedan_num decimal(10, 2),
	transporter_supplier_before_one_day_jiedan_success_num decimal(10, 2),
	transporter_supplier_before_one_day_jiedan_lv decimal(10, 6),
 	transporter_supplier_before_seven_day_jiedan_num decimal(10, 2),
 	transporter_supplier_before_seven_day_jiedan_success_num decimal(10, 2),
 	transporter_supplier_before_seven_day_jiedan_lv decimal(10, 6),
	transporter_supplier_before_fourteen_day_jiedan_num decimal(10, 2),
 	transporter_supplier_before_fourteen_day_jiedan_success_num decimal(10, 2),
 	transporter_supplier_before_fourteen_day_jiedan_lv decimal(10, 6)
);
create index idx1 on dw_api.api_paidan_model_history_transporter_supplier_jiedan(transporter_id);
create index idx2 on dw_api.api_paidan_model_history_transporter_supplier_jiedan(supplier_id);
create index idx3 on dw_api.api_paidan_model_history_transporter_supplier_jiedan(create_dt);


-- 达达商户15天内的接单数据
insert into dw_api.api_paidan_model_history_transporter_supplier_jiedan
(create_dt,
transporter_id,
supplier_id,
transporter_supplier_before_one_day_jiedan_num,
transporter_supplier_before_one_day_jiedan_success_num,
transporter_supplier_before_one_day_jiedan_lv,
transporter_supplier_before_seven_day_jiedan_num,
transporter_supplier_before_seven_day_jiedan_success_num,
transporter_supplier_before_seven_day_jiedan_lv,
transporter_supplier_before_fourteen_day_jiedan_num,
transporter_supplier_before_fourteen_day_jiedan_success_num,
transporter_supplier_before_fourteen_day_jiedan_lv)
select
	curdate(),
	transporter_id,
	supplier_id,
	sum(case when create_dt = date_sub(curdate(),interval 1 day) then 1 else 0 end),
	sum(case when create_dt = date_sub(curdate(),interval 1 day) and is_finished = 1 then 1 else 0 end) ,
	sum(case when create_dt = date_sub(curdate(),interval 1 day) and is_finished = 1 then 1 else 0 end) * 1.0 / sum(case when create_dt = date_sub(curdate(),interval 1 day) then 1 else 0 end),
	sum(case when create_dt > date_sub(curdate(),interval 8 day) then 1 else 0 end),
	sum(case when create_dt > date_sub(curdate(),interval 8 day) and is_finished = 1 then 1 else 0 end),
	sum(case when create_dt > date_sub(curdate(),interval 8 day) and is_finished = 1 then 1 else 0 end) * 1.0 / sum(case when create_dt > date_sub(curdate(),interval 8 day) then 1 else 0 end) ,
	sum(1),
	sum(case when is_finished = 1 then 1 else 0 end),
	sum(case when is_finished = 1 then 1 else 0 end) * 1.0 / sum(1) 
from
	dw_api.api_paidan_model_order_history
where
	create_dt < curdate()
group by
	1,2,3;
#################################
# @负责人: bianwenbing@imdada.cn
# @描述: 商户画像基础信息
# @创建日期: 2015-12-25
# @备注: 每天运行dw.dw_plq_ord_order_head中商户前30天的数据,
#        计算总销售额，客单价，总订单数，活跃天数（有订单的天数），今天距第一次订单的日期
# @目标表: dw_api.api_plq_supplier_photo_basic_info
# @来源表: dw.dw_plq_ord_order_head
#################################


drop table if exists dw_tmp.bian_plq_create_order_date;
create table dw_tmp.bian_plq_create_order_date as 
select 
	supplier_id,
	min(create_dt) as first_order_dt
from 
	dw.dw_plq_ord_order_head
where 
	is_finished = 1
group by
	supplier_id;
create index inx1 on dw_tmp.bian_plq_create_order_date(supplier_id);


delete from dw_api.api_plq_supplier_photo_basic_info where create_dt = curdate(); 
insert into dw_api.api_plq_supplier_photo_basic_info
(create_dt,
supplier_id,
city_id,
fee_sum,
fee_per_user,
order_shuliang,
active_day,
active_lv,
create_day
)
select
	curdate() as create_dt,
	a.supplier_id as supplier_id,
	city_id,
	sum(order_allowance_amt + order_pay_amt) as fee_sum,
	sum(order_allowance_amt + order_pay_amt) / sum(1) as fee_per_user,
	sum(1) as order_shuliang,
	count(distinct create_dt) as active_day,
	case
		when datediff(curdate(),first_order_dt) < 30 then count(distinct create_dt) / datediff(curdate(),first_order_dt)
		else count(distinct create_dt) / 30 
	end as active_lv,
	datediff(curdate(),first_order_dt) as create_day
from 
	dw.dw_plq_ord_order_head a
left join
	dw_tmp.bian_plq_create_order_date b
on a.supplier_id = b.supplier_id
where 
	a.create_dt between date_sub(curdate(),interval 30 day) and date_sub(curdate(),interval 1 day) and 
	a.is_finished = 1
group by 
	1,2,3,9;


create table if not exists dw_api.api_plq_supplier_photo_basic_info(
 	id bigint(20) not null primary key auto_increment,
 	create_dt date,
 	supplier_id bigint(20),
 	city_id int(11) ,
 	fee_sum decimal(15, 2),
	fee_per_user decimal(10, 2),
	order_shuliang int(11),
 	active_day int(11),
 	active_lv decimal(10, 4),
 	create_day int(11)
);
create index idx1 on dw_api.api_plq_supplier_photo_basic_info(supplier_id);
create index idx2 on dw_api.api_plq_supplier_photo_basic_info(create_dt);


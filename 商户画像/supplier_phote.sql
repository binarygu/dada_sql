#####################################################################################################
#商户画像
#每天取最近30天的商家计算总销售额，客单价，总订单数，活跃天数（有订单的天数），今天距第一次订单的日期
#创建时间：2015-12-23
######################################################################################################

drop table if exists dw_tmp.bian_plq_supplier_first_order_date;
create table dw_tmp.bian_plq_supplier_first_order_date as 
select 
	supplier_id,
	min(create_dt) as first_order_dt
from 
	dw.dw_plq_ord_order_head
where 
	is_finished = 1
group by
	supplier_id;
create index inx1 on dw_tmp.bian_plq_supplier_first_order_date(supplier_id);

-- create table dw_test.bian_plq_supplier_photo as 
delete from dw_test.bian_plq_supplier_photo where create_dt = curdate(); 
insert into dw_test.bian_plq_supplier_photo
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
	datediff(curdate(),first_order_dt) as supplier_first_date
from 
	dw.dw_plq_ord_order_head a
left join
	dw_tmp.bian_plq_supplier_first_order_date b
on a.supplier_id = b.supplier_id
where 
	a.create_dt between date_sub(curdate(),interval 30 day) and date_sub(curdate(),interval 1 day) and 
	a.is_finished = 1
group by 
	1,2,3,9;
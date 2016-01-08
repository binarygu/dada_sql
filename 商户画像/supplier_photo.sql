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


-- delete from dw_api.api_plq_supplier_photo_basic_info where create_dt = curdate(); 
-- insert into dw_api.api_plq_supplier_photo_basic_info
drop table dw_tmp.bian_test;
create table dw_tmp.bian_test as
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

create table dw_test.bian_plq_supplier_photo as 
-- delete from dw_test.bian_plq_supplier_photo where create_dt = curdate(); 
-- insert into dw_test.bian_plq_supplier_photo
select 
	create_dt,
	supplier_id,
	city_id,
	case 
		when fee_per_user < 20 then '0-20yuan'
		when fee_per_user >= 20 and fee_per_user < 25 then '20-25yuan'
		when fee_per_user >= 25 and fee_per_user < 30 then '25-30yuan'
		when fee_per_user >= 30 and fee_per_user < 35 then '30-35yuan'
		when fee_per_user >= 35 and fee_per_user < 40 then '35-40yuan'
		else '40+yuan'
	end as fee_per_user_label,
	case
		when order_shuliang < 10 then '0-10order'
		when order_shuliang >= 10 and order_shuliang < 50 then '10-50order'
		when order_shuliang >= 50 and order_shuliang < 100 then '50-100order'
		when order_shuliang >= 100 and order_shuliang < 200 then '100-200order'
		when order_shuliang >= 200 and order_shuliang < 500 then '200-500order'
		else '500+order'
	end as order_shuliang_label,
	case 
 		when active_lv < 0.9 then 'low'
		else 'high'
	end as active_label,
	case 
		when create_day < 3 then '0-3day'
		when create_day >= 3 and create_day < 7 then '3-7day'
		when create_day >= 7 and create_day < 15 then '7-15day'
		when create_day >= 15 and create_day < 30 then '15-30day'
		else '30+day'
	end as create_day_label
from
	dw_tmp.bian_test
where
	create_dt = curdate();
	




#################################
# @负责人: bianwenbing@imdada.cn
# @描述: 商户画像
# @创建日期: 2015-12-25
# @备注: 把用户画像基础信息表中的数据标签化
# @目标表: dw_api.api_plq_supplier_photo
# @来源表: dw_api.api_plq_supplier_photo_basic_info
#################################


delete from dw_api.api_plq_supplier_photo where create_dt = curdate(); 
insert into dw_api.api_plq_supplier_photo
(create_dt,
supplier_id,
city_id,
fee_per_user_label,
order_shuliang_label,
active_label,
create_day_label)

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
	dw_api.api_plq_supplier_photo_basic_info
where
	create_dt = curdate();
	

create table if not exists dw_api.api_plq_supplier_photo(
 	id bigint(20) not null primary key auto_increment,
 	create_dt date,
 	supplier_id bigint(20),
 	city_id int(4) ,
	fee_per_user_label varchar(20),
	order_shuliang_label varchar(20),
	active_label varchar(20),
	create_day_label varchar(20)
);
create index idx1 on dw_api.api_plq_supplier_photo(supplier_id);
create index idx2 on dw_api.api_plq_supplier_photo(create_dt);


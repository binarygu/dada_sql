-- 近15天的订单量
create table dw_tmp.bian_order_supplier_tmp as	
select 
	supplier_id,
	count(distinct order_id) as order_num
from
	dw.dw_tsp_order 
where 
	create_dt between '2015-12-01' and '2015-12-14' and 
	city_id = 1 and 
	area_id = 1
group by 
	1;
create index ix1 on dw_tmp.bian_order_supplier_tmp(supplier_id);

-- 商户作弊分数
create table dw_tmp.bian_supplier_zuobi_score as 							
select  
	supplier_id,
	cal_dt,
	zuobi_score,
	if(@pa=a.supplier_id,@rank:=@rank+1,@rank:=1) as rank,
	@pa:=a.supplier_id												
from 
	(select 
		supplier_id,
		cal_dt,
		zuobi_score
	from 
		dw_api.api_grade_shanghu_zuobi_score 
	where 
		city_name = '上海'																			
	order by    
		supplier_id,
		cal_dt desc    
	)  a,
	(select @rank:=0 ,@pa:=null) t
having rank=1;	
create index ix1 on dw_tmp.bian_supplier_zuobi_score(supplier_id);

-- 商户信息与作弊分
select
	a.supplier_id as supplier_id,
	a.supplier_name as supplier_name,
	a.supplier_address as supplier_address,
	a.supplier_phone as supplier_phone,
	c.zuobi_score as zuobi_score,
	b.order_num as order_num
from
	dw.dw_usr_supplier a
left join
	dw_tmp.bian_order_supplier_tmp b
on a.supplier_id = b.supplier_id
left join
	dw_tmp.bian_supplier_zuobi_score c
on a.supplier_id = c.supplier_id
where
	a.city_id = 1 and 
	a.area_id = 1 and 
	a.supplier_status = 2
order by 
	order_num desc;												




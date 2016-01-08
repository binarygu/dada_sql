-- 商户近7天的物流端订单量
drop table if exists dw_tmp.bian_order_supplier_tsp;
create table dw_tmp.bian_order_supplier_tsp as
select 
	supplier_id,
	count(distinct order_id) as tsp_order_num
from
	dw.dw_tsp_order 
where 
	create_dt between '2015-12-21' and '2015-12-27' and 
	city_id = 1 and 
	area_id = 1 and 
	is_finished = 1
group by 
	1;
create index ix1 on dw_tmp.bian_order_supplier_tsp(supplier_id);

-- 商户近7天派乐趣端订单
drop table if exists dw_tmp.bian_order_supplier_plq;
create table dw_tmp.bian_order_supplier_plq as	
select 
	supplier_id,
	count(distinct order_id) as plq_order_num
from
	dw.dw_plq_ord_order_head
where 
	create_dt between '2015-12-21' and '2015-12-27' and 
	city_id = 1 and 
	is_finished = 1
group by 
	1;
create index ix1 on dw_tmp.bian_order_supplier_plq(supplier_id);



-- 商户作弊分数
drop table if exists dw_tmp.bian_supplier_zuobi_score;
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

-- 没辞职的BD信息
drop table if exists dw_tmp.bian_bd;
create table dw_tmp.bian_bd as 
select 
	bd_id,
	bd_name
from
	dw.dw_usr_bd
where
	is_resigned = 0;



-- 商户信息
drop table if exists dw_tmp.bian_supplier_info;
create table dw_tmp.bian_supplier_info as 
select 
	supplier_id,
	supplier_contact_name as supplier_name,
	supplier_contact_address as supplier_address,
	supplier_contact_mobile as supplier_phone,
	area_id,
	block_id,
	update_time,
	if(@pa=a.supplier_id,@rank:=@rank+1,@rank:=1) as rank,
	@pa:=a.supplier_id	
from 
	(select
		supplier_id,
		supplier_contact_name,
		supplier_contact_address,
		supplier_contact_mobile,
		area_id,
		block_id,
		update_time
	from
		dw.dw_usr_supplier_contact
	where 
		area_id = 1 and 
		is_del = 0
	order by 
		supplier_id,
		update_time desc) a,
	(select @rank:=0 ,@pa:=null) t
having rank=1;
create index ix1 on dw_tmp.bian_supplier_info(supplier_id);


-- 商户信息与作弊分
select
	a.supplier_id as supplier_id,
	a.supplier_name as supplier_name,
	a.supplier_address as supplier_address,
	a.supplier_phone as supplier_phone,
	a.block_id as block_id,
	e.block_name as block_name,
	g.bd_id as bd_id,
	f.bd_name as bd_name,
	c.zuobi_score as zuobi_score,
	b.tsp_order_num as tsp_order_num,
	d.plq_order_num as plq_order_num
from
	dw_tmp.bian_supplier_info a
left join
	dw_tmp.bian_order_supplier_tsp b
on a.supplier_id = b.supplier_id
left join
	dw_tmp.bian_supplier_zuobi_score c
on a.supplier_id = c.supplier_id
left join 
	dw_tmp.bian_order_supplier_plq d 
on a.supplier_id = d.supplier_id
left join
	dim.dim_block e
on a.block_id = e.block_id 
left join 
	dw.dw_usr_supplier g
on a.supplier_id = g.supplier_id
left join 
	dw_tmp.bian_bd f
on g.bd_id = f.bd_id
order by 
	tsp_order_num desc;												

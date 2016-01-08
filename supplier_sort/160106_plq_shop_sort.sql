#/*
#负责人: 左俊杰
#描述: 对用商户的排序进行评分
#需求: 需要每天更新到dw_api，并推送到新项目的服务器上
#
#日期: 2016-01-06

#目标表: dw_api.api_plq_shop_sort
#来源表: dw.dw_plq_ord_order_head  dw.dw_plq_usr_supplier_shop_manage ods.`ods_plq_order_appraisal`
#  ods.`ods_plq_supplier_operate_record`  ods.ods_plq_supplier_blocked_record 
#  ods.`ods_plq_goods` ods.`ods_plq_goods_type ods.ods_plq_supplier_qualification
*/

#近7天订单量
SET @f_dt=DATE_SUB(CURDATE(),INTERVAL 7 DAY);
SET @t_dt=CURDATE();

drop table if exists dw_tmp.zuo_cshop_sort_1;
create table dw_tmp.zuo_cshop_sort_1 as
select 
supplier_id,COUNT(DISTINCT order_id) AS order_total,
  COUNT(DISTINCT CASE WHEN is_finished=1 THEN order_id END) AS order_finish,
  COUNT(DISTINCT CASE WHEN order_status_id=5 THEN order_id END) AS order_cancel,
  COUNT(DISTINCT CASE WHEN order_status_id=6 THEN order_id END) AS order_payfaild,
  round(COUNT(DISTINCT CASE WHEN order_status_id=5 THEN order_id END)/COUNT(DISTINCT order_id),3) as cancel_rate
FROM dw.dw_plq_ord_order_head
WHERE create_dt>=@f_dt AND create_dt<@t_dt
GROUP BY supplier_id
;

create index zuo_cshop_sort_1 on dw_tmp.zuo_cshop_sort_1(supplier_id);

#近60天评价分数
DROP TABLE IF EXISTS dw_tmp.zuo_cshop_sort_2;
CREATE TABLE dw_tmp.zuo_cshop_sort_2 AS
SELECT supplier_id,COUNT(id) AS comm_num,
  ROUND(AVG(express_score),1) AS avg_express_score,
  ROUND(AVG(shop_score),1) AS avg_shop_score,
  ROUND(AVG(dishes_score),1) AS avg_dishes_score,
  ROUND((AVG(express_score)*0.2+AVG(shop_score)*0.4+AVG(dishes_score)*0.4),1) AS grade_t
FROM ods.`ods_plq_order_appraisal` 
WHERE create_time>=DATE_SUB(CURDATE(),INTERVAL 60 DAY)
AND create_time<CURDATE()
GROUP BY supplier_id
;

update dw_tmp.zuo_cshop_sort_2 set grade_t=4.5 where comm_num<5;
create index zuo_cshop_sort_2 on dw_tmp.zuo_cshop_sort_2(supplier_id);

#--商户列表--
drop table if exists dw_tmp.zuo_cshop_sort_3_1;
create table dw_tmp.zuo_cshop_sort_3_1
select supplier_id,city_id,supplier_alias_name,shop_phone_number,create_dt,
transporter_delivery_flag
from dw.dw_plq_usr_supplier_shop_manage
;

create index zuo_cshop_sort_3_1 on dw_tmp.zuo_cshop_sort_3_1(supplier_id);

#商户最早上线日期
drop table if exists dw_tmp.zuo_cshop_first_on_1;
create table dw_tmp.zuo_cshop_first_on_1 as
SELECT supplier_id,DATE(MIN(create_time)) AS first_on_dt
FROM ods.`ods_plq_supplier_operate_record`
WHERE shop_status=10
GROUP BY supplier_id
;


create index zuo_cshop_first_on_1 on dw_tmp.zuo_cshop_first_on_1(supplier_id);

drop table if exists dw_tmp.zuo_cshop_sort_3_2;
create table dw_tmp.zuo_cshop_sort_3_2
select a.*,b.first_on_dt
from dw_tmp.zuo_cshop_sort_3_1 a join dw_tmp.zuo_cshop_first_on_1 b
on a.supplier_id=b.supplier_id
;

create index zuo_cshop_sort_3_2 on dw_tmp.zuo_cshop_sort_3_2(supplier_id);


#近30天拉黑记录
drop table if exists dw_tmp.zuo_cshop_black_1;
create table dw_tmp.zuo_cshop_black_1 as
SELECT DISTINCT supplier_id
FROM ods.ods_plq_supplier_blocked_record
WHERE create_time>=DATE_SUB(CURDATE(),INTERVAL 30 DAY)
and is_blocked=1
;

create index zuo_cshop_black_1 on dw_tmp.zuo_cshop_black_1(supplier_id);

drop table if exists dw_tmp.zuo_cshop_black_2;
create table dw_tmp.zuo_cshop_black_2 as
SELECT DISTINCT supplier_id
FROM ods.ods_plq_supplier_blocked_record
WHERE create_time>=DATE_SUB(CURDATE(),INTERVAL 14 DAY)
and is_blocked=1
;

create index zuo_cshop_black_2 on dw_tmp.zuo_cshop_black_2(supplier_id);


DROP TABLE IF EXISTS dw_tmp.zuo_cshop_sort_3_3;
CREATE TABLE dw_tmp.zuo_cshop_sort_3_3
SELECT a.*,
  (CASE WHEN b.supplier_id IS NOT NULL THEN 1 ELSE 0 END) AS black_30day,
  (CASE WHEN c.supplier_id IS NOT NULL THEN 1 ELSE 0 END) AS black_14day
FROM  dw_tmp.zuo_cshop_sort_3_2 a
LEFT JOIN dw_tmp.zuo_cshop_black_1 b ON a.supplier_id=b.supplier_id
LEFT JOIN dw_tmp.zuo_cshop_black_2 c ON a.supplier_id=c.supplier_id
;


create index zuo_cshop_sort_3_3 on dw_tmp.zuo_cshop_sort_3_3(supplier_id);

#商户商品无图比例
drop table if exists dw_tmp.zuo_cshop_goodtype_1;
create table dw_tmp.zuo_cshop_goodtype_1 as
SELECT id FROM ods.ods_plq_goods_type
WHERE is_del=0
;
CREATE INDEX zuo_cshop_goodtype_1 ON dw_tmp.zuo_cshop_goodtype_1(id);

drop table if exists dw_tmp.zuo_cshop_goodtype_2;
create table dw_tmp.zuo_cshop_goodtype_2 as
SELECT id,supplier_id,type_id,image,on_sale,is_del
FROM ods.`ods_plq_goods` 
;

CREATE INDEX zuo_cshop_goodtype_2 ON dw_tmp.zuo_cshop_goodtype_2(type_id);

drop table if exists dw_tmp.zuo_cshop_sort_img_1;
create table dw_tmp.zuo_cshop_sort_img_1 as
select supplier_id,count(a.id) as goods_n,
  count(case when image='' then a.id end) as goods_image_miss,
  round(count(case when image='' then a.id end)/count(a.id),3) as miss_p
from dw_tmp.zuo_cshop_goodtype_2 a join  dw_tmp.zuo_cshop_goodtype_1 b
on a.type_id=b.id
where on_sale=1 and is_del=0
group by supplier_id
;

create index zuo_cshop_sort_img_1 on dw_tmp.zuo_cshop_sort_img_1(supplier_id);

drop table if exists dw_tmp.zuo_cshop_sort_3_4;
create table dw_tmp.zuo_cshop_sort_3_4
	select a.*,ifnull(b.goods_n,0) as goods_n,
    ifnull(b.miss_p,0) as miss_img_p
	from dw_tmp.zuo_cshop_sort_3_3 a
	left join dw_tmp.zuo_cshop_sort_img_1 b on a.supplier_id=b.supplier_id
;
create index zuo_cshop_sort_3_4 on dw_tmp.zuo_cshop_sort_3_4(supplier_id);

#---商户资质上传---
drop table if exists dw_tmp.zuo_shop_qualification_1;
create table dw_tmp.zuo_shop_qualification_1 as
SELECT supplier_id,COUNT(DISTINCT `type`) AS qualification_type_n
FROM ods.ods_plq_supplier_qualification
GROUP BY supplier_id
;
create index zuo_shop_qualification_1 on dw_tmp.zuo_shop_qualification_1(supplier_id);

#----商户自出补贴的情况---

#------未上-----

DROP TABLE IF EXISTS dw_tmp.zuo_cshop_sort_3_5;
CREATE TABLE dw_tmp.zuo_cshop_sort_3_5
  SELECT date_sub(@t_dt,interval 1 day) as cal_dt,a.*,
  TIMESTAMPDIFF(DAY,first_on_dt,@t_dt) AS on_day,
  IFNULL(b.order_total,0) AS order_total,IFNULL(b.order_finish,0) AS order_finish,
  IFNULL(b.order_cancel,0) AS order_cancel,
  IFNULL(b.order_payfaild,0) AS order_payfaild,
  IFNULL(b.cancel_rate,0) AS cancel_rate,
  IFNULL(c.comm_num,0) AS comm_num,IFNULL(c.avg_express_score,0) AS avg_express_score,
  IFNULL(c.avg_shop_score,0) AS avg_shop_score,IFNULL(c.avg_dishes_score,0) AS avg_dishes_score,
  IFNULL(c.grade_t,0) AS grade_t,round(IFNULL(c.grade_t,0)*2,0) AS comment_score,
  ifnull(d.qualification_type_n,0) as qualification_type_n
FROM  dw_tmp.zuo_cshop_sort_3_4 a
LEFT JOIN dw_tmp.zuo_cshop_sort_1 b ON a.supplier_id=b.supplier_id
LEFT JOIN dw_tmp.zuo_cshop_sort_2 c ON a.supplier_id=c.supplier_id
left join dw_tmp.zuo_shop_qualification_1 d on a.supplier_id=d.supplier_id
;

#如果没有评论，将评论分置默认值(4.5,9)
update dw_tmp.zuo_cshop_sort_3_5 set grade_t=4.5,comment_score=9 where comm_num=0;

DROP TABLE IF EXISTS dw_tmp.zuo_cshop_sort_3_6;
CREATE TABLE dw_tmp.zuo_cshop_sort_3_6
select *,
  (case when on_day>7 then 
    (case when ROUND(order_finish/7,0)<=5 then 4
      when ROUND(order_finish/7,0)>5 AND ROUND(order_finish/7,0)<=10 then 5
      when ROUND(order_finish/7,0)>10 AND ROUND(order_finish/7,0)<=20 THEN 6
      WHEN ROUND(order_finish/7,0)>20 AND ROUND(order_finish/7,0)<=40 THEN 7
      WHEN ROUND(order_finish/7,0)>40 AND ROUND(order_finish/7,0)<=60 THEN 8
      WHEN ROUND(order_finish/7,0)>60 AND ROUND(order_finish/7,0)<=100 THEN 9
      WHEN ROUND(order_finish/7,0)>100 THEN 10
      end)
    when on_day<=7 and on_day>0 then 
      round(((case when ROUND(order_finish/on_day,0)<=5 then 4
      when ROUND(order_finish/on_day,0)>5 AND ROUND(order_finish/on_day,0)<=10 then 5
      when ROUND(order_finish/on_day,0)>10 AND ROUND(order_finish/on_day,0)<=20 THEN 6
      WHEN ROUND(order_finish/on_day,0)>20 AND ROUND(order_finish/on_day,0)<=40 THEN 7
      WHEN ROUND(order_finish/on_day,0)>40 AND ROUND(order_finish/on_day,0)<=60 THEN 8
      WHEN ROUND(order_finish/on_day,0)>60 AND ROUND(order_finish/on_day,0)<=100 THEN 9
      WHEN ROUND(order_finish/on_day,0)>100 THEN 10
      end)*on_day+6*(7-on_day))/7,0)
    when on_day=0 then 6
  end) as order_num_score,
  (case when on_day>7 then 
    (case when cancel_rate=0 then 10
      when cancel_rate>0 and cancel_rate<=0.01 then 9
      when cancel_rate>0.01 and cancel_rate<=0.05 then 8
      when cancel_rate>0.05 and cancel_rate<=0.1 then 6
      when cancel_rate>0.1 and cancel_rate<=0.2 then 4
      when cancel_rate>0.2 and cancel_rate<=0.4 then 3
      when cancel_rate>0.4 and cancel_rate<=0.6 then 2
      when cancel_rate>0.6 then 1 end) 
    when on_day<=7 then 
      round(
       ((case when cancel_rate=0 then 10
      when cancel_rate>0 and cancel_rate<=0.01 then 9
      when cancel_rate>0.01 and cancel_rate<=0.05 then 8
      when cancel_rate>0.05 and cancel_rate<=0.1 then 6
      when cancel_rate>0.1 and cancel_rate<=0.2 then 4
      when cancel_rate>0.2 and cancel_rate<=0.4 then 3
      when cancel_rate>0.4 and cancel_rate<=0.6 then 2
      when cancel_rate>0.6 then 1 end)*on_day+9*(7-on_day))/7,0)
   end) as cancel_rate_score,
(CASE WHEN miss_img_p<=0.1 THEN 10
  WHEN miss_img_p>0.1 AND miss_img_p<=0.2 THEN 8
  WHEN miss_img_p>0.2 AND miss_img_p<=0.3 THEN 6
  WHEN miss_img_p>0.3 AND miss_img_p<=0.4 THEN 4
  WHEN miss_img_p>0.4 AND miss_img_p<=0.6 THEN 2
  WHEN miss_img_p>0.6 THEN 1
  END) AS miss_img_score,
(case when qualification_type_n=2 then 10
  when qualification_type_n=1 then 5
  when qualification_type_n=0 then 1
end) as shop_info_score,
(CASE WHEN black_14day=1 THEN 0.5
 WHEN black_14day=0 AND black_30day=1 THEN 0.7
 WHEN black_14day=0 AND black_30day=0 THEN 1 END)*10 AS black_factor,
(CASE 
  WHEN TIMESTAMPDIFF(DAY,first_on_dt,curdate())<=2 THEN 1
  WHEN TIMESTAMPDIFF(DAY,first_on_dt,curdate())>2 
    AND TIMESTAMPDIFF(DAY,first_on_dt,curdate())<=6 THEN 0.8
  WHEN TIMESTAMPDIFF(DAY,first_on_dt,curdate())>6 THEN 0.7 
  END)*10 AS newshop_factor,
(CASE WHEN transporter_delivery_flag=1 THEN 1 ELSE 0.8 END)*10 AS dada_send_factor
from dw_tmp.zuo_cshop_sort_3_5
;

/*
#--160106调整新用户系数影响天数
(CASE 
  WHEN TIMESTAMPDIFF(DAY,first_on_dt,cal_dt)<=3 THEN 1
  WHEN TIMESTAMPDIFF(DAY,first_on_dt,cal_dt)>3 
    AND TIMESTAMPDIFF(DAY,first_on_dt,cal_dt)<=7 THEN 0.8
  WHEN TIMESTAMPDIFF(DAY,first_on_dt,cal_dt)>7 THEN 0.7 
  END)*10 AS newshop_factor
改成
(CASE 
  WHEN TIMESTAMPDIFF(DAY,first_on_dt,curdate())<=2 THEN 1
  WHEN TIMESTAMPDIFF(DAY,first_on_dt,curdate())>2 
    AND TIMESTAMPDIFF(DAY,first_on_dt,curdate())<=6 THEN 0.8
  WHEN TIMESTAMPDIFF(DAY,first_on_dt,curdate())>6 THEN 0.7 
  END)*10 AS newshop_factor
*/
create index zuo_cshop_sort_3_6sid on dw_tmp.zuo_cshop_sort_3_6(supplier_id);

ALTER TABLE dw_tmp.zuo_cshop_sort_3_6 MODIFY order_num_score INT(10);
ALTER TABLE dw_tmp.zuo_cshop_sort_3_6 MODIFY comment_score INT(10);
ALTER TABLE dw_tmp.zuo_cshop_sort_3_6 MODIFY cancel_rate_score INT(10);
ALTER TABLE dw_tmp.zuo_cshop_sort_3_6 MODIFY miss_img_score INT(10);
-- ALTER TABLE dw_tmp.zuo_cshop_sort_3_6 MODIFY shop_info_score INT(10);
-- ALTER TABLE dw_tmp.zuo_cshop_sort_3_6 MODIFY shop_zuobi_score INT(10);
ALTER TABLE dw_tmp.zuo_cshop_sort_3_6 MODIFY black_factor INT(10);
ALTER TABLE dw_tmp.zuo_cshop_sort_3_6 MODIFY newshop_factor INT(10);
ALTER TABLE dw_tmp.zuo_cshop_sort_3_6 MODIFY dada_send_factor INT(10);


#订单量平均每天不超过3单的商户，取15%使其订单量评分增幅，从而给机会排到前面
DROP TABLE IF EXISTS dw_tmp.zuo_cshop_sort_3_6_1;
CREATE TABLE dw_tmp.zuo_cshop_sort_3_6_1
select city_id,round(count(supplier_id)*0.15,0) as shop_num_jump
from dw_tmp.zuo_cshop_sort_3_6
where TIMESTAMPDIFF(day,first_on_dt,cal_dt)>7
and order_num_score<=4
group by city_id
;

DROP TABLE IF EXISTS dw_tmp.zuo_cshop_sort_3_6_2;
CREATE TABLE dw_tmp.zuo_cshop_sort_3_6_2
select supplier_id,city_id,
  RAND(order_total*goods_n) as order_i
from dw_tmp.zuo_cshop_sort_3_6
where TIMESTAMPDIFF(day,first_on_dt,cal_dt)>7
and order_num_score<=4
;

DROP TABLE IF EXISTS dw_tmp.zuo_cshop_sort_3_6_3;
CREATE TABLE dw_tmp.zuo_cshop_sort_3_6_3
select a.*,
  (case when @cid=city_id then @i:=@i+1 else @i:=1 end) as ii,
  @cid:=city_id as cid
from dw_tmp.zuo_cshop_sort_3_6_2 a,(select @cid:=null,@i:=1) b
order by city_id,order_i
;

DROP TABLE IF EXISTS dw_tmp.zuo_cshop_sort_3_6_4;
CREATE TABLE dw_tmp.zuo_cshop_sort_3_6_4
select a.*,b.shop_num_jump
from dw_tmp.zuo_cshop_sort_3_6_3 a join dw_tmp.zuo_cshop_sort_3_6_1 b
on a.city_id=b.city_id
where a.ii<=b.shop_num_jump
;

create index zuo_cshop_sort_3_6_4 on dw_tmp.zuo_cshop_sort_3_6_4(supplier_id);

#给订单量低的商户提高订单分,给机会排到前面
update dw_tmp.zuo_cshop_sort_3_6 a join dw_tmp.zuo_cshop_sort_3_6_4 b on a.supplier_id=b.supplier_id
set a.order_num_score=round(a.order_num_score+(10-a.order_num_score)*0.3,0)
;

delete from dw_api.api_plq_shop_sort where cal_dt=date_sub(curdate(),interval 1 day);

insert into dw_api.api_plq_shop_sort
  (cal_dt,supplier_id,city_id,order_num_score,comment_score,cancel_rate_score,
    miss_img_score,shop_info_score,shop_zuobi_score,black_factor,newshop_factor,dada_send_factor)
SELECT cal_dt,supplier_id,city_id,
order_num_score,comment_score,cancel_rate_score,
miss_img_score,shop_info_score,0 AS shop_zuobi_score,
black_factor,newshop_factor,dada_send_factor
FROM dw_tmp.zuo_cshop_sort_3_6
where cal_dt=date_sub(curdate(),interval 1 day)
;


#/*SELECT *,
#black_factor*newshop_factor*dada_send_factor*
#(order_num_score*0.25+cancel_rate_score*0.15+miss_img_score*0.15+comment_score*0.15+distance_score*0.3)
#AS t_score
#FROM dw_tmp.zuo_cshop_sort_3_6
#;
#
#DROP TABLE IF EXISTS dw_sum_1.api_plq_shop_sort;
#CREATE TABLE dw_sum_1.api_plq_shop_sort
#SELECT cal_dt,supplier_id,city_id,
#order_num_score,comment_score,cancel_rate_score,
#miss_img_score,'' AS shop_info_score,'' AS shop_zuobi_score,
#black_factor,newshop_factor,dada_send_factor
#FROM dw_tmp.zuo_cshop_sort_3_6
#;
#*/

#/*
#
#得分 = 拉黑系数 * 新店系数 * 100%达达配送系数 * 品类系数 * 
#(销量得分*权重1 +距离得分*权重2+评价得分*权重3+取消率得分*权重4+商品照片评分*权重5)
#
#证件齐全，有照片商品比例
#
#*/
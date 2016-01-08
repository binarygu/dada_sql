#! /usr/bin/env python
# -*- coding: utf-8 -*-

#################################
# @author: qiangwei@imdada.cn
# @desc: 每日订单基础
# @since: 2015-03-04
# @comment: 刷新30天内数据
# @target table: dm_tsp_order_base_day
# @source tables: dw_tsp_order
#                 dw_plq_ord_order_head
#                 dw_tsp_order_distance
#                 dw_tsp_order_bad_receiver
#                 dw_tsp_violate_order
#                 dw_tsp_area_position_check_config
#                 dim_cal_dt
# update by qiangwei@imdada.cn 2015-03-05 增加补贴订单数
# update by qiangwei@imdada.cn 2015-03-28 增加cancel_order_cnt字段
# update by qiangwei@imdada.cn 2015-04-30 增加order_type_id字段
# update by qiangwei@imdada.cn 2015-05-12 增加online_pay_order_cnt与online_pay_amt字段
# update by qiangwei@imdada.cn 2015-05-18 完成订单口径切换
# update by qiangwei@imdada.cn 2015-05-27 增加月结运费订单数
# update by qiangwei@imdada.cn 2015-06-08 限制商品金额不超过五万，并增加追单订单数、接单秒数、取单描述、完成秒数
# update by qiangwei@imdada.cn 2015-06-10 增加小距离订单数、坏收货人订单数、运费收入、运费支出
# update by qiangwei@imdada.cn 2015-06-15 对秒数最小值增加限制
# update by qiangwei@imdada.cn 2015-09-29 增加到店位置异常订单数与完成位置异常订单数
# update by qiangwei@imdada.cn 2015-10-20 到店异常订单数逻辑调整，并增加60分钟完成订单数等3个指标 
# update by qiangwei@imdada.cn 2015-11-10 优化临时表表结构以及缓存参数
# update by qiangwei@imdada.cn 2015-11-18 将程序Python化
# update by qiangwei@imdada.cn 2015-11-27 优化程序逻辑
# update by zhangjunqi@imdada.cn 2015-12-07 修改字段名is_from_plq为order_plateform_type_id、增加字段fifteen_min_arrive_order_cnt 
# update by zhangjunqi@imdada.cn 2015-12-18 增加字arrive_time_finish_order_cnt、short_distance_order_cnt、long_distance_order_cnt
# update by qiangwei@imdada.cn 2015-12-24 对临时表进行优化
##################################

import os
import sys
import datetime
import re
import multiprocessing
import mysql.connector

def run_mysql(data_dt):
    # 准备SQL
    sql_str = """\
-- 创建订单临时表
drop table if exists temp.temp_dm_tsp_order_base_day_1_order_%(schedule_timestamp)s_%(dt)s;
create temporary table temp.temp_dm_tsp_order_base_day_1_order_%(schedule_timestamp)s_%(dt)s (
    order_id bigint NOT NULL DEFAULT 0,
    order_type_id int NOT NULL DEFAULT 0,
    order_status int NOT NULL DEFAULT 0,
    is_finished tinyint NOT NULL DEFAULT 0,
    is_monthly_settle tinyint NOT NULL DEFAULT 0,
    supplier_type_id int NOT NULL DEFAULT 0,
    delivery_range_id int NOT NULL DEFAULT 0,
    supplier_id int NOT NULL DEFAULT 0,
    transporter_id int NOT NULL DEFAULT 0,
    transporter_type_id int NOT NULL DEFAULT 0,
    cargo_type_id int NOT NULL DEFAULT 0,
	order_plateform_type_id tinyint NOT NULL DEFAULT 0,
	receiver_id bigint NOT NULL DEFAULT 0,
    is_short_move tinyint NOT NULL DEFAULT 0,
    cargo_amt decimal(20,2) NOT NULL DEFAULT 0,
    tips_amt decimal(10,2) NOT NULL DEFAULT 0,
    allowance_amt decimal(10,2) NOT NULL DEFAULT 0,
    deliver_fee_amt decimal(10,2) NOT NULL DEFAULT 0,
    revenue_deliver_fee_amt decimal(10,2) NOT NULL DEFAULT 0,
    expense_deliver_fee_amt decimal(10,2) NOT NULL DEFAULT 0,
    online_pay_amt decimal(10,2) NOT NULL DEFAULT 0,
    block_id int NOT NULL DEFAULT 0,
    area_id int NOT NULL DEFAULT 0,
    city_id int NOT NULL DEFAULT 0,
    appoint_status int NOT NULL DEFAULT 0,
    accept_time datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
    arrive_time datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
    fetch_time datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
    finish_time datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
    show_time datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
    finish_dt date NOT NULL DEFAULT '0000-00-00',
    create_dt date NOT NULL DEFAULT '0000-00-00',
    show_dt date NOT NULL DEFAULT '0000-00-00',
    PRIMARY KEY (order_id),
    KEY k_create_dt (create_dt),
    KEY k_is_finished_finish_dt (is_finished, finish_dt)
) ENGINE=MEMORY
;

replace into temp.temp_dm_tsp_order_base_day_1_order_%(schedule_timestamp)s_%(dt)s
select
    a.order_id,
    a.order_type_id,
    a.order_status,
    a.is_finished,
    a.is_monthly_settle,
    a.supplier_type_id,
    a.delivery_range_id,
    a.supplier_id,
    a.transporter_id,
    a.transporter_type_id,
    a.cargo_type_id,
    a.order_plateform_type_id,
	a.receiver_id,
    coalesce(c.is_short_move, 0) as is_short_move,
    a.cargo_amt,
    a.tips_amt,
    a.allowance_amt,
    a.deliver_fee_amt,
    a.revenue_deliver_fee_amt,
    a.expense_deliver_fee_amt,
    a.online_pay_amt,
    a.block_id,
    a.area_id,
    a.city_id,
    a.appoint_status,
    a.accept_time,
    a.arrive_time,
    a.fetch_time,
    a.finish_time,
    a.show_time,
    a.finish_dt,
    a.create_dt,
    a.show_dt
from
    dw.dw_tsp_order a
left outer join
    dw.dw_tsp_order_distance c
on
    a.order_id = c.order_id
where
    a.create_dt = '%(data_dt)s'
    or a.finish_dt = '%(data_dt)s'
;

-- 创建数据临时表
drop table if exists temp.temp_dm_tsp_order_base_day_2_data_%(schedule_timestamp)s_%(dt)s;
create temporary table temp.temp_dm_tsp_order_base_day_2_data_%(schedule_timestamp)s_%(dt)s (
    step tinyint NOT NULL DEFAULT 0,
    city_id int NOT NULL DEFAULT 0,
    area_id int NOT NULL DEFAULT 0,
    block_id int NOT NULL DEFAULT 0,
	order_plateform_type_id int NOT NULL DEFAULT 0,
    supplier_id int NOT NULL DEFAULT 0,
    supplier_type_id int NOT NULL DEFAULT 0,
    delivery_range_id int NOT NULL DEFAULT 0,
    cargo_type_id int NOT NULL DEFAULT 0,
    transporter_id int NOT NULL DEFAULT 0,
    transporter_type_id int NOT NULL DEFAULT 0,
    order_type_id int NOT NULL DEFAULT 0,
    publish_order_cnt bigint NOT NULL DEFAULT 0,
    cancel_order_cnt bigint NOT NULL DEFAULT 0,
    finish_order_cnt bigint NOT NULL DEFAULT 0,
    allowance_order_cnt bigint NOT NULL DEFAULT 0,
    tips_order_cnt bigint NOT NULL DEFAULT 0,
    online_pay_order_cnt bigint NOT NULL DEFAULT 0,
    monthly_settle_order_cnt bigint NOT NULL DEFAULT 0,
    appoint_order_cnt bigint NOT NULL DEFAULT 0,
    short_move_order_cnt bigint NOT NULL DEFAULT 0,
    bad_receiver_order_cnt bigint NOT NULL DEFAULT 0,
    order_amt numeric(20,2) NOT NULL DEFAULT 0,
    deliver_fee_amt numeric(20,2) NOT NULL DEFAULT 0,
    revenue_deliver_fee_amt numeric(20,2) NOT NULL DEFAULT 0,
    expense_deliver_fee_amt numeric(20,2) NOT NULL DEFAULT 0,
    allowance_amt numeric(20,2) NOT NULL DEFAULT 0,
    tips_amt numeric(20,2) NOT NULL DEFAULT 0,
    online_pay_amt numeric(20,2) NOT NULL DEFAULT 0,
    accept_seconds bigint NOT NULL DEFAULT 0,
    fetch_seconds bigint NOT NULL DEFAULT 0,
    finish_seconds bigint NOT NULL DEFAULT 0,
	arrive_abnormal_order_cnt bigint NOT NULL DEFAULT 0,
	finish_abnormal_order_cnt bigint NOT NULL DEFAULT 0,
    sixty_min_finish_order_cnt bigint NOT NULL DEFAULT 0,
    ninety_min_finish_order_cnt bigint NOT NULL DEFAULT 0,
    area_check_finish_order_cnt bigint NOT NULL DEFAULT 0,
	fifteen_min_arrive_order_cnt bigint NOT NULL DEFAULT 0,
	arrive_time_finish_order_cnt bigint NOT NULL DEFAULT 0,
	short_distance_order_cnt bigint NOT NULL DEFAULT 0,
	long_distance_order_cnt bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (
        step,
        city_id,
        area_id,
        block_id,
		order_plateform_type_id,
        supplier_id,
        supplier_type_id,
        delivery_range_id,
        cargo_type_id,
        transporter_id,
        transporter_type_id,
        order_type_id
    )
) ENGINE=MEMORY
;

-- 计算发布口径指标数据(step 1)
insert into temp.temp_dm_tsp_order_base_day_2_data_%(schedule_timestamp)s_%(dt)s
select
    1 as step,
    a.city_id,
    a.area_id,
    a.block_id,
	a.order_plateform_type_id,	
    a.supplier_id,
    a.supplier_type_id,
    a.delivery_range_id,
    a.cargo_type_id,
    a.transporter_id,
    a.transporter_type_id,
    a.order_type_id,
    count(*) as publish_order_cnt,
    count(case when a.order_status = 5 then 1 else null end) as cancel_order_cnt,
    0 as finish_order_cnt,
    0 as allowance_order_cnt,
    0 as tips_order_cnt,
    0 as online_pay_order_cnt,
    0 as monthly_settle_order_cnt,
    0 as appoint_order_cnt,
    0 as short_move_order_cnt,
    0 as bad_receiver_order_cnt,
    0.0 as order_amt,
    0.0 as deliver_fee_amt,
    0.0 as revenue_deliver_fee_amt,
    0.0 as expense_deliver_fee_amt,
    0.0 as allowance_amt,
    0.0 as tips_amt,
    0.0 as online_pay_amt,
    0 as accept_seconds,
    0 as fetch_seconds,
    0 as finish_seconds,
	0 as arrive_abnormal_order_cnt,
	0 as finish_abnormal_order_cnt,
    0 as sixty_min_finish_order_cnt,
    0 as ninety_min_finish_order_cnt,
    0 as area_check_finish_order_cnt,
	0 as fifteen_min_arrive_order_cnt,
	0 as arrive_time_finish_order_cnt,
	0 as short_distance_order_cnt,
	0 as long_distance_order_cnt
from
    temp.temp_dm_tsp_order_base_day_1_order_%(schedule_timestamp)s_%(dt)s a
where
    a.order_status > 0
    and a.create_dt = '%(data_dt)s'
group by
    2, 3, 4, 5, 6, 7, 8, 9, 10, 11,12
;

-- 计算完成口径指标数据(step 2)
insert into temp.temp_dm_tsp_order_base_day_2_data_%(schedule_timestamp)s_%(dt)s
select
    2 as step,
    a.city_id,
    a.area_id,
    a.block_id,
    a.order_plateform_type_id,	
    a.supplier_id,
    a.supplier_type_id,
    a.delivery_range_id,
    a.cargo_type_id,
    a.transporter_id,
    a.transporter_type_id,
    a.order_type_id,
    0 as publish_order_cnt,
    0 as cancel_order_cnt,
    count(*) as finish_order_cnt,
    count(case when a.allowance_amt > 0 then 1 else null end) as allowance_order_cnt,
    count(case when a.tips_amt > 0 then 1 else null end) as tips_order_cnt,
    count(case when a.online_pay_amt > 0 then 1 else null end) as online_pay_order_cnt,
    count(case when a.is_monthly_settle = 1 then 1 else null end) as monthly_settle_order_cnt,
    count(case when a.appoint_status = 5 then 1 else null end) as appoint_order_cnt,
    count(case when a.is_short_move = 1 then 1 else null end) as short_move_order_cnt,
    0 as bad_receiver_order_cnt,
    sum(case when a.cargo_amt <= 50000 then a.cargo_amt else 0 end) as order_amt,
    sum(a.deliver_fee_amt) as deliver_fee_amt,
    sum(a.revenue_deliver_fee_amt) as revenue_deliver_fee_amt,
    sum(a.expense_deliver_fee_amt) as expense_deliver_fee_amt,
    sum(a.allowance_amt) as allowance_amt,
    sum(a.tips_amt) as tips_amt,
    sum(a.online_pay_amt) as online_pay_amt,
    sum(case when a.order_type_id = 1 then greatest(least(unix_timestamp(a.accept_time) - unix_timestamp(a.show_time), 7200), 0) else 0 end) as accept_seconds,
    sum(case when a.order_type_id = 1 then greatest(least(unix_timestamp(a.fetch_time) - unix_timestamp(a.accept_time), 7200), 0) else 0 end) as fetch_seconds,
    sum(case when a.order_type_id = 1 then greatest(least(unix_timestamp(a.finish_time) - unix_timestamp(a.accept_time), 7200), 0) else 0 end) as finish_seconds,
	0 as arrive_abnormal_order_cnt,
	0 as finish_abnormal_order_cnt,
    count(case when a.order_type_id = 1 and unix_timestamp(a.finish_time) - unix_timestamp(a.accept_time) <= 3600 then 1 else null end) as sixty_min_finish_order_cnt,
    count(case when a.order_type_id = 1 and unix_timestamp(a.finish_time) - unix_timestamp(a.accept_time) <= 5400 then 1 else null end) as ninety_min_finish_order_cnt,
    0 as area_check_finish_order_cnt,
	count(case when a.arrive_time > '0000-00-00 00:00:00'
                    and unix_timestamp(a.arrive_time) - unix_timestamp(a.accept_time) <= 900
               then 1
               else null end)  as fifteen_min_arrive_order_cnt,
	count(case when a.arrive_time > '0000-00-00 00:00:00' then 1 else null end) as arrive_time_finish_order_cnt,
	count(case when c.receiver_id is not null and c.supplier_to_receiver_distance > 0 and c.supplier_to_receiver_distance < 1000 then 1 else null end) as short_distance_order_cnt,
	count(case when c.receiver_id is not null and c.supplier_to_receiver_distance > 3000 then 1 else null end) as long_distance_order_cnt		   
from
    temp.temp_dm_tsp_order_base_day_1_order_%(schedule_timestamp)s_%(dt)s a
left join
    dw.dw_usr_receiver c
on
    a.receiver_id = c.receiver_id	
where
    a.is_finished = 1
    and a.finish_dt = '%(data_dt)s'
group by
    2, 3, 4, 5, 6, 7, 8, 9, 10, 11,12
;

-- 计算坏收货人数据(step 3)
insert into temp.temp_dm_tsp_order_base_day_2_data_%(schedule_timestamp)s_%(dt)s
select
    3 as step,
    a.city_id,
    a.area_id,
    a.block_id,
    a.order_plateform_type_id,	
    a.supplier_id,
    a.supplier_type_id,
    a.delivery_range_id,
    a.cargo_type_id,
    a.transporter_id,
    a.transporter_type_id,
    a.order_type_id,
    0 as publish_order_cnt,
    0 as cancel_order_cnt,
    0 as finish_order_cnt,
    0 as allowance_order_cnt,
    0 as tips_order_cnt,
    0 as online_pay_order_cnt,
    0 as monthly_settle_order_cnt,
    0 as appoint_order_cnt,
    0 as short_move_order_cnt,
    count(*) as bad_receiver_order_cnt,
    0.0 as order_amt,
    0.0 as deliver_fee_amt,
    0.0 as revenue_deliver_fee_amt,
    0.0 as expense_deliver_fee_amt,
    0.0 as allowance_amt,
    0.0 as tips_amt,
    0.0 as online_pay_amt,
    0 as accept_seconds,
    0 as fetch_seconds,
    0 as finish_seconds,
	0 as arrive_abnormal_order_cnt,
	0 as finish_abnormal_order_cnt,
    0 as sixty_min_finish_order_cnt,
    0 as ninety_min_finish_order_cnt,
    0 as area_check_finish_order_cnt,
	0 as fifteen_min_arrive_order_cnt,
	0 as arrive_time_finish_order_cnt,
	0 as short_distance_order_cnt,
	0 as long_distance_order_cnt
from
    temp.temp_dm_tsp_order_base_day_1_order_%(schedule_timestamp)s_%(dt)s a
join
    dw.dw_tsp_order_bad_receiver b
on
    a.order_id = b.order_id
where
    a.finish_dt = '%(data_dt)s'
group by
    2, 3, 4, 5, 6, 7, 8, 9, 10, 11,12
;

-- 计算位置异常数据(step 4)
insert into temp.temp_dm_tsp_order_base_day_2_data_%(schedule_timestamp)s_%(dt)s
select
    4 as step,
    a.city_id,
    a.area_id,
    a.block_id,
	a.order_plateform_type_id,	
    a.supplier_id,
    a.supplier_type_id,
    a.delivery_range_id,
    a.cargo_type_id,
    a.transporter_id,
    a.transporter_type_id,
    a.order_type_id,
    0 as publish_order_cnt,
    0 as cancel_order_cnt,
    0 as finish_order_cnt,
    0 as allowance_order_cnt,
    0 as tips_order_cnt,
    0 as online_pay_order_cnt,
    0 as monthly_settle_order_cnt,
    0 as appoint_order_cnt,
    0 as short_move_order_cnt,
    0 as bad_receiver_order_cnt,
    0.0 as order_amt,
    0.0 as deliver_fee_amt,
    0.0 as revenue_deliver_fee_amt,
    0.0 as expense_deliver_fee_amt,
    0.0 as allowance_amt,
    0.0 as tips_amt,
    0.0 as online_pay_amt,
    0 as accept_seconds,
    0 as fetch_seconds,
    0 as finish_seconds,
	count(distinct case when b.violate_type_id = 5 and a.arrive_time <> '0000-00-00 00:00:00' then a.order_id else null end) as arrive_abnormal_order_cnt,
	count(distinct case when b.violate_type_id = 2 then a.order_id else null end) as finish_abnormal_order_cnt,
    0 as sixty_min_finish_order_cnt,
    0 as ninety_min_finish_order_cnt,
    0 as area_check_finish_order_cnt,
	0 as fifteen_min_arrive_order_cnt,
	0 as arrive_time_finish_order_cnt,
	0 as short_distance_order_cnt,
	0 as long_distance_order_cnt
from
    temp.temp_dm_tsp_order_base_day_1_order_%(schedule_timestamp)s_%(dt)s a
join
    dw.dw_tsp_violate_order b
on
    a.order_id = b.order_id
where
    a.is_finished = 1
    and a.finish_dt = '%(data_dt)s'
    and b.violate_type_id in (2, 5)
group by
    2, 3, 4, 5, 6, 7, 8, 9, 10, 11,12
;

-- 获取区域配置参数
drop table if exists temp.temp_dm_tsp_order_base_day_2_area_%(schedule_timestamp)s_%(dt)s;
create temporary table temp.temp_dm_tsp_order_base_day_2_area_%(schedule_timestamp)s_%(dt)s (
    city_id int NOT NULL DEFAULT 0,
    area_id int NOT NULL DEFAULT 0,
    PRIMARY KEY (city_id, area_id)
) ENGINE=MEMORY
;

insert into temp.temp_dm_tsp_order_base_day_2_area_%(schedule_timestamp)s_%(dt)s
select distinct
    city_id,
    area_id
from
    dw.dw_tsp_area_position_check_config
;

-- 计算区域检测数据(step 5)
insert into temp.temp_dm_tsp_order_base_day_2_data_%(schedule_timestamp)s_%(dt)s
select
    5 as step,
    a.city_id,
    a.area_id,
    a.block_id,
	a.order_plateform_type_id,	
    a.supplier_id,
    a.supplier_type_id,
    a.delivery_range_id,
    a.cargo_type_id,
    a.transporter_id,
    a.transporter_type_id,
    a.order_type_id,
    0 as publish_order_cnt,
    0 as cancel_order_cnt,
    0 as finish_order_cnt,
    0 as allowance_order_cnt,
    0 as tips_order_cnt,
    0 as online_pay_order_cnt,
    0 as monthly_settle_order_cnt,
    0 as appoint_order_cnt,
    0 as short_move_order_cnt,
    0 as bad_receiver_order_cnt,
    0.0 as order_amt,
    0.0 as deliver_fee_amt,
    0.0 as revenue_deliver_fee_amt,
    0.0 as expense_deliver_fee_amt,
    0.0 as allowance_amt,
    0.0 as tips_amt,
    0.0 as online_pay_amt,
    0 as accept_seconds,
    0 as fetch_seconds,
    0 as finish_seconds,
	0 as arrive_abnormal_order_cnt,
	0 as finish_abnormal_order_cnt,
    0 as sixty_min_finish_order_cnt,
    0 as ninety_min_finish_order_cnt,
    count(*) as area_check_finish_order_cnt,
	0 as fifteen_min_arrive_order_cnt,
	0 as arrive_time_finish_order_cnt,
	0 as short_distance_order_cnt,
	0 as long_distance_order_cnt
from
    temp.temp_dm_tsp_order_base_day_1_order_%(schedule_timestamp)s_%(dt)s a
join
    temp.temp_dm_tsp_order_base_day_2_area_%(schedule_timestamp)s_%(dt)s b
on
    a.city_id = b.city_id
    and a.area_id = b.area_id
where
    a.is_finished = 1
    and a.finish_dt = '%(data_dt)s'
group by
    2, 3, 4, 5, 6, 7, 8, 9, 10, 11,12
;

-- 汇总数据，插入目标表
alter table dm.dm_tsp_order_base_day truncate partition p%(dt)s;
insert into dm.dm_tsp_order_base_day
select
    '%(data_dt)s' as cal_dt,
    city_id,
    area_id,
    block_id,
	order_plateform_type_id,	
    supplier_id,
    supplier_type_id,
    delivery_range_id,
    cargo_type_id,
    transporter_id,
    transporter_type_id,
    order_type_id,
    sum(publish_order_cnt) as publish_order_cnt,
    sum(cancel_order_cnt) as cancel_order_cnt,
    sum(finish_order_cnt) as finish_order_cnt,
    sum(allowance_order_cnt) as allowance_order_cnt,
    sum(tips_order_cnt) as tips_order_cnt,
    sum(online_pay_order_cnt) as online_pay_order_cnt,
    sum(monthly_settle_order_cnt) as monthly_settle_order_cnt,
    sum(appoint_order_cnt) as appoint_order_cnt,
    sum(short_move_order_cnt) as short_move_order_cnt,
    sum(bad_receiver_order_cnt) as bad_receiver_order_cnt,
    sum(order_amt) as order_amt,
    sum(deliver_fee_amt) as deliver_fee_amt,
    sum(revenue_deliver_fee_amt) as revenue_deliver_fee_amt,
    sum(expense_deliver_fee_amt) as expense_deliver_fee_amt,
    sum(allowance_amt) as allowance_amt,
    sum(tips_amt) as tips_amt,
    sum(online_pay_amt) as online_pay_amt,
    sum(accept_seconds) as accept_seconds,
    sum(fetch_seconds) as fetch_seconds,
    sum(finish_seconds) as finish_seconds,
	sum(arrive_abnormal_order_cnt) as arrive_abnormal_order_cnt,
	sum(finish_abnormal_order_cnt) as finish_abnormal_order_cnt,
    sum(sixty_min_finish_order_cnt) as sixty_min_finish_order_cnt,
    sum(ninety_min_finish_order_cnt) as ninety_min_finish_order_cnt,
    sum(area_check_finish_order_cnt) as area_check_finish_order_cnt,
	sum(fifteen_min_arrive_order_cnt) as fifteen_min_arrive_order_cnt,
    sum(arrive_time_finish_order_cnt) as arrive_time_finish_order_cnt,
	sum(short_distance_order_cnt) as short_distance_order_cnt,
	sum(long_distance_order_cnt) as long_distance_order_cnt
from
    temp.temp_dm_tsp_order_base_day_2_data_%(schedule_timestamp)s_%(dt)s
group by
    2, 3, 4, 5, 6, 7, 8, 9, 10, 11,12
;
""" % {"schedule_timestamp": schedule_timestamp, "data_dt": data_dt, "dt": data_dt.replace("-", "")}

    # 执行SQL
    db_dst = mysql.connector.connect(
        user = etl_common.connection_dwetl['user'],
        password = etl_common.connection_dwetl['password'],
        database = "dm",
        host = etl_common.connection_dwetl['host']
)
    cursor_dst=db_dst.cursor()
    cursor_dst.execute("SET SESSION lock_wait_timeout=1800;")
    cursor_dst.execute("SET SESSION read_rnd_buffer_size=536870912;")
    cursor_dst.execute("SET SESSION sort_buffer_size=536870912;")
    for sql_item in sql_str.split(";\n"):
        if (len(sql_item.strip().lstrip()) > 0):
            cursor_dst.execute(sql_item)
    print "DATA @ %s IS DONE" % data_dt
    cursor_dst.close()
    db_dst.close()
    
    # 返回执行状态
    return 0
    
if __name__ == '__main__':
    # 参数变量
    execfile("%s/../etl_common.py" % os.path.dirname(os.path.realpath(__file__)))
    schedule_timestamp = int(sys.argv[1])
    data_update_offset = 29
    process_pool_size = 4
    last_date = datetime.date.fromtimestamp(int(schedule_timestamp) - 86400)
    current_date = last_date - datetime.timedelta(data_update_offset)
    data_dt_list = []
    while (current_date <= last_date):
        data_dt_list.append(current_date.strftime("%Y-%m-%d"))
        current_date +=  datetime.timedelta(1)

    # 多线程处理数据
    pool = multiprocessing.Pool(processes=process_pool_size)
    pool.map(run_mysql, data_dt_list)

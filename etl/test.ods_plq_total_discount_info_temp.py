#!/usr/bin/python
# -*- coding: UTF-8 -*-

#解析活动脚本
#@jacky
#时间：2016/1/4

import sys
import os
import datetime
import MySQLdb
reload(sys)
sys.setdefaultencoding("utf-8")

class CON_MYSQL(object):
	def __init__(self,myhost,myuser,mypasswd):
		self.conn=MySQLdb.connect(host=myhost,
								user=myuser,
								passwd=mypasswd,
								port=3306,
								charset=utf8)
		self.cur=self.conn.cursor()
    #从mysql数据库中选择训练数据
	def selectFromMysql(self,sql):
		result = []
		try:
			self.cur.execute(sql)
			all_data = self.cur.fetchall()
			for i in all_data:
				result.append(i)
		except MySQLdb.Error, e:
			print "Mysql Error %d: %s" % (e.args[0], e.args[1])	
		return result
		
    #插入数据
	def insertToMysql(self,sql):
		try:
			self.cur.execute(sql)
			self.conn.commit()
		except MySQLdb.Error, e:
			print "Mysql Error %d: %s" % (e.args[0], e.args[1])
    #删除数据
	def deleteFromMysql(self,sql):
		try:
			self.cur.execute(sql)
		except MySQLdb.Error, e:
			print "Mysql Error %d: %s" % (e.args[0], e.args[1])
    #关闭Mysql连接
	def closeMysql(self):
		try:
			self.cur.close()
			self.conn.close()
		except MySQLdb.Error, e:
			print "Mysql Error %d: %s" % (e.args[0], e.args[1])


if __name__ == '__main__':

	if len(sys.argv) < 3 :
		end_dt = datetime.date.today() - datetime.timedelta(days=1)
	else:
		end_dt = sys.argv[2]

	if len(sys.argv) < 2:
		start_dt = datetime.date.today() - datetime.timedelta(days=1)
	else:
		start_dt = sys.argv[1]
	

	sql = '''select 
				* 
			from 
				ods.ods_plq_total_discount_info 
			where 
				date(update_time) between '{0}' and '{1}'
		'''.format(start_dt,end_dt)

	execfile("%s/../etl_common.py" % os.path.dirname(os.path.realpath(__file__)))
	connect_mysql = CON_MYSQL(etl_common.connection_dwetl['host'],etl_common.connection_dwetl['user'],etl_common.connection_dwetl['password'])

	train_data = connect_mysql.selectFromMysql(sql)
	connect_mysql.deleteFromMysql('''delete from test.ods_plq_total_discount_info where date(update_time) between '{0}' and '{1}' '''.format(start_dt,end_dt))

	for every_data in train_data:
		city_list = eval(every_data[5])
		supplier_content = eval(every_data[6])
		content_type = supplier_content['content_type']
		value_list = supplier_content['value']
		for city_id in city_list:
			for value in value_list:
				insert_sql = '''insert into test.ods_plq_total_discount_info_temp(
							total_discount_info_id,
							title,
							start_time,
							end_time,
							max_money, 
							city_id,
							content_type_id,
							content_type_value, 
							is_open,
							discount_type,
							create_time, 
							update_time, 
							is_force)
							values ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}',
							'{12}')'''.format(every_data[0],every_data[1],every_data[2],every_data[3],every_data[4],city_id,
							content_type,value,every_data[7],every_data[8],every_data[9],every_data[10],every_data[11])
				connect_mysql.insertToMysql(insert_sql)

	connect_mysql.closeMysql()











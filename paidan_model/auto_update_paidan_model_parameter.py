#!/usr/bin/python
# -*- coding: UTF-8 -*-

##########################################################
###############自动化更新派单模型的参数###################
########插入到dw_test.bian_paidan_model_parameter#########
########如果新模型的评价大于一定的阈值则更新参数##########
#################否则为昨天的参数值#######################
################完成时间：2015-12-22######################
#依赖于训练数据（dw_test.bian_paidan_model_data）是否跑完#
##########################################################

import datetime
import MySQLdb
import pandas as pd 
import numpy as np 
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score
from sklearn.metrics import precision_score
from sklearn.metrics import roc_auc_score 


class CON_MYSQL(object):
	def __init__(self):
		self.conn=MySQLdb.connect(host='10.10.135.131',
								user='analyst',
								passwd='a25d487f2b52698@FORanalyst?',
								port=3306)
		self.cur=self.conn.cursor()
    #从mysql数据库中选择训练数据
	def selectFromMysql(self,sql):
		try:
			self.cur.execute(sql)
			all_data = self.cur.fetchall()
		except MySQLdb.Error, e:
			print "Mysql Error %d: %s" % (e.args[0], e.args[1])	
		result = []
		for i in all_data:
			result.append(i)
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

def data_process(data):
	col = ['supplier_receiver_distance',
			'transporter_supplier_distance',
			'today_finish_order_num',
			'transporter_before_fourteen_day_paidan_lv',
			'paidan_receiver_distance_jiedan_receiver_min',
			'paidan_supplier_distance_jiedan_receiver_min',
			'paidan_hour',
			'transporter_before_seven_day_paidan_lv',
			'paidan_supplier_distance_jiedan_receiver_max',
			'paidan_receiver_distance_jiedan_receiver_max',
			'allowance_amt',
			'label']
	#数据预处理
	paidan_data = pd.DataFrame(data,columns = col)
	paidan_data = paidan_data.fillna(value = 0)
	paidan_data.loc[paidan_data.paidan_receiver_distance_jiedan_receiver_max > 10000,"paidan_receiver_distance_jiedan_receiver_max"] = paidan_data["paidan_receiver_distance_jiedan_receiver_max"][paidan_data.paidan_receiver_distance_jiedan_receiver_max < 10000].mean()
	paidan_data.loc[paidan_data.paidan_receiver_distance_jiedan_receiver_min > 10000,"paidan_receiver_distance_jiedan_receiver_min"] = paidan_data["paidan_receiver_distance_jiedan_receiver_min"][paidan_data.paidan_receiver_distance_jiedan_receiver_min < 10000].mean()
	paidan_data.loc[paidan_data.paidan_supplier_distance_jiedan_receiver_max > 10000,"paidan_supplier_distance_jiedan_receiver_max"] = paidan_data["paidan_supplier_distance_jiedan_receiver_max"][paidan_data.paidan_supplier_distance_jiedan_receiver_max < 10000].mean()
	paidan_data.loc[paidan_data.paidan_supplier_distance_jiedan_receiver_min > 10000,"paidan_supplier_distance_jiedan_receiver_min"] = paidan_data["paidan_supplier_distance_jiedan_receiver_min"][paidan_data.paidan_supplier_distance_jiedan_receiver_min < 10000].mean()
	paidan_data.loc[paidan_data.supplier_receiver_distance > 10000,"supplier_receiver_distance"] = paidan_data["supplier_receiver_distance"][paidan_data.supplier_receiver_distance < 10000].mean()
	paidan_data.loc[paidan_data.transporter_supplier_distance > 10000,"transporter_supplier_distance"] = paidan_data["transporter_supplier_distance"][paidan_data.transporter_supplier_distance < 10000].mean()
	x = paidan_data.ix[:,:-1]
	y = paidan_data.ix[:,-1]
	return x,y

def train_model(x_train,y_train):
	#训练模型
	clf = LogisticRegression(C = 100)
	clf.fit(x_train,y_train)
	return clf

def test_model(clf,x_test,y_test):
	#测试模型
    label_pre = clf.predict(x_test)
    acs = accuracy_score(y_test,label_pre)
    prec = precision_score(y_test,label_pre)
    auc = roc_auc_score(y_test,label_pre)
    return acs,prec,auc

if __name__ == '__main__':
	today = datetime.date.today()
	yesterday = today - datetime.timedelta(days=1) 
	sql_select = '''select 
				supplier_receiver_distance,
				transporter_supplier_distance,
				today_finish_order_num,
				transporter_before_fourteen_day_paidan_lv,
				paidan_receiver_distance_jiedan_receiver_min,
				paidan_supplier_distance_jiedan_receiver_min,
				paidan_hour,
				transporter_before_seven_day_paidan_lv,
				paidan_supplier_distance_jiedan_receiver_max,
				paidan_receiver_distance_jiedan_receiver_max,
				allowance_amt,
				label 
			from 
				dw_test.bian_paidan_model_data
			where 
				create_dt = '{0}'
	        '''.format(yesterday)

	connect_mysql_train = CON_MYSQL()
	train_data = connect_mysql_train.selectFromMysql(sql_select)
	connect_mysql_train.closeMysql()

	x_train, y_train = data_process(train_data) 
	clf = train_model(x_train, y_train)
	acs, prec, auc = test_model(clf, x_train, y_train)

	sql_delete = "delete from dw_test.bian_paidan_model_parameter where create_dt = '{0}'".format(today)
	sql_insert_old = '''insert into dw_test.bian_paidan_model_parameter 
					select 
						'{0}',w0,w1,w2,w3,w4,w5,w6,w7,w8,w9,wx,b
					from
						dw_test.bian_paidan_model_parameter
					where
						create_dt = '{1}'
					'''.format(today, yesterday)
	sql_insert_new = '''insert into dw_test.bian_paidan_model_parameter 
						values ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}',
						'{12}')'''.format(today, clf.coef_[0][0], clf.coef_[0][1],clf.coef_[0][2],clf.coef_[0][3],
						clf.coef_[0][4],clf.coef_[0][5],clf.coef_[0][6],clf.coef_[0][7],clf.coef_[0][8],clf.coef_[0][9],
						clf.coef_[0][10],clf.intercept_[0])

	#判断新模型的准确率，命中率，和acu是否大于阈值
	#如果大于就把新参数插入，否则插入昨天的参数
	connect_mysql_parameter = CON_MYSQL()
	connect_mysql_parameter.deleteFromMysql(sql_delete)
	if acs >= 0.75 and prec >= 0.65 and auc >= 0.68:
		connect_mysql_parameter.insertToMysql(sql_insert_new)
		print 'update new success'
	else:
		connect_mysql_parameter.insertToMysql(sql_insert_old)
		print 'update old success'
	connect_mysql_parameter.closeMysql()





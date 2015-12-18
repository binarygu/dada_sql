#################################
# @负责人: weihuo@imdada.cn
# @描述: 物料达达打分
# @创建日期: 2015-07-14
# @备注: 每天运行，清空已有数据，插入最新数据
# @目标表: dw_api.api_material_transporter_score
# @来源表: dw.dw_usr_transporter
#          dw.dw_tsp_order
#################################


DROP TABLE IF EXISTS dw_tmp.huo_dada_score_01;
CREATE TABLE dw_tmp.huo_dada_score_01
SELECT
transporter_id,
COUNT(*) orders
FROM dw.dw_tsp_order
WHERE delivery_range_id=1 AND is_finished=1
GROUP BY 1
;
CREATE INDEX transporter_id ON dw_tmp.huo_dada_score_01(transporter_id);#45938

 

DROP TABLE IF EXISTS dw_tmp.huo_dada_score_011;
CREATE TABLE dw_tmp.huo_dada_score_011
SELECT 
a.transporter_id,
a.orders score,
b.city_id
FROM dw_tmp.huo_dada_score_01 a
JOIN dw.dw_usr_transporter b ON a.transporter_id=b.transporter_id
WHERE b.transporter_type_id=2
;#45531



TRUNCATE TABLE dw_api.api_material_transporter_score;
INSERT INTO dw_api.api_material_transporter_score
SELECT
transporter_id,
DATE_SUB(CURDATE(),INTERVAL 1 DAY) cal_dt,
city_id,
score
FROM dw_tmp.huo_dada_score_011
;

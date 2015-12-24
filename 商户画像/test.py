import numpy as np 
import pandas as pd
import matplotlib.pyplot as plt
supplier_data = pd.read_csv('supplier_photo.csv', index_col = 1)
del supplier_data['create_dt']

city_active_day = pd.crosstab(supplier_data.active_day,supplier_data.city_id)
city_active_day.ix[:,1:10].plot(kind = 'bar')

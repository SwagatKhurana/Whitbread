#!/usr/bin/env python
# coding: utf-8

# In[307]:


#import libraries
import json
import pandas as pd
import datetime as dt


#defined functions
def applyDiscount(quantity,amount):
    if quantity <= 30:
        return amount;
    elif quantity <=60:
        return amount*.975;
    elif quantity <=80:
        return amount*.96;
    elif quantity <=100:
        return amount*.94;
    elif quantity > 100:
        return amount*.9;
    else:
        return amount;

    
#read json files
with open("SalesData_2003.json") as jsonFile_2003:
    jsonObject_2003 = json.load(jsonFile_2003)
with open("SalesData_2004.json") as jsonFile_2004:
    jsonObject_2004 = json.load(jsonFile_2004)
with open("SalesData_2005.json") as jsonFile_2005:
    jsonObject_2005 = json.load(jsonFile_2005)
    
    #load data to dataframe to perform transformations
    result_2003 = pd.json_normalize(jsonObject_2003,record_path=['attributes'],meta=['ORDERNUMBER','PRODUCTCODE'])
    result_2004 = pd.json_normalize(jsonObject_2004,record_path=['attributes'],meta=['ORDERNUMBER','PRODUCTCODE'])
    result_2005 = pd.json_normalize(jsonObject_2005,record_path=['attributes'],meta=['ORDERNUMBER','PRODUCTCODE'])
    
    #consolidate input
    result = pd.concat([result_2003,result_2004,result_2005])

    #apply filters
    filter_list = ['Vintage Cars', 'Classic Cars', 'Motorcycles','Trucks and Buses']
    result[result.PRODUCTLINE.isin(filter_list)]
    
    #add calculated cols 
    result['SALES_AFTER_DISCOUNT'] = result.apply(lambda x: applyDiscount(x['QUANTITYORDERED'],x['SALES']),axis=1)
    result['PRICEEACH_AFTER_DISCOUNT'] = result.apply(lambda x: x['SALES_AFTER_DISCOUNT']/x['QUANTITYORDERED'],axis=1)
    
    result['ORDERDATE'] = pd.to_datetime(result['ORDERDATE']) 
    result['YEAR']= result['ORDERDATE'].dt.year
    result['MONTH']= result['ORDERDATE'].dt.month
    result['DAY']= result['ORDERDATE'].dt.day
    
    #write to parquet
    result.to_parquet('Output',compression='gzip',partition_cols=['YEAR','MONTH','DAY'])
    print(result)







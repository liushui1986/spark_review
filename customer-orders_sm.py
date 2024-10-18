# -*- coding: utf-8 -*-
"""
Created on Thu Oct 17 20:25:33 2024

@author: mashu
"""

from pyspark import SparkConf, SparkContext


conf = SparkConf().setMaster("local").setAppName("trasaction")
sc = SparkContext(conf = conf)


input = sc.textFile("file:///Users/mashu/Documents/study/spark/customer-orders.csv")

transactions = input.map(lambda x: (int(x.split(',')[0]), float(x.split(',')[2])))

total_trans = transactions.reduceByKey(lambda x, y: x + y)

results = total_trans.collect();

for result in results:
    print(result)


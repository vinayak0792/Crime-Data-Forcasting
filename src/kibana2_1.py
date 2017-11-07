
from datetime import time, datetime
from dateutil.parser import parse 
import json
from elasticsearch import Elasticsearch
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
sc = SparkContext(conf=conf)

def dateconvert(line):
	dt = parse(line)
	datetime_obj = dt.strftime('%d/%m/%Y')
	return datetime_obj
	

data = sc.textFile('/Users/deepaks/Desktop/Output2_1/part-00000')
data1 = data.map(lambda line: line.split("\n")).map(lambda line: line[0].split(','))
data2 = data1.map(lambda line: "{\"date\":\""+str(dateconvert(line[0][1:]))+"\",\"count\":\""+str(line[1][:-1]) +"\"}")
sentiment_json=data2.map(lambda line: (None,line))
sentiment_json.saveAsNewAPIHadoopFile(path='-', outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat", keyClass="org.apache.hadoop.io.NullWritable", valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", conf={ "es.resource" : "final3/ana","es.input.json" : "true" })


from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import urllib2
import urllib
import json

conf = SparkConf().setAppName("Temp").setMaster("local")
sc = SparkContext(conf=conf)


url = "http://144.202.34.148:5045/obtenerTemperatura"

post_params = {"Tipo":"Completa","NoCo":"14590575"}

params = urllib.urlencode(post_params)
response = urllib2.urlopen(url, params)
json_response = json.loads(response.read())

datos = json_response["D"]

datosRDD = sc.parallelize(datos)
reg = datosRDD.count()
temp = datosRDD.map(lambda x: x["Temp"])
temp_sum = temp.map(lambda x: x).reduce(lambda x, y: float(x) + float(y))
temp_prom = temp_sum/reg

print(temp_prom)



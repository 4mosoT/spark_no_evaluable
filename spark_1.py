# -*- coding: utf-8 -*-
from pyspark import SparkContext

sc = SparkContext("local", "spark_1")

rddLineas=sc.textFile('tweets-110928-es.txt') # Lee el fichero de texto y crea un elemento en el RDD por cada línea


def parseTweet(t):
    parts=t.split(" - ")
    return (parts[0],parts[1]," - ".join(parts[2:]))

rddTweets=rddLineas.map(parseTweet)

usuarioMax=rddTweets.map(lambda x: (x[0],1)).reduceByKey(lambda c1,c2: c1+c2).max(lambda (u,c): c)
print usuarioMax
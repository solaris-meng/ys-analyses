#-*- coding: utf8 -*- 
import traceback
import urllib
import datetime
import jieba
import requests
import json
import codecs
import os
import sys     
reload(sys)
sys.setdefaultencoding('utf8')


from pymongo import MongoClient
from pyspark import SparkContext

if __name__ == '__main__':
 
    BASE = '/root/spark/ys-analyses/data'
    today = datetime.datetime.today()
    yes = today - datetime.timedelta(days=1)

    for parent,dirnames,filenames in os.walk(BASE):
        for fname in filenames:
            if len(fname.split('_')) < 3:
                continue
            if fname.split('_')[1] == today.strftime('%Y-%m-%d'):
                print(fname)
                if True:
                    sc = SparkContext("local", "Simple App")
                    textFile = sc.textFile("%s/%s" % (BASE, fname))
                    rdd = textFile.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b).map(lambda (a,b): (b,a)).sortByKey(False)
                    rv = rdd.collect()
                    print(rv)
                    fd = codecs.open('%s/%s.count' % (BASE, fname), 'a+', 'utf8')
                    for k,v in rv:
                        fd.write('%d' % k)
                        fd.write('\t')
                        fd.write('%s' % v)
                        fd.write('\n')
                    fd.close()
                    sc.stop()

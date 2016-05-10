#-*- coding: utf8 -*- 
import traceback
import urllib
import datetime
import jieba
import requests
import json
import codecs
import sys     
reload(sys)
sys.setdefaultencoding('utf8')


from pymongo import MongoClient
#from pyspark import SparkContext

def dump_pvlog_browser():
    try:
        filename = 'browser.out'

        fd = open('%s' % filename,'w+')

        client = MongoClient("mongodb://dds-2ze74067495e13241.mongodb.rds.aliyuncs.com:3717")

        r = client.yushan.authenticate("yushan", "yushan", mechanism='SCRAM-SHA-1')
        db = client.yushan

        cur = db.pvlog.find()

        total = 0
        for d in cur:
            if not d['agent']:
                continue

            s1 = d['agent'].split()
            if len(s1) < 2:
                continue

            total += 1
            if total % 10000 == 0:
                print(total)
                pass
            if total > 1000:
                break
            if s1[-3] == 'Mobile':
                print(s1)

            try:
                #print(s1[-3])
                wd = s1[-3]
                fd.write('%s\n' % wd)
            except Exception as e:
                continue
        fd.close()

        print('finish')
        return '%s.out' % filename
    except Exception as e:
        print('Error:')
        err = traceback.format_exc()
        print(err)


def process_split():
    data = 'browser.out'
    data_out = 'browser2.out'
    fd = open(data, 'r')
    fd_out = open(data_out, 'w+')

    total = 0
    for line in fd.readlines():
        if total > 100:
            pass
        if '/' in line:
            w = line.split('/')[0]
            fd_out.write(w+'\n')
        else:
            fd_out.write(line)
        total += 1
    fd_out.close()
if __name__ == '__main__':
 
    dump_pvlog_browser()
    basename = 'browser'
    data = 'browser2.out'

    #process_split()
    if False:
        sc = SparkContext("local", "Simple App")
        textFile = sc.textFile("%s" % data)

        #rdd = textFile.flatMap(lambda line: line.split()).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b).map(lambda (a,b): (b,a)).sortByKey(False)
        #rdd = textFile.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b).map(lambda (a,b): (b,a)).sortByKey(False)
        #rv = rdd.collect()
        rv = ''

        print(rv)
        #fd = codecs.open('m2.out', 'a+', 'gbk')
        fd = codecs.open('%s.count' % basename, 'a+', 'utf8')
        for k,v in rv:
            fd.write('%d' % k)
            fd.write('\t')
            fd.write('%s' % v)
            fd.write('\n')
        fd.close()

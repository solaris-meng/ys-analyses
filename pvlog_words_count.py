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
from pyspark import SparkContext

BASE = '/root/spark/spark-1.6.1-bin-hadoop2.6/yushan'

def get_customer_appid():
    rv = requests.get('http://101.200.130.178/api/get/customer')
    if rv.text == 'failed':
        return

    r = rv.json()
    print(r)
    return r
#get_customer_appid()

def dump_pvlog_words(appid, sdate, edate):
    try:
        filename = 'words_%s_%s' % (appid, edate.strftime('%Y-%m-%d'))

        fd = open('%s/%s' % (BASE, filename),'w+')
        fd_out = open('%s/%s.out' % (BASE, filename), 'w+')
        fd_full = open('%s/%s.full' % (BASE, filename), 'w+')

        client = MongoClient("mongodb://dds-2ze74067495e13241.mongodb.rds.aliyuncs.com:3717")

        r = client.yushan.authenticate("yushan", "yushan", mechanism='SCRAM-SHA-1')
        db = client.yushan

        cur = db.pvlog.find({'c1':appid,"createdAt":{"$gt": sdate, "$lt": edate}})

        total = 0
        for d in cur:
            if not d['referer']:
                continue

            s1 = d['referer'].split('word=')
            if len(s1) < 2:
                continue

            total += 1
            if total > 30:
                pass

            #print(d)
            #fd.write(str(d['_id']))
            #fd.write(' '+d['brand'])
            #fd.write(' '+d['host'][0])

            #wd = s1[1].split('&')[0].encode('utf-8').decode('gb2312')
            wd_raw = s1[1].split('&')[0].encode().decode('utf-8')

            # python3
            #wd = urllib.parse.unquote(wd_raw)
            # python2
            try:
                wd = urllib.unquote(str(wd_raw)).decode('utf8')
                fd_full.write(' '+wd)
                wds = jieba.cut(wd, cut_all=True)
                for w in wds:
                    fd_out.write(' '+w)
                #print(wd)
            except Exception as e:
                continue

            fd.write('%s\n' % wd)
        fd.close()
        fd_out.close()
        fd_full.close()

        print('finish %s' %  filename)
        return filename
    except Exception as e:
        print('Error:')
        err = traceback.format_exc()
        print(err)
        return 'err'

# hsp
appid = '38983f5c407a498998af678cc38d5ce2'
def dump_pvlog_v1(yes_2, yes_1):

    yes_1 = datetime.datetime(yes_1.year, yes_1.month, yes_1.day, 14)
    yes_2 = datetime.datetime(yes_2.year, yes_2.month, yes_2.day, 14)

    appids = get_customer_appid()
    for appid in appids:
        dump_pvlog_words(appid, yes_2, yes_1)

if __name__ == '__main__':

    yes_1 = datetime.date.today() - datetime.timedelta(days=1)
    yes_2 = yes_1 - datetime.timedelta(days=1)

    # prepare data
    datafile = dump_pvlog_v1(yes_2, yes_1)
    print('prepare data finishe, filename-%s' % datafile)

    sc = SparkContext("local", "Simple App")

    appids = get_customer_appid()
    for appid in appids:
        filename = 'words_%s_%s' % (appid, yes_1.strftime('%Y-%m-%d'))

        if True:
            textFile = sc.textFile("%s/%s.out" % (BASE,filename))
            rdd = textFile.flatMap(lambda line: line.split()).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b).map(lambda (a,b): (b,a)).sortByKey(False)
            rv = rdd.collect()
            #fd = codecs.open('m2.out', 'a+', 'gbk')
            fd = codecs.open("%s/%s.count" % (BASE,filename), 'a+', 'utf8')
            for k,v in rv:
                fd.write('%d' % k)
                fd.write('\t')
                fd.write('%s' % v)
                fd.write('\n')
            fd.close()
            print 'Split Word, %s - finished' % appid

        if True:
            textFile = sc.textFile("%s/%s.full" % (BASE,filename))
            rdd = textFile.flatMap(lambda line: line.split()).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b).map(lambda (a,b): (b,a)).sortByKey(False)
            rv = rdd.collect()
            #fd = codecs.open('m2.out', 'a+', 'gbk')
            fd = codecs.open("%s/%s.fullcount" % (BASE,filename), 'a+', 'utf8')
            for k,v in rv:
                fd.write('%d' % k)
                fd.write('\t')
                fd.write('%s' % v)
                fd.write('\n')
            fd.close()
            print 'Split Word, %s - finished' % appid

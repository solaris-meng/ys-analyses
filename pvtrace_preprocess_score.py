# encoding=utf-8
import requests
import json
import logging
import jieba
import os
import datetime

from pyspark import SparkContext

logger = logging.getLogger('ms')

file_base = '/root/zhaoxq/analyze_file'

####################
# score model
download_score = 5

long_time_min = 3000
long_time_score = 2

mid_time_min = 1000
mid_time_score = 1

short_time_min = 0
short_time_score = 0

skip_score = -1
####################


def score_trace(trace_file):

    sc = SparkContext("local", "Simple App")
    trace_data = sc.textFile(trace_file)

    score_data = trace_data.map(lambda line: line.split("\t")).map(trace_line_score)
    
    output_file_url = trace_file + '-scores'
    output_file = open(output_file_url, 'w')

    sd = score_data.collect()
    for each_s in sd:
        line_str = each_s[0]
        p_count = len(each_s)
        for i in range(1, p_count):
            line_str = line_str + '\t' + each_s[i]
        line_str = line_str + '\n'
        output_file.write(line_str)

    output_file.close()
    
    sc.stop()
    print('---------------- SCORE_TRACE, Done - %s -------------------' % file_dir)
    return output_file_url

def trace_line_score(line):
    scores = []

    page_count = len(line) - 1
    sword = line[0]
    scores.append(sword)
    for i in range(1, page_count + 1):
        num = int(line[i])
        if num == -1:
            scores.append("5")
        elif num == -2:
            scores.append("-1")
        elif num >= short_time_min and num < mid_time_min:
            scores.append("%d" % short_time_score)
        elif num >= mid_time_min and num < long_time_min:
            scores.append("%d" % mid_time_score)
        elif num >= long_time_min:
            scores.append("%d" % long_time_score)
        else:
            scores.append("0")
    
    return scores

def score_tag(score_file_url):
    
    sc = SparkContext("local", "Simple App") 
    trace_score_data = sc.textFile(score_file_url)
    tag_score_data = trace_score_data.map(lambda line: line.split("\t")).flatMap(sword_jieba_tag).groupByKey().mapValues(list).map(tag_score_combine)

    """ 
    count = tag_score_data.count()
    print('---------------- SCORE_TAG, Count - %d -------------------' % count)
    for each in tag_score_data.collect():
        print(each)
    """

    file_name = os.path.basename(score_file_url)
    output_file_url = '/root/zhaoxq/analyze_file/' + file_name + '-bytags'
    output_file = open(output_file_url, 'w')
    
    
    tsd = tag_score_data.collect()
    for each_t in tsd:
        line_str = each_t[0] + '\t%d' % each_t[1]
        for  each_page in each_t[2]:
            line_str = line_str + '\t%f' % each_page
        line_str = line_str + '\n'
        output_file.write(line_str)

    output_file.close()
    sc.stop()
    print('---------------- SCORE_TAG, Done - %s -------------------' % output_file_url)
    return output_file_url

def sword_jieba_tag(line):
    tag_scores = []
    page_count = len(line) - 1 
    sword = line[0]
    seg_list = jieba.cut(sword, cut_all=False)
    for each_seg in seg_list:
        tag_line = []
        #tag_line.append(each_seg)
        for i in range(1 , page_count + 1):
            tag_line.append(line[i])
        tag_kv = (each_seg, tag_line)
        tag_scores.append(tag_kv)

    return tag_scores

def tag_score_combine(line):
    
    tag = line[0]
    scores = line[1]
    count = len(scores)

    total_scores = []
    for i in range(count):
        page_count = len(scores[i])
        page_score = scores[i]
        for j in range(page_count):
            if i == 0:
                total_scores.append(float(page_score[j]))
            else:
                total_scores[j] += float(page_score[j])

    for k in range(len(total_scores)):
        total_scores[k] = total_scores[k] / float(count)

    return (tag, count, total_scores)


if __name__ == '__main__':
    logger.info('DAILY_PAGE_SCORE, started')
    
    all_material_api_url = 'http://101.200.130.178:7001/tags/api/getallpage'
    r = requests.get(all_material_api_url)
    if r.status_code != 200:
        logger.info('Get request, all material url failed, status code - %d' % r.status_code)
    else:
        result = r.json()
        if result['result'] != 'success':
            logger.info('/tags/api.getallpage, result - %s, info - %s' % (result['result'], result['info']))
        else:
            urls = result['urls']
            this_date = datetime.date.today()
            yesterday = this_date - datetime.timedelta(days=1)
            date_str = yesterday.isoformat()

            file_dir = '/root/zhaoxq/analyze_file'
            for each in urls:
                trace_file_url = file_dir + '/' + each['router'] + '-' + each['page'] + '-' + date_str
                score_file_url = score_trace(trace_file_url)
                tags_file_url = score_tag(score_file_url)
    logger.info('DAILY_PAGE_SCORE, finished')

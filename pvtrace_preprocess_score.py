import requests
import json
import logging

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


def score_trace(file_dir, file_name):

    trace_file = file_base + "/" + file_name
    sc = SparkContext("local", "Simple App")
    trace_data = sc.textFile(trace_file)

    score_data = trace_data.map(lambda line: line.split("\t")).map(trace_line_score)
    
    output_file_url = file_dir + '/' + file_name + '-scores'
    output_file = open(output_file_url, 'w')

    sd = score_data.collect()
    for each_s in sd:
        line_str = ''
        for each_p in each_s:
            line_str = line_str + '\t' + each_p
        line_str = line_str + '\n'
        output_file.write(line_str)

    output_file.close()
    return output_file_url
    print('---------------- SCORE_TRACE, Done - %s -------------------' % file_dir)

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

if __name__ == '__main__':

    print("score_trace, started")
    file_dir = '/root/zhaoxq/analyze_file'
    file_name = 'gaode-lp2-2016-5-11'
    score_file_url = score_trace(file_dir, file_name)

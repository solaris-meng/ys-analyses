
tm=`date --date="1 days ago" +%Y-%m-%d`
echo 'cp -rf  /root/spark/spark-1.6.1-bin-hadoop2.6/yushan/*'${tm}'* /root/yushan_spark/'
echo 'pscp.pssh -h ips /root/yushan_spark/*'${tm}'* /root/yushan_spark/'

cp -rf  /root/spark/spark-1.6.1-bin-hadoop2.6/yushan/*${tm}* /root/yushan_spark/
pscp.pssh -h ips /root/yushan_spark/*${tm}* /root/yushan_spark/
